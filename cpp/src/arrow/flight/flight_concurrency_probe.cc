// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// Probe to determine whether the gRPC sync server defaults serialize
// concurrent Flight RPCs. Hosts a vanilla FlightServerBase whose DoGet
// handler sleeps for a fixed duration, fans out N concurrent client
// calls, and prints peak observed server-side concurrency along with
// wall-clock time. Optional flags install a builder_hook that tunes
// gRPC sync server options so the same probe can verify a fix.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <grpcpp/grpcpp.h>

#include "arrow/api.h"
#include "arrow/record_batch.h"
#include "arrow/util/logging.h"
#include "arrow/util/stopwatch_internal.h"

#include "arrow/flight/api.h"

DEFINE_int32(num_threads, 8, "Number of concurrent client DoGet calls");
DEFINE_int32(handler_sleep_ms, 1000,
             "Milliseconds the server's DoGet handler sleeps before returning");
DEFINE_bool(shared_client, true,
            "If true, all worker threads share one FlightClient (one TCP "
            "connection, HTTP/2-multiplexed streams). If false, each worker "
            "constructs its own FlightClient.");
DEFINE_int32(num_cqs, 0,
             "If > 0, install a builder_hook that calls "
             "SetSyncServerOption(NUM_CQS, ...).");
DEFINE_int32(min_pollers, 0,
             "If > 0, install a builder_hook that calls "
             "SetSyncServerOption(MIN_POLLERS, ...).");
DEFINE_int32(max_pollers, 0,
             "If > 0, install a builder_hook that calls "
             "SetSyncServerOption(MAX_POLLERS, ...).");

namespace arrow {

using internal::StopWatch;

namespace flight {

// Server whose DoGet sleeps for handler_sleep_ms while tracking how many
// handlers are simultaneously in flight. Used to distinguish a server that
// processes RPCs in parallel from one that serializes them.
class ConcurrencyProbeServer : public FlightServerBase {
 public:
  Status DoGet(const ServerCallContext&, const Ticket&,
               std::unique_ptr<FlightDataStream>* stream) override {
    const int now = in_flight_.fetch_add(1, std::memory_order_relaxed) + 1;
    int prev = peak_in_flight_.load(std::memory_order_relaxed);
    while (now > prev &&
           !peak_in_flight_.compare_exchange_weak(prev, now,
                                                  std::memory_order_relaxed)) {
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_handler_sleep_ms));

    in_flight_.fetch_sub(1, std::memory_order_relaxed);

    auto schema = arrow::schema({arrow::field("v", arrow::int64())});
    Int64Builder builder;
    ARROW_RETURN_NOT_OK(builder.Append(0));
    std::shared_ptr<Array> arr;
    ARROW_RETURN_NOT_OK(builder.Finish(&arr));
    auto batch = RecordBatch::Make(schema, 1, {arr});
    ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchReader::Make({batch}, schema));
    *stream = std::make_unique<RecordBatchStream>(reader);
    return Status::OK();
  }

  int peak_in_flight() const {
    return peak_in_flight_.load(std::memory_order_relaxed);
  }

 private:
  std::atomic<int> in_flight_{0};
  std::atomic<int> peak_in_flight_{0};
};

namespace {

std::string OrDefault(int v) { return v > 0 ? std::to_string(v) : "default"; }

}  // namespace

Status RunProbe() {
  auto server = std::make_unique<ConcurrencyProbeServer>();
  ARROW_ASSIGN_OR_RAISE(auto bind_location, Location::ForGrpcTcp("127.0.0.1", 0));
  FlightServerOptions options(bind_location);

  const bool tune_grpc =
      FLAGS_num_cqs > 0 || FLAGS_min_pollers > 0 || FLAGS_max_pollers > 0;
  if (tune_grpc) {
    options.builder_hook = [](void* raw) {
      auto* b = static_cast<grpc::ServerBuilder*>(raw);
      if (FLAGS_num_cqs > 0) {
        b->SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::NUM_CQS,
                               FLAGS_num_cqs);
      }
      if (FLAGS_min_pollers > 0) {
        b->SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::MIN_POLLERS,
                               FLAGS_min_pollers);
      }
      if (FLAGS_max_pollers > 0) {
        b->SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::MAX_POLLERS,
                               FLAGS_max_pollers);
      }
    };
  }

  ARROW_RETURN_NOT_OK(server->Init(options));
  std::thread serve_thread([&]() { ARROW_CHECK_OK(server->Serve()); });

  ARROW_ASSIGN_OR_RAISE(auto server_location,
                        Location::ForGrpcTcp("127.0.0.1", server->port()));
  auto client_options = FlightClientOptions::Defaults();

  std::unique_ptr<FlightClient> shared_client;
  if (FLAGS_shared_client) {
    ARROW_ASSIGN_OR_RAISE(shared_client,
                          FlightClient::Connect(server_location, client_options));
  }

  const int n = FLAGS_num_threads;
  std::vector<uint64_t> latencies_ns(n, 0);
  std::vector<std::thread> workers;
  workers.reserve(n);

  StopWatch wall;
  wall.Start();
  for (int i = 0; i < n; ++i) {
    workers.emplace_back([&, i]() {
      std::unique_ptr<FlightClient> local_client;
      FlightClient* client;
      if (FLAGS_shared_client) {
        client = shared_client.get();
      } else {
        auto r = FlightClient::Connect(server_location, client_options);
        ARROW_CHECK_OK(r.status());
        local_client = std::move(r).ValueOrDie();
        client = local_client.get();
      }

      FlightCallOptions call_options;
      Ticket ticket{"probe"};
      StopWatch timer;
      timer.Start();
      auto reader_r = client->DoGet(call_options, ticket);
      ARROW_CHECK_OK(reader_r.status());
      auto reader = std::move(reader_r).ValueOrDie();
      while (true) {
        auto chunk_r = reader->Next();
        ARROW_CHECK_OK(chunk_r.status());
        if (!chunk_r->data) break;
      }
      latencies_ns[i] = timer.Stop();
    });
  }
  for (auto& t : workers) t.join();
  const uint64_t wall_ns = wall.Stop();

  const int peak = server->peak_in_flight();
  ARROW_RETURN_NOT_OK(server->Shutdown());
  ARROW_RETURN_NOT_OK(server->Wait());
  serve_thread.join();

  std::sort(latencies_ns.begin(), latencies_ns.end());
  const uint64_t p50 = latencies_ns[latencies_ns.size() / 2];
  const size_t p95_idx =
      std::min<size_t>(latencies_ns.size() - 1, latencies_ns.size() * 95 / 100);
  const uint64_t p95 = latencies_ns[p95_idx];

  std::cout << "N=" << n << " shared_client=" << (FLAGS_shared_client ? "true" : "false")
            << " sleep_ms=" << FLAGS_handler_sleep_ms
            << " cqs=" << OrDefault(FLAGS_num_cqs)
            << " min=" << OrDefault(FLAGS_min_pollers)
            << " max=" << OrDefault(FLAGS_max_pollers)
            << " wall_ms=" << wall_ns / 1000000
            << " peak_inflight=" << peak << " p50_ms=" << p50 / 1000000
            << " p95_ms=" << p95 / 1000000 << std::endl;

  // Decision hint for humans.
  const uint64_t lower = static_cast<uint64_t>(FLAGS_handler_sleep_ms) * 1000000;
  const uint64_t upper = lower * static_cast<uint64_t>(n);
  if (peak <= 1 && wall_ns >= upper * 9 / 10) {
    std::cout << "verdict: SERIAL (handlers ran one at a time)" << std::endl;
  } else if (peak >= n) {
    std::cout << "verdict: PARALLEL (all " << n << " handlers ran concurrently)"
              << std::endl;
  } else {
    std::cout << "verdict: PARTIAL (peak " << peak << " of " << n
              << " concurrent handlers)" << std::endl;
  }
  return Status::OK();
}

}  // namespace flight
}  // namespace arrow

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto status = arrow::flight::RunProbe();
  if (!status.ok()) {
    std::cerr << "probe failed: " << status.ToString() << std::endl;
    return 1;
  }
  return 0;
}
