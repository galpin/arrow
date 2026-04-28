# Flight Server Single-Request-At-A-Time: Likely Causes

Investigation of the Apache Arrow Flight server source code surfaced two
distinct issues — one for each language.

## 1. C++: gRPC's default sync server has very few worker threads

`cpp/src/arrow/flight/transport/grpc/grpc_server.cc:585-633` shows the
`ServerBuilder` is set up with **no thread or completion-queue
configuration**:

```cpp
::grpc::ServerBuilder builder;
builder.SetMaxReceiveMessageSize(-1);
...
builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);
if (options.builder_hook) options.builder_hook(&builder);
grpc_server_ = builder.BuildAndStart();
```

For sync services (which Flight uses — `GrpcServiceHandler` derives from
`FlightService::Service`), gRPC's defaults are:

- `MIN_POLLERS = 1`, `MAX_POLLERS = 2` per completion queue
- `NUM_CQS = 1`
- A small thread pool grown on demand

That's enough to *accept* multiple connections, but long-running
`DoGet` / `DoPut` / `DoExchange` streams pin a sync handler thread for
the whole call. If the application opens few connections and runs
streams sequentially over each, throughput collapses to near-serial.
There's no `SetSyncServerOption(NUM_CQS, …)`, no
`MIN_POLLERS`/`MAX_POLLERS` tuning, and no resource quota / executor
injection. The only escape hatch is
`FlightServerOptions::builder_hook` (`cpp/src/arrow/flight/server.h:172-180`),
which most users don't know to use.

## 2. Python: every handler is invoked under the GIL

`python/pyarrow/_flight.pyx:2257-2361` defines the C++→Python trampolines
(`_do_get`, `_do_put`, `_do_exchange`, `_do_action`, …) as plain
`cdef CStatus … except *` — no `nogil`. Cython acquires the GIL on
entry, so the user's `do_get` / `do_put` body runs with the GIL held.

Even though the C++/gRPC layer happily dispatches multiple RPCs onto
separate threads, all those threads block on the single GIL the moment
they reach the Python handler, so request processing is serialized.
The codebase itself acknowledges this constraint in the `shutdown()`
docstring at `_flight.pyx:3237-3240`:

> "Must not hold the GIL: shutdown waits for pending RPCs to complete.
> Holding the GIL means Python-implemented Flight methods will never
> get to run, so this will hang indefinitely."

For `do_get` specifically, after the handler returns a
`FlightDataStream`, batches are pulled by `CPyFlightDataStream` which
re-acquires the GIL per batch — so streaming work also serializes on
the GIL. `do_put` / `do_exchange` are worse: the entire stream lifetime
runs inside the GIL-holding callback.

`serve()` itself (`_flight.pyx:3214`) does
`with nogil: ServeWithSignals()`, so the listener thread is fine — the
bottleneck is purely the handler dispatch.

## Common misconfigurations / gotchas to check

1. **Single client / single connection.** gRPC multiplexes streams over
   HTTP/2, but a sync server still allocates one handler thread per
   active streaming RPC. With only one connection and few in-flight
   RPCs, you can look serial even on a properly threaded server. Try
   multiple parallel clients to distinguish "server is serial" from
   "client is serial".
2. **Handlers doing CPU-bound Python work.** In Python, even a
   perfectly threaded server can't help — release the GIL inside hot
   loops (NumPy/PyArrow C calls usually do, pure-Python doesn't), or
   move heavy work to processes.
3. **Holding a global lock / shared state in the handler** — e.g., a
   `threading.Lock` around a dataset, or DB connections that
   serialize. Worth auditing your own `do_get` / `do_put`.
4. **No `builder_hook` on C++ servers.** For high concurrency you
   typically want something like:

   ```cpp
   options.builder_hook = [](void* raw) {
     auto* b = static_cast<grpc::ServerBuilder*>(raw);
     b->SetSyncServerOption(grpc::ServerBuilder::NUM_CQS, n_cqs);
     b->SetSyncServerOption(grpc::ServerBuilder::MIN_POLLERS, min);
     b->SetSyncServerOption(grpc::ServerBuilder::MAX_POLLERS, max);
   };
   ```

## Recommended next steps

- Confirm with multiple concurrent clients (multiple
  processes/connections), not threads in one client, to rule out
  client-side serialization.
- For C++, plumb `builder_hook` to raise `NUM_CQS` / `MAX_POLLERS` and
  re-test.
- For Python, run the server with multiple processes (one per core)
  behind a load balancer, or push the heavy work into C/native code
  that releases the GIL — there's no in-process fix for the GIL
  serialization in the current `_flight.pyx` callback design.

## Key file references

- `cpp/src/arrow/flight/transport/grpc/grpc_server.cc:585-633` — gRPC
  `ServerBuilder` setup with no thread tuning.
- `cpp/src/arrow/flight/server.h:144-181` — `FlightServerOptions`,
  including `builder_hook`.
- `python/pyarrow/_flight.pyx:2257-2361` — Python handler trampolines
  invoked under the GIL.
- `python/pyarrow/_flight.pyx:3206-3249` — `serve()` / `shutdown()` /
  `wait()` lifecycle.
