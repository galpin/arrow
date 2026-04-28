    /workspaces/arrow/cpp/build/release/arrow-flight-concurrency-probe --num_threads=1 --handler_sleep_ms=500
  /workspaces/arrow/cpp/build/release/arrow-flight-concurrency-probe --num_threads=8 --shared_client=true
  /workspaces/arrow/cpp/build/release/arrow-flight-concurrency-probe --num_threads=8 --shared_client=false
  /workspaces/arrow/cpp/build/release/arrow-flight-concurrency-probe --num_threads=8 --num_cqs=4 --max_pollers=32
  /workspaces/arrow/cpp/build/release/arrow-flight-concurrency-probe --num_threads=32 --shared_client=false --num_cqs=4 --max_pollers=64