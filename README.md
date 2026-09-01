# rust-reapi

A soon to be rust implementation of https://github.com/bazelbuild/remote-apis. At least for caching. This is a toy project and meant to learn more about Bazel. The code is largely human crafted slop with AI to help review and search things.

## It works
```bash
$ cargo run -p rust_reapi --bin cache
$ bazel build --remote_cache=grpc://127.0.0.1:50051 --remote_upload_local_results //rust-reapi:rust_reapi_cache
INFO: Invocation ID: 4c86ded3-3e89-4d37-928a-b3ae80fa7474
INFO: Analyzed target //rust-reapi:rust_reapi_cache (0 packages loaded, 0 targets configured).
INFO: Found 1 target...
Target //rust-reapi:rust_reapi_cache up-to-date:
  bazel-bin/rust-reapi/rust_reapi_cache
INFO: Elapsed time: 0.208s, Critical Path: 0.00s
INFO: 1 process: 1 internal.
INFO: Build completed successfully, 1 total action

$ bazel clean
INFO: Starting clean (this may take a while). Use --async if the clean takes more than several minutes.

$ rust-reapi main ❯ bazel build --remote_cache=grpc://127.0.0.1:50051 --remote_upload_local_results //rust-reapi:rust_reapi_cache
INFO: Invocation ID: 15eefe07-3061-4473-a2f3-a0585520c040
INFO: Analyzed target //rust-reapi:rust_reapi_cache (299 packages loaded, 15398 targets configured, 165 aspect applications).
INFO: Found 1 target...
Target //rust-reapi:rust_reapi_cache up-to-date:
  bazel-bin/rust-reapi/rust_reapi_cache
INFO: Elapsed time: 3.142s, Critical Path: 0.49s
INFO: 665 processes: 362 remote cache hit, 303 internal.
INFO: Build completed successfully, 665 total actions
```
