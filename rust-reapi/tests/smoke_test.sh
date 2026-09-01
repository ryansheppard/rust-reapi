#!/usr/bin/env bash

set -euo pipefail

cache_binary="${TEST_SRCDIR:?TEST_SRCDIR is required}/${TEST_WORKSPACE:?TEST_WORKSPACE is required}/rust-reapi/rust_reapi_cache"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/rust-reapi-smoke.XXXXXX")"
cache_log="$work_dir/reapi-cache.log"
cache_pid=""

cleanup() {
    if [[ -n "$cache_pid" ]]; then
        kill "$cache_pid" 2>/dev/null || true
        wait "$cache_pid" 2>/dev/null || true
    fi
    rm -rf "$work_dir"
}
trap cleanup EXIT INT TERM

if (: >/dev/tcp/127.0.0.1/50051) 2>/dev/null; then
    echo "port 50051 is already in use; stop the existing cache server first" >&2
    exit 1
fi

if [[ ! -x "$cache_binary" ]]; then
    echo "cache binary is missing from test runfiles: $cache_binary" >&2
    exit 1
fi

"$cache_binary" >"$cache_log" 2>&1 &
cache_pid=$!

for _ in {1..100}; do
    if (: >/dev/tcp/127.0.0.1/50051) 2>/dev/null; then
        break
    fi
    if ! kill -0 "$cache_pid" 2>/dev/null; then
        echo "cache server exited before becoming ready:" >&2
        cat "$cache_log" >&2
        exit 1
    fi
    sleep 0.1
done

if ! (: >/dev/tcp/127.0.0.1/50051) 2>/dev/null; then
    echo "cache server did not become ready:" >&2
    cat "$cache_log" >&2
    exit 1
fi

cat >"$work_dir/MODULE.bazel" <<'EOF'
module(name = "reapi_smoke", version = "0.0.1")
EOF

cat >"$work_dir/BUILD.bazel" <<'EOF'
genrule(
    name = "hello",
    outs = ["hello.txt"],
    cmd = "echo hello > $@",
)
EOF

cd "$work_dir"
bazel build \
    --remote_cache=grpc://127.0.0.1:50051 \
    --remote_upload_local_results \
    //:hello 2>&1 | tee "$work_dir/first-build.log"

bazel clean

second_build_log="$work_dir/second-build.log"
bazel build \
    --remote_cache=grpc://127.0.0.1:50051 \
    --remote_upload_local_results \
    //:hello 2>&1 | tee "$second_build_log"

if ! rg -q "remote cache hit" "$second_build_log"; then
    echo "expected the second build to report a remote cache hit" >&2
    cat "$cache_log" >&2
    exit 1
fi

echo "remote-cache smoke test passed"
