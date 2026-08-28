// It is at this point I began to question everything
#[cfg(not(feature = "bazel_build"))]
pub use remote_execution_proto::google::rpc::Status as RpcStatus;

#[cfg(feature = "bazel_build")]
pub use status_proto::google::rpc::Status as RpcStatus;
