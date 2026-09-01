use std::sync::Arc;

use bytestream_proto::google::bytestream::byte_stream_server::ByteStreamServer;
use remote_execution_proto::build::bazel::remote::execution::v2::{
    action_cache_server::ActionCacheServer, capabilities_server::CapabilitiesServer,
    content_addressable_storage_server::ContentAddressableStorageServer,
};
use rust_reapi::{
    services::{
        action_cache::ActionCacheService, bytestream::ByteStreamService,
        capabilities::CapabilitiesService, cas::CasService,
    },
    storage::{BlobStore, InMemoryStore},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse()?;

    let store: Arc<dyn BlobStore + Send + Sync> = Arc::new(InMemoryStore::new());

    let capabilities_service = CapabilitiesService::new();

    let cas_service = CasService::new(Arc::clone(&store));
    let bytestream_service = ByteStreamService::new(Arc::clone(&store));
    let action_cache_service = ActionCacheService::new(Arc::clone(&store));

    tonic::transport::Server::builder()
        .add_service(CapabilitiesServer::new(capabilities_service))
        .add_service(ByteStreamServer::new(bytestream_service))
        .add_service(ContentAddressableStorageServer::new(cas_service))
        .add_service(ActionCacheServer::new(action_cache_service))
        .serve(addr)
        .await?;

    Ok(())
}
