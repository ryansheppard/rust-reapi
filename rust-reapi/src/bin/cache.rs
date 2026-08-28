use remote_execution_proto::build::bazel::remote::execution::v2::{
    capabilities_server::CapabilitiesServer,
    content_addressable_storage_server::ContentAddressableStorageServer,
};
use rust_reapi::{
    services::{capabilities::CapabilitiesService, cas::CasService},
    storage::InMemoryStore,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse()?;

    let cas_service = CasService::new(InMemoryStore::new());
    let capabilities_service = CapabilitiesService::new();

    tonic::transport::Server::builder()
        .add_service(ContentAddressableStorageServer::new(cas_service))
        .add_service(CapabilitiesServer::new(capabilities_service))
        .serve(addr)
        .await?;

    Ok(())
}
