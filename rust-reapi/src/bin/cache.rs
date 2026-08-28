use rust_reapi::{services::cas::CasService, storage::InMemoryStore};
use remote_execution_proto::build::bazel::remote::execution::v2::content_addressable_storage_server::ContentAddressableStorageServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse()?;

    let service = CasService::new(InMemoryStore::new());

    tonic::transport::Server::builder()
        .add_service(ContentAddressableStorageServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
