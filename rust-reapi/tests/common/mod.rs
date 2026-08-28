#![allow(dead_code)] // TODO: probably can split this up between clients to avoid

use remote_execution_proto::build::bazel::remote::execution::v2::{
    capabilities_client::CapabilitiesClient, capabilities_server::CapabilitiesServer,
    content_addressable_storage_client::ContentAddressableStorageClient,
    content_addressable_storage_server::ContentAddressableStorageServer,
};
use rust_reapi::{
    services::{capabilities::CapabilitiesService, cas::CasService},
    storage::InMemoryStore,
};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};

pub async fn cas_client()
-> Result<(ContentAddressableStorageClient<Channel>, JoinHandle<()>), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(ContentAddressableStorageServer::new(CasService::new(
                InMemoryStore::new(),
            )))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap_or_else(|error| panic!("test server failed: {error}"));
    });

    let client = ContentAddressableStorageClient::connect(endpoint).await?;
    Ok((client, server))
}

pub async fn capabilities_client()
-> Result<(CapabilitiesClient<Channel>, JoinHandle<()>), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(CapabilitiesServer::new(CapabilitiesService::new()))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap_or_else(|error| panic!("test server failed: {error}"));
    });

    let client = CapabilitiesClient::connect(endpoint).await?;
    Ok((client, server))
}
