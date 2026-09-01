#![allow(dead_code)] // TODO: probably can split this up between clients to avoid

use std::sync::Arc;

use bytestream_proto::google::bytestream::{
    byte_stream_client::ByteStreamClient, byte_stream_server::ByteStreamServer,
};
use remote_execution_proto::build::bazel::remote::execution::v2::{
    action_cache_client::ActionCacheClient, action_cache_server::ActionCacheServer,
    capabilities_client::CapabilitiesClient, capabilities_server::CapabilitiesServer,
    content_addressable_storage_client::ContentAddressableStorageClient,
    content_addressable_storage_server::ContentAddressableStorageServer,
};
use rust_reapi::{
    services::{
        action_cache::ActionCacheService, bytestream::ByteStreamService,
        capabilities::CapabilitiesService, cas::CasService,
    },
    storage::{BlobStore, InMemoryStore},
};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};

pub async fn cas_client()
-> Result<(ContentAddressableStorageClient<Channel>, JoinHandle<()>), Box<dyn std::error::Error>> {
    let (cas, _bytestream, server) = cache_clients().await?;
    Ok((cas, server))
}

pub async fn cache_clients() -> Result<
    (
        ContentAddressableStorageClient<Channel>,
        ByteStreamClient<Channel>,
        JoinHandle<()>,
    ),
    Box<dyn std::error::Error>,
> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let store: Arc<dyn BlobStore + Send + Sync> = Arc::new(InMemoryStore::new());

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(ByteStreamServer::new(ByteStreamService::new(Arc::clone(
                &store,
            ))))
            .add_service(ContentAddressableStorageServer::new(CasService::new(store)))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap_or_else(|error| panic!("test server failed: {error}"));
    });

    let cas = ContentAddressableStorageClient::connect(endpoint.clone()).await?;
    let bytestream = ByteStreamClient::connect(endpoint).await?;
    Ok((cas, bytestream, server))
}

pub async fn action_cache_client() -> Result<
    (
        ActionCacheClient<Channel>,
        Arc<InMemoryStore>,
        JoinHandle<()>,
    ),
    Box<dyn std::error::Error>,
> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let store = Arc::new(InMemoryStore::new());
    let service_store: Arc<dyn BlobStore + Send + Sync> = store.clone();

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(ActionCacheServer::new(ActionCacheService::new(service_store)))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap_or_else(|error| panic!("test server failed: {error}"));
    });

    let client = ActionCacheClient::connect(endpoint).await?;
    Ok((client, store, server))
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
