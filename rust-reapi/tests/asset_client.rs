use remote_execution_proto::build::bazel::remote::execution::v2::{
    Digest, FindMissingBlobsRequest,
    content_addressable_storage_client::ContentAddressableStorageClient,
    content_addressable_storage_server::ContentAddressableStorageServer,
    digest_function::Value as DigestFunction,
};
use rust_reapi::services::cas::CasService;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request};

#[tokio::test]
async fn client_can_reach_the_cas_server() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);

    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(ContentAddressableStorageServer::new(CasService))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("test server failed");
    });

    let mut cas = ContentAddressableStorageClient::connect(endpoint).await?;
    let request = FindMissingBlobsRequest {
        instance_name: String::new(),
        blob_digests: vec![Digest {
            hash: "012345".into(),
            size_bytes: 123,
        }],
        digest_function: DigestFunction::Sha256 as i32,
    };

    let error = cas
        .find_missing_blobs(Request::new(request))
        .await
        .expect_err("CAS is not implemented yet");
    assert_eq!(error.code(), Code::Unimplemented);

    server.abort();
    Ok(())
}
