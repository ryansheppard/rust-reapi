use remote_execution_proto::build::bazel::remote::execution::v2::{
    BatchUpdateBlobsRequest, Digest, FindMissingBlobsRequest,
    batch_update_blobs_request::Request as BlobRequest, digest_function::Value as DigestFunction,
};
use tonic::Request;

mod common;

#[tokio::test]
async fn client_can_find_missing_blobs() -> Result<(), Box<dyn std::error::Error>> {
    let (mut cas, server) = common::cas_client().await?;

    let request = FindMissingBlobsRequest {
        instance_name: String::new(),
        blob_digests: vec![Digest {
            hash: "012345".into(),
            size_bytes: 123,
        }],
        digest_function: DigestFunction::Sha256 as i32,
    };

    let response = cas
        .find_missing_blobs(Request::new(request))
        .await?
        .into_inner();
    assert_eq!(response.missing_blob_digests.len(), 1);
    assert_eq!(response.missing_blob_digests[0].hash, "012345");

    server.abort();
    Ok(())
}

#[tokio::test]
async fn client_can_batch_update_blobs() -> Result<(), Box<dyn std::error::Error>> {
    let (mut cas, server) = common::cas_client().await?;

    let request = BatchUpdateBlobsRequest {
        instance_name: "test".to_string(),
        requests: vec![BlobRequest {
            digest: Some(Digest {
                hash: "012345".into(),
                size_bytes: 123,
            }),
            data: "test".as_bytes().to_vec(),
            compressor: 0,
        }],
        digest_function: DigestFunction::Sha256 as i32,
    };

    let response = cas
        .batch_update_blobs(Request::new(request))
        .await?
        .into_inner();
    assert_eq!(response.responses.len(), 1);

    server.abort();
    Ok(())
}
