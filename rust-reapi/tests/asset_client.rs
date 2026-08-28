use remote_execution_proto::build::bazel::remote::execution::v2::{
    BatchReadBlobsRequest, BatchUpdateBlobsRequest, Digest, FindMissingBlobsRequest,
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

#[tokio::test]
async fn client_can_batch_update_and_read_blobs() -> Result<(), Box<dyn std::error::Error>> {
    let (mut cas, server) = common::cas_client().await?;

    let request = BatchUpdateBlobsRequest {
        instance_name: "test".to_string(),
        requests: vec![BlobRequest {
            digest: Some(Digest {
                hash: "012345".into(),
                size_bytes: 4,
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

    let to_fetch = BatchReadBlobsRequest {
        instance_name: "test".to_string(),
        digests: vec![Digest {
            hash: "012345".into(),
            size_bytes: 4,
        }],
        acceptable_compressors: vec![0],
        digest_function: DigestFunction::Sha256 as i32,
    };

    let fetched = cas
        .batch_read_blobs(Request::new(to_fetch))
        .await?
        .into_inner();
    assert_eq!(fetched.responses.len(), 1);
    let fetched_response = &fetched.responses[0];
    assert_eq!(fetched_response.digest.as_ref().unwrap().hash, "012345");
    assert_eq!(fetched_response.digest.as_ref().unwrap().size_bytes, 4);
    assert_eq!(fetched_response.data, b"test");
    assert_eq!(fetched_response.compressor, 0);
    assert_eq!(fetched_response.status.as_ref().unwrap().code, 0);

    server.abort();
    Ok(())
}
