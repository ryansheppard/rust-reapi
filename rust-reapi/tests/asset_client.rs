use bytestream_proto::google::bytestream::{ReadRequest, WriteRequest};
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

#[tokio::test]
async fn client_can_read_cas_blob_via_bytestream() -> Result<(), Box<dyn std::error::Error>> {
    const HASH: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    let (mut cas, mut bytestream, server) = common::cache_clients().await?;
    cas.batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
        instance_name: "test".to_string(),
        requests: vec![BlobRequest {
            digest: Some(Digest {
                hash: HASH.into(),
                size_bytes: 4,
            }),
            data: b"test".to_vec(),
            compressor: 0,
        }],
        digest_function: DigestFunction::Sha256 as i32,
    }))
    .await?;

    let mut stream = bytestream
        .read(Request::new(ReadRequest {
            resource_name: format!("test/blobs/sha256/{HASH}/4"),
            read_offset: 0,
            read_limit: 0,
        }))
        .await?
        .into_inner();
    let mut data = Vec::new();
    while let Some(response) = stream.message().await? {
        data.extend_from_slice(&response.data);
    }

    assert_eq!(data, b"test");
    server.abort();
    Ok(())
}

#[tokio::test]
async fn client_can_write_bytestream_blob_and_read_it_via_cas()
-> Result<(), Box<dyn std::error::Error>> {
    const HASH: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    let (mut cas, mut bytestream, server) = common::cache_clients().await?;
    let response = bytestream
        .write(Request::new(tokio_stream::iter([WriteRequest {
            resource_name: format!("test/uploads/upload-123/blobs/sha256/{HASH}/4"),
            write_offset: 0,
            finish_write: true,
            data: b"test".to_vec(),
        }])))
        .await?
        .into_inner();

    assert_eq!(response.committed_size, 4);

    let response = cas
        .batch_read_blobs(Request::new(BatchReadBlobsRequest {
            instance_name: "test".to_string(),
            digests: vec![Digest {
                hash: HASH.into(),
                size_bytes: 4,
            }],
            acceptable_compressors: vec![0],
            digest_function: DigestFunction::Sha256 as i32,
        }))
        .await?
        .into_inner();

    assert_eq!(response.responses[0].data, b"test");
    server.abort();
    Ok(())
}
