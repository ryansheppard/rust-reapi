use bytestream_proto::google::bytestream::{ReadRequest, WriteRequest};
use remote_execution_proto::build::bazel::remote::execution::v2::{
    BatchReadBlobsRequest, BatchUpdateBlobsRequest, Digest, FindMissingBlobsRequest,
    batch_update_blobs_request::Request as BlobRequest, compressor::Value as Compressor,
    digest_function::Value as DigestFunction,
};
use tonic::{Code, Request};

mod common;

const TEST_DATA: &[u8] = b"test";
const TEST_HASH: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

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
                hash: TEST_HASH.into(),
                size_bytes: TEST_DATA.len() as i64,
            }),
            data: TEST_DATA.to_vec(),
            compressor: Compressor::Identity as i32,
        }],
        digest_function: DigestFunction::Sha256 as i32,
    };

    let response = cas
        .batch_update_blobs(Request::new(request))
        .await?
        .into_inner();
    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].status.as_ref().unwrap().code, 0);

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
                hash: TEST_HASH.into(),
                size_bytes: TEST_DATA.len() as i64,
            }),
            data: TEST_DATA.to_vec(),
            compressor: Compressor::Identity as i32,
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
            hash: TEST_HASH.into(),
            size_bytes: TEST_DATA.len() as i64,
        }],
        acceptable_compressors: vec![Compressor::Identity as i32],
        digest_function: DigestFunction::Sha256 as i32,
    };

    let fetched = cas
        .batch_read_blobs(Request::new(to_fetch))
        .await?
        .into_inner();
    assert_eq!(fetched.responses.len(), 1);
    let fetched_response = &fetched.responses[0];
    assert_eq!(fetched_response.digest.as_ref().unwrap().hash, TEST_HASH);
    assert_eq!(
        fetched_response.digest.as_ref().unwrap().size_bytes,
        TEST_DATA.len() as i64
    );
    assert_eq!(fetched_response.data, TEST_DATA);
    assert_eq!(fetched_response.compressor, Compressor::Identity as i32);
    assert_eq!(fetched_response.status.as_ref().unwrap().code, 0);

    server.abort();
    Ok(())
}

#[tokio::test]
async fn client_can_read_cas_blob_via_bytestream() -> Result<(), Box<dyn std::error::Error>> {
    let (mut cas, mut bytestream, server) = common::cache_clients().await?;
    cas.batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
        instance_name: "test".to_string(),
        requests: vec![BlobRequest {
            digest: Some(Digest {
                hash: TEST_HASH.into(),
                size_bytes: TEST_DATA.len() as i64,
            }),
            data: TEST_DATA.to_vec(),
            compressor: Compressor::Identity as i32,
        }],
        digest_function: DigestFunction::Sha256 as i32,
    }))
    .await?;

    let mut stream = bytestream
        .read(Request::new(ReadRequest {
            resource_name: format!("test/blobs/sha256/{TEST_HASH}/{}", TEST_DATA.len()),
            read_offset: 0,
            read_limit: 0,
        }))
        .await?
        .into_inner();
    let mut data = Vec::new();
    while let Some(response) = stream.message().await? {
        data.extend_from_slice(&response.data);
    }

    assert_eq!(data, TEST_DATA);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn client_can_write_bytestream_blob_and_read_it_via_cas()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut cas, mut bytestream, server) = common::cache_clients().await?;
    let response = bytestream
        .write(Request::new(tokio_stream::iter([WriteRequest {
            resource_name: format!(
                "test/uploads/upload-123/blobs/sha256/{TEST_HASH}/{}",
                TEST_DATA.len()
            ),
            write_offset: 0,
            finish_write: true,
            data: TEST_DATA.to_vec(),
        }])))
        .await?
        .into_inner();

    assert_eq!(response.committed_size, TEST_DATA.len() as i64);

    let response = cas
        .batch_read_blobs(Request::new(BatchReadBlobsRequest {
            instance_name: "test".to_string(),
            digests: vec![Digest {
                hash: TEST_HASH.into(),
                size_bytes: TEST_DATA.len() as i64,
            }],
            acceptable_compressors: vec![Compressor::Identity as i32],
            digest_function: DigestFunction::Sha256 as i32,
        }))
        .await?
        .into_inner();

    assert_eq!(response.responses[0].data, TEST_DATA);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn compressed_batch_update_and_read_reports_actual_encoding()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut cas, server) = common::cas_client().await?;
    let compressed = zstd::stream::encode_all(TEST_DATA, zstd::DEFAULT_COMPRESSION_LEVEL)?;

    let updated = cas
        .batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
            instance_name: "test".to_string(),
            requests: vec![BlobRequest {
                digest: Some(Digest {
                    hash: TEST_HASH.into(),
                    size_bytes: TEST_DATA.len() as i64,
                }),
                data: compressed,
                compressor: Compressor::Zstd as i32,
            }],
            digest_function: DigestFunction::Sha256 as i32,
        }))
        .await?
        .into_inner();
    assert_eq!(updated.responses[0].status.as_ref().unwrap().code, 0);

    let fetched = cas
        .batch_read_blobs(Request::new(BatchReadBlobsRequest {
            instance_name: "test".to_string(),
            digests: vec![Digest {
                hash: TEST_HASH.into(),
                size_bytes: TEST_DATA.len() as i64,
            }],
            acceptable_compressors: vec![Compressor::Zstd as i32],
            digest_function: DigestFunction::Sha256 as i32,
        }))
        .await?
        .into_inner();
    let response = &fetched.responses[0];
    assert_eq!(response.compressor, Compressor::Zstd as i32);
    assert_eq!(
        zstd::stream::decode_all(response.data.as_slice())?,
        TEST_DATA
    );

    server.abort();
    Ok(())
}

#[tokio::test]
async fn compressed_batch_update_rejects_uncompressed_size_mismatch()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut cas, server) = common::cas_client().await?;
    let compressed = zstd::stream::encode_all(TEST_DATA, zstd::DEFAULT_COMPRESSION_LEVEL)?;

    let response = cas
        .batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
            instance_name: "test".to_string(),
            requests: vec![BlobRequest {
                digest: Some(Digest {
                    hash: TEST_HASH.into(),
                    size_bytes: TEST_DATA.len() as i64 + 1,
                }),
                data: compressed,
                compressor: Compressor::Zstd as i32,
            }],
            digest_function: DigestFunction::Sha256 as i32,
        }))
        .await?
        .into_inner();

    assert_eq!(
        response.responses[0].status.as_ref().unwrap().code,
        Code::InvalidArgument as i32
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn client_can_read_a_compressed_bytestream_suffix() -> Result<(), Box<dyn std::error::Error>>
{
    let (mut cas, mut bytestream, server) = common::cache_clients().await?;
    cas.batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
        instance_name: "test".to_string(),
        requests: vec![BlobRequest {
            digest: Some(Digest {
                hash: TEST_HASH.into(),
                size_bytes: TEST_DATA.len() as i64,
            }),
            data: TEST_DATA.to_vec(),
            compressor: Compressor::Identity as i32,
        }],
        digest_function: DigestFunction::Sha256 as i32,
    }))
    .await?;

    let resource_name = format!(
        "test/compressed-blobs/zstd/sha256/{TEST_HASH}/{}",
        TEST_DATA.len()
    );
    let mut stream = bytestream
        .read(Request::new(ReadRequest {
            resource_name: resource_name.clone(),
            read_offset: 1,
            read_limit: 0,
        }))
        .await?
        .into_inner();
    let mut compressed = Vec::new();
    while let Some(response) = stream.message().await? {
        compressed.extend_from_slice(&response.data);
    }
    assert_eq!(zstd::stream::decode_all(compressed.as_slice())?, b"est");

    let error = bytestream
        .read(Request::new(ReadRequest {
            resource_name,
            read_offset: 0,
            read_limit: 1,
        }))
        .await
        .expect_err("compressed reads with a limit should fail");
    assert_eq!(error.code(), Code::InvalidArgument);

    server.abort();
    Ok(())
}

#[tokio::test]
async fn compressed_bytestream_write_rejects_hash_mismatch()
-> Result<(), Box<dyn std::error::Error>> {
    let (_cas, mut bytestream, server) = common::cache_clients().await?;
    let compressed = zstd::stream::encode_all(TEST_DATA, zstd::DEFAULT_COMPRESSION_LEVEL)?;
    let wrong_hash = "0000000000000000000000000000000000000000000000000000000000000000";

    let error = bytestream
        .write(Request::new(tokio_stream::iter([WriteRequest {
            resource_name: format!(
                "test/uploads/upload-123/compressed-blobs/zstd/sha256/{wrong_hash}/{}",
                TEST_DATA.len()
            ),
            write_offset: 0,
            finish_write: true,
            data: compressed,
        }])))
        .await
        .expect_err("a mismatched uncompressed hash should fail");
    assert_eq!(error.code(), Code::InvalidArgument);

    server.abort();
    Ok(())
}

#[tokio::test]
async fn client_can_write_a_compressed_bytestream_blob() -> Result<(), Box<dyn std::error::Error>> {
    let (mut cas, mut bytestream, server) = common::cache_clients().await?;
    let compressed = zstd::stream::encode_all(TEST_DATA, zstd::DEFAULT_COMPRESSION_LEVEL)?;
    let committed_size = compressed.len() as i64;

    let response = bytestream
        .write(Request::new(tokio_stream::iter([WriteRequest {
            resource_name: format!(
                "test/uploads/upload-123/compressed-blobs/zstd/sha256/{TEST_HASH}/{}",
                TEST_DATA.len()
            ),
            write_offset: 0,
            finish_write: true,
            data: compressed,
        }])))
        .await?
        .into_inner();
    assert_eq!(response.committed_size, committed_size);

    let fetched = cas
        .batch_read_blobs(Request::new(BatchReadBlobsRequest {
            instance_name: "test".to_string(),
            digests: vec![Digest {
                hash: TEST_HASH.into(),
                size_bytes: TEST_DATA.len() as i64,
            }],
            acceptable_compressors: vec![Compressor::Identity as i32],
            digest_function: DigestFunction::Sha256 as i32,
        }))
        .await?
        .into_inner();
    assert_eq!(fetched.responses[0].data, TEST_DATA);

    server.abort();
    Ok(())
}
