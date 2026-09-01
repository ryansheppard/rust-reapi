use std::{num::ParseIntError, pin::Pin, sync::Arc};

use bytestream_proto::google::bytestream::{
    QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest,
    WriteResponse, byte_stream_server::ByteStream,
};
use tonic::{Request, Response, Status};

use crate::{
    digest::DigestAlgorithm,
    storage::{BlobKey, BlobStore, CacheKind},
};

const READ_CHUNK_SIZE: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
enum ParseResourceError {
    #[error("invalid bytestream resource name")]
    InvalidShape,

    #[error("resource size is invalid")]
    InvalidSize(#[from] ParseIntError),
}

#[derive(Debug, PartialEq)]
enum Compression {
    Identity,
    Named(String),
}

#[derive(Debug, PartialEq)]
struct ParsedReadResource {
    instance: String,
    upload_id: Option<String>,
    algorithm: Option<String>,
    hash: String,
    expected_size: i64,
    compression: Compression,
}

pub struct ByteStreamService {
    store: Arc<dyn BlobStore + Send + Sync>,
}

impl ByteStreamService {
    pub fn new(store: Arc<dyn BlobStore + Send + Sync>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl ByteStream for ByteStreamService {
    type ReadStream = Pin<
        Box<
            dyn tonic::codegen::tokio_stream::Stream<Item = Result<ReadResponse, Status>>
                + Send
                + 'static,
        >,
    >;

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let request = request.into_inner();

        let parsed = parse_bytestream_resource_name(request.resource_name.as_str())
            .map_err(|err| Status::internal(err.to_string()))?;
        let key = resolve_blob_key(parsed)?;

        let blob = self
            .store
            .get(&key)
            .map_err(|err| Status::internal(err.to_string()))?
            .ok_or_else(|| Status::not_found("blob not found"))?;

        let offset = usize::try_from(request.read_offset)
            .map_err(|_| Status::invalid_argument("negative read offset"))?;

        if offset > blob.len() {
            return Err(Status::out_of_range("read offset larger than blob size"));
        }

        let available = &blob[offset..];
        let limit = match request.read_limit {
            0 => available.len(),
            limit => usize::try_from(limit)
                .map_err(|_| Status::invalid_argument("negative read limit"))?
                .min(available.len()),
        };

        let responses = available[..limit]
            .chunks(READ_CHUNK_SIZE)
            .map(|chunk| {
                Ok(ReadResponse {
                    data: chunk.to_vec(),
                })
            })
            .collect::<Vec<_>>();

        Ok(Response::new(Box::pin(tonic::codegen::tokio_stream::iter(
            responses,
        ))))
    }

    async fn write(
        &self,
        request: Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        let mut requests = request.into_inner();

        let mut accumulator: Vec<u8> = Vec::new();
        let mut resource_name = None;

        while let Some(chunk) = requests.message().await? {
            if chunk.write_offset != accumulator.len() as i64 {
                return Err(Status::invalid_argument("unexpected write offset"));
            }

            if resource_name.is_some() {
                if !chunk.resource_name.is_empty() {
                    return Err(Status::invalid_argument("unexpected chunk name"));
                }
            } else {
                if chunk.resource_name.is_empty() {
                    return Err(Status::invalid_argument(
                        "expected chunk name on first write",
                    ));
                }

                resource_name = Some(
                    parse_bytestream_resource_name(chunk.resource_name.as_str())
                        .map_err(|err| Status::invalid_argument(err.to_string()))?,
                );
            }

            accumulator.extend_from_slice(&chunk.data);

            if chunk.finish_write {
                let resource_name = resource_name.expect("expected resource name");
                let key = resolve_blob_key(resource_name)?;

                let committed_size = i64::try_from(accumulator.len())
                    .map_err(|_| Status::internal("blob exceeds exepected size"))?;

                self.store
                    .put(key, accumulator)
                    .map_err(|err| Status::internal(err.to_string()))?;
                return Ok(Response::new(WriteResponse { committed_size }));
            }
        }

        Err(Status::invalid_argument(
            "write stream ended before finish_write",
        ))
    }

    async fn query_write_status(
        &self,
        _request: Request<QueryWriteStatusRequest>,
    ) -> Result<Response<QueryWriteStatusResponse>, Status> {
        Err(Status::unimplemented("not implemented yet"))
    }
}

fn parse_bytestream_resource_name(
    resource_name: &str,
) -> Result<ParsedReadResource, ParseResourceError> {
    let parts: Vec<_> = resource_name.split('/').collect();

    let marker_index = parts
        .iter()
        .position(|part| *part == "blobs" || *part == "compressed-blobs")
        .ok_or(ParseResourceError::InvalidShape)?;

    let prefix = &parts[..marker_index];

    let (instance, upload_id) = if prefix.len() >= 2
        && prefix[prefix.len() - 2] == "uploads"
        && !prefix[prefix.len() - 1].is_empty()
    {
        (
            prefix[..prefix.len() - 2].join("/"),
            Some(prefix[prefix.len() - 1].to_owned()),
        )
    } else {
        (prefix.join("/"), None)
    };

    let marker = parts[marker_index];
    let suffix = &parts[marker_index + 1..];

    let (compression, digest_parts) = match marker {
        "blobs" => (Compression::Identity, suffix),
        "compressed-blobs" => {
            let (compressor, digest_parts) = suffix
                .split_first()
                .ok_or(ParseResourceError::InvalidShape)?;

            if compressor.is_empty() {
                return Err(ParseResourceError::InvalidShape);
            }

            (Compression::Named((*compressor).to_owned()), digest_parts)
        }

        _ => unreachable!("marker was checked above"),
    };

    let (algorithm, hash, size) = match digest_parts {
        [hash, size] => (None, *hash, *size),
        [algorithm, hash, size] => (Some((*algorithm).to_owned()), *hash, *size),
        _ => return Err(ParseResourceError::InvalidShape),
    };

    if hash.is_empty() {
        return Err(ParseResourceError::InvalidShape);
    }

    Ok(ParsedReadResource {
        instance,
        upload_id,
        algorithm: algorithm.to_owned(),
        hash: hash.to_owned(),
        compression,
        expected_size: size.parse()?,
    })
}

fn resolve_blob_key(resource: ParsedReadResource) -> Result<BlobKey, Status> {
    let algorithm = match resource.algorithm {
        Some(name) => name.parse::<DigestAlgorithm>(),
        None => Ok(DigestAlgorithm::Sha256),
    }
    .map_err(Status::invalid_argument)?;

    Ok(BlobKey {
        instance: resource.instance,
        algorithm,
        hash: resource.hash,
        kind: CacheKind::ContentAddressableStorage,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_uncompressed_bytestream_url() {
        let resource = "test/blobs/sha256/abc123/12";
        let actual = parse_bytestream_resource_name(resource).unwrap();

        let expected = ParsedReadResource {
            instance: "test".to_string(),
            upload_id: None,
            algorithm: Some("sha256".to_string()),
            hash: "abc123".to_string(),
            compression: Compression::Identity,
            expected_size: 12,
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_parse_upload_bytestream_url() {
        let resource = "test/uploads/upload-123/blobs/sha256/abc123/12";
        let actual = parse_bytestream_resource_name(resource).unwrap();

        assert_eq!(actual.instance, "test");
    }

    #[test]
    fn infers_sha256_for_bazel_style_resource_names() {
        let parsed = parse_bytestream_resource_name("test/uploads/upload-123/blobs/abc123/12")
            .expect("Bazel-style resource name should parse");

        let key =
            resolve_blob_key(parsed).expect("SHA-256 should be inferred for an omitted algorithm");

        assert_eq!(key.instance, "test");
        assert_eq!(key.algorithm, DigestAlgorithm::Sha256);
        assert_eq!(key.hash, "abc123");
        assert_eq!(key.kind, CacheKind::ContentAddressableStorage);
    }

    #[test]
    fn test_resolve_blob_key() {
        let parsed = ParsedReadResource {
            instance: "test".to_string(),
            upload_id: None,
            algorithm: Some("sha256".to_string()),
            hash: "abc123".to_string(),
            compression: Compression::Identity,
            expected_size: 12,
        };
        let actual = resolve_blob_key(parsed).unwrap();

        let expected = BlobKey {
            instance: "test".to_string(),
            algorithm: DigestAlgorithm::Sha256,
            hash: "abc123".to_string(),
            kind: CacheKind::ContentAddressableStorage,
        };

        assert_eq!(actual, expected);
    }
}
