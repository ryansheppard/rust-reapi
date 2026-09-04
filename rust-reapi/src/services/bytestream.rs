use std::{num::ParseIntError, pin::Pin, sync::Arc};

use tokio::io::AsyncReadExt;

use bytestream_proto::google::bytestream::{
    QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest,
    WriteResponse, byte_stream_server::ByteStream,
};
use tonic::{Request, Response, Status};

use crate::{
    digest::DigestAlgorithm,
    storage::{BlobCodec, BlobKey, BlobRead, BlobStore, CacheKind, StorageEncoding},
};

const READ_CHUNK_SIZE: usize = 64 * 1024;
const STORAGE_ENCODING: StorageEncoding = StorageEncoding::Zstd;

#[derive(Debug, thiserror::Error)]
enum ParseResourceError {
    #[error("invalid bytestream resource name")]
    InvalidShape,

    #[error("resource size is invalid")]
    InvalidSize(#[from] ParseIntError),

    #[error("resource size must not be negative")]
    NegativeSize,
}

#[derive(Debug, PartialEq)]
enum Compression {
    Identity,
    Named(String),
}

impl Compression {
    fn storage_encoding(&self) -> Result<StorageEncoding, Status> {
        match self {
            Self::Identity => Ok(StorageEncoding::Identity),
            Self::Named(name) if name == "zstd" => Ok(StorageEncoding::Zstd),
            Self::Named(name) => Err(Status::unimplemented(format!(
                "unsupported compression method: {name}"
            ))),
        }
    }
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
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        if parsed.upload_id.is_some() {
            return Err(Status::invalid_argument(
                "read resource name must not contain an upload id",
            ));
        }
        let response_encoding = parsed.compression.storage_encoding()?;
        if response_encoding != StorageEncoding::Identity && request.read_limit != 0 {
            return Err(Status::invalid_argument(
                "read_limit is not supported for compressed reads",
            ));
        }
        let key = resolve_blob_key(&parsed)?;

        let blob = self
            .store
            .get(&key)
            .await
            .map_err(|err| Status::internal(err.to_string()))?
            .ok_or_else(|| Status::not_found("blob not found"))?;
        if blob.metadata().uncompressed_size()
            != u64::try_from(parsed.expected_size)
                .map_err(|_| Status::invalid_argument("resource size must not be negative"))?
        {
            return Err(Status::not_found("blob size does not match resource name"));
        }
        let identity = BlobCodec::into_identity_data(blob)
            .await
            .map_err(|err| Status::internal(format!("failed to decompress blob: {err}")))?;
        key.algorithm
            .validate(&parsed.hash, parsed.expected_size, &identity)
            .map_err(|err| Status::internal(format!("stored blob failed validation: {err}")))?;

        let offset = usize::try_from(request.read_offset)
            .map_err(|_| Status::out_of_range("negative read offset"))?;
        if offset > identity.len() {
            return Err(Status::out_of_range("read offset larger than blob size"));
        }

        let available = identity[offset..].to_vec();
        let data = if response_encoding == StorageEncoding::Identity {
            let limit = match request.read_limit {
                0 => available.len(),
                limit => usize::try_from(limit)
                    .map_err(|_| Status::invalid_argument("negative read limit"))?
                    .min(available.len()),
            };
            available[..limit].to_vec()
        } else {
            let mut body = BlobCodec::from_identity_data(available, response_encoding)
                .await
                .map_err(|err| Status::internal(format!("failed to compress blob: {err}")))?
                .into_body();
            let mut data = Vec::new();
            body.read_to_end(&mut data).await.map_err(|err| {
                Status::internal(format!("failed to read compressed blob: {err}"))
            })?;
            data
        };

        let responses = data
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
        let mut parsed_resource = None;

        while let Some(chunk) = requests.message().await? {
            if chunk.write_offset != accumulator.len() as i64 {
                return Err(Status::invalid_argument("unexpected write offset"));
            }

            if let Some(expected_name) = &resource_name {
                if !chunk.resource_name.is_empty() && chunk.resource_name != *expected_name {
                    return Err(Status::invalid_argument("unexpected chunk name"));
                }
            } else {
                if chunk.resource_name.is_empty() {
                    return Err(Status::invalid_argument(
                        "expected chunk name on first write",
                    ));
                }

                let parsed = parse_bytestream_resource_name(chunk.resource_name.as_str())
                    .map_err(|err| Status::invalid_argument(err.to_string()))?;
                if parsed.upload_id.is_none() {
                    return Err(Status::invalid_argument(
                        "write resource name must contain an upload id",
                    ));
                }
                resource_name = Some(chunk.resource_name.clone());
                parsed_resource = Some(parsed);
            }

            accumulator.extend_from_slice(&chunk.data);

            if chunk.finish_write {
                let parsed = parsed_resource
                    .as_ref()
                    .ok_or_else(|| Status::invalid_argument("missing resource name"))?;
                let key = resolve_blob_key(parsed)?;
                let incoming_encoding = parsed.compression.storage_encoding()?;
                let expected_size = u64::try_from(parsed.expected_size)
                    .map_err(|_| Status::invalid_argument("resource size must not be negative"))?;
                let incoming = BlobRead::encoded(accumulator, incoming_encoding, expected_size);
                let committed_size = i64::try_from(incoming.metadata().stored_size())
                    .map_err(|_| Status::internal("blob exceeds expected size"))?;
                let identity = BlobCodec::into_identity_data(incoming)
                    .await
                    .map_err(|err| {
                        Status::invalid_argument(format!("invalid compressed blob: {err}"))
                    })?;
                key.algorithm
                    .validate(&parsed.hash, parsed.expected_size, &identity)
                    .map_err(|err| Status::invalid_argument(err.to_string()))?;

                let stored = BlobCodec::from_identity_data(identity, STORAGE_ENCODING)
                    .await
                    .map_err(|err| {
                        Status::internal(format!("failed to encode blob for storage: {err}"))
                    })?;

                self.store
                    .put(key, stored.metadata().clone(), stored.into_body())
                    .await
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

    let (algorithm, hash, size) =
        if digest_parts.len() >= 3 && digest_parts[0].parse::<DigestAlgorithm>().is_ok() {
            (
                Some(digest_parts[0].to_owned()),
                digest_parts[1],
                digest_parts[2],
            )
        } else if digest_parts.len() >= 2 {
            (None, digest_parts[0], digest_parts[1])
        } else {
            return Err(ParseResourceError::InvalidShape);
        };

    if hash.is_empty() {
        return Err(ParseResourceError::InvalidShape);
    }

    let expected_size = size.parse()?;
    if expected_size < 0 {
        return Err(ParseResourceError::NegativeSize);
    }

    Ok(ParsedReadResource {
        instance,
        upload_id,
        algorithm,
        hash: hash.to_owned(),
        compression,
        expected_size,
    })
}

fn resolve_blob_key(resource: &ParsedReadResource) -> Result<BlobKey, Status> {
    let algorithm = match &resource.algorithm {
        Some(name) => name.parse::<DigestAlgorithm>(),
        None => Ok(DigestAlgorithm::Sha256),
    }
    .map_err(Status::invalid_argument)?;

    Ok(BlobKey {
        instance: resource.instance.clone(),
        algorithm,
        hash: resource.hash.clone(),
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
            resolve_blob_key(&parsed).expect("SHA-256 should be inferred for an omitted algorithm");

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
        let actual = resolve_blob_key(&parsed).unwrap();

        let expected = BlobKey {
            instance: "test".to_string(),
            algorithm: DigestAlgorithm::Sha256,
            hash: "abc123".to_string(),
            kind: CacheKind::ContentAddressableStorage,
        };

        assert_eq!(actual, expected);
    }
}
