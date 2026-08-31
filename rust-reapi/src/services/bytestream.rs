use std::{fmt::Error, num::ParseIntError, pin::Pin};

use bytestream_proto::google::bytestream::{
    QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest,
    WriteResponse, byte_stream_server::ByteStream,
};
use remote_execution_proto::build::bazel::remote::execution::v2::compressor;
use tonic::{Request, Response, Status};

use crate::storage::{BlobKey, BlobStore};

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
    algorithm: Option<String>,
    hash: String,
    expected_size: i64,
    compression: Compression,
}

pub struct ByteStreamService {
    store: Box<dyn BlobStore + Send + Sync>,
}

impl ByteStreamService {
    pub fn new<S>(store: S) -> Self
    where
        S: BlobStore + Send + Sync + 'static,
    {
        Self {
            store: Box::new(store),
        }
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
        _request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        Err(Status::unimplemented("not implemented yet"))
    }

    async fn write(
        &self,
        _request: Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        Err(Status::unimplemented("not implemented yet"))
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

    let instance = parts[..marker_index].join("/");
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
        algorithm: algorithm.to_owned(),
        hash: hash.to_owned(),
        compression,
        expected_size: size.parse()?,
    })
}

fn resolve_blob_key(
    resource: ParsedReadResource,
    supported_algorithms: &[&str],
) -> Result<BlobKey, Status> {
    let algorithm = match resource.algorithm {
        Some(algorithm) if supported_algorithms.contains(&algorithm.as_str()) => algorithm,
        Some(_) => return Err(Status::invalid_argument("unsupported digest function")),
        None => return Err(Status::unimplemented("missing digest not yet implemented")),
    };

    Ok(BlobKey {
        instance: resource.instance,
        algorithm,
        hash: resource.hash,
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
            algorithm: Some("sha256".to_string()),
            hash: "abc123".to_string(),
            compression: Compression::Identity,
            expected_size: 12,
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_resolve_blob_key() {
        let parsed = ParsedReadResource {
            instance: "test".to_string(),
            algorithm: Some("sha256".to_string()),
            hash: "abc123".to_string(),
            compression: Compression::Identity,
            expected_size: 12,
        };
        let actual = resolve_blob_key(parsed, &["sha256"]).unwrap();

        let expected = BlobKey {
            instance: "test".to_string(),
            algorithm: "sha256".to_string(),
            hash: "abc123".to_string(),
        };

        assert_eq!(actual, expected);
    }
}
