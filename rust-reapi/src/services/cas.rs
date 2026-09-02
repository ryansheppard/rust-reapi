use std::{pin::Pin, sync::Arc};

use remote_execution_proto::build::bazel::remote::execution::v2::{
    BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
    BatchUpdateBlobsResponse, Digest, FindMissingBlobsRequest, FindMissingBlobsResponse,
    GetTreeRequest, GetTreeResponse, SpliceBlobRequest, SpliceBlobResponse, SplitBlobRequest,
    SplitBlobResponse, batch_read_blobs_response::Response as BlobReadResponse,
    batch_update_blobs_response::Response as BlobUpdateResponse,
    content_addressable_storage_server::ContentAddressableStorage,
};
use status_proto::google::rpc::Status as RpcStatus;
use tonic::{Code, Request, Response, Status};

use crate::{
    digest::DigestAlgorithm,
    storage::{BlobCodec, BlobKey, BlobStore, CacheKind, StorageEncoding, StoredBlob},
};

const STORAGE_ENCODING: StorageEncoding = StorageEncoding::Zstd;

pub struct CasService {
    store: Arc<dyn BlobStore + Send + Sync>,
}

impl CasService {
    pub fn new(store: Arc<dyn BlobStore + Send + Sync>) -> Self {
        Self { store }
    }

    fn put_verified_blob(
        &self,
        key: BlobKey,
        algorithm: DigestAlgorithm,
        digest: &Digest,
        data: Vec<u8>,
        encoding: StorageEncoding,
    ) -> Result<(), RpcStatus> {
        let expected_size = u64::try_from(digest.size_bytes)
            .map_err(|_| rpc_status(Code::InvalidArgument, "digest size must not be negative"))?;
        let incoming = StoredBlob::encoded(data, encoding, expected_size);
        let identity = BlobCodec::into_identity_data(incoming).map_err(|err| {
            rpc_status(
                Code::InvalidArgument,
                format!("invalid compressed blob: {err}"),
            )
        })?;

        algorithm
            .validate(&digest.hash, digest.size_bytes, &identity)
            .map_err(|err| rpc_status(Code::InvalidArgument, err.to_string()))?;

        let stored = BlobCodec::from_identity_data(identity, STORAGE_ENCODING).map_err(|err| {
            rpc_status(
                Code::Internal,
                format!("failed to encode blob for storage: {err}"),
            )
        })?;
        self.store
            .put(key, stored)
            .map_err(|err| rpc_status(Code::Internal, err.to_string()))
    }
}

fn rpc_status(code: Code, message: impl Into<String>) -> RpcStatus {
    RpcStatus {
        code: code as i32,
        message: message.into(),
        details: Vec::new(),
    }
}

#[tonic::async_trait]
impl ContentAddressableStorage for CasService {
    type GetTreeStream = Pin<
        Box<
            dyn tonic::codegen::tokio_stream::Stream<Item = Result<GetTreeResponse, Status>>
                + Send
                + 'static,
        >,
    >;

    async fn find_missing_blobs(
        &self,
        request: Request<FindMissingBlobsRequest>,
    ) -> Result<Response<FindMissingBlobsResponse>, Status> {
        let request = request.into_inner();
        let mut missing_blob_digests = Vec::new();

        let algorithm = DigestAlgorithm::resolve_proto_value(request.digest_function)
            .map_err(Status::invalid_argument)?;

        for digest in request.blob_digests {
            let key = BlobKey {
                instance: request.instance_name.clone(),
                algorithm,
                hash: digest.hash.clone(),
                kind: CacheKind::ContentAddressableStorage,
            };

            let exists = self
                .store
                .contains(&key)
                .map_err(|err| Status::internal(err.to_string()))?;

            if !exists {
                missing_blob_digests.push(digest);
            }
        }

        Ok(Response::new(FindMissingBlobsResponse {
            missing_blob_digests,
        }))
    }

    async fn batch_read_blobs(
        &self,
        request: Request<BatchReadBlobsRequest>,
    ) -> Result<Response<BatchReadBlobsResponse>, Status> {
        let request = request.into_inner();
        let mut responses = Vec::new();

        let algorithm = DigestAlgorithm::resolve_proto_value(request.digest_function)
            .map_err(Status::invalid_argument)?;

        let acceptable_encodings = request
            .acceptable_compressors
            .iter()
            .copied()
            .filter_map(StorageEncoding::from_proto_value)
            .collect::<Vec<_>>();

        for digest in request.digests {
            let key = BlobKey {
                instance: request.instance_name.clone(),
                algorithm,
                hash: digest.hash.clone(),
                kind: CacheKind::ContentAddressableStorage,
            };

            let ret = self.store.get(&key);

            let resp = match ret {
                Err(err) => BlobReadResponse {
                    digest: Some(digest),
                    data: Vec::new(),
                    compressor: 0,
                    status: Some(RpcStatus {
                        code: 13,
                        message: err.to_string(),
                        details: vec![],
                    }),
                },
                Ok(None) => BlobReadResponse {
                    digest: Some(digest),
                    data: Vec::new(),
                    compressor: 0,
                    status: Some(RpcStatus {
                        code: 5,
                        message: String::new(),
                        details: vec![],
                    }),
                },
                Ok(Some(blob)) => {
                    let target = BlobCodec::select_batch_read_response_encoding(
                        blob.metadata.encoding,
                        &acceptable_encodings,
                    );

                    match BlobCodec::transcode(blob, target) {
                        Ok(blob) => BlobReadResponse {
                            digest: Some(digest),
                            data: blob.data,
                            compressor: blob.metadata.encoding.as_proto_value(),
                            status: Some(RpcStatus {
                                code: 0,
                                message: String::new(),
                                details: vec![],
                            }),
                        },
                        Err(err) => BlobReadResponse {
                            digest: Some(digest),
                            data: Vec::new(),
                            compressor: StorageEncoding::Identity.as_proto_value(),
                            status: Some(RpcStatus {
                                code: 13,
                                message: err.to_string(),
                                details: vec![],
                            }),
                        },
                    }
                }
            };

            responses.push(resp);
        }

        Ok(Response::new(BatchReadBlobsResponse { responses }))
    }

    async fn batch_update_blobs(
        &self,
        request: Request<BatchUpdateBlobsRequest>,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
        let request = request.into_inner();
        let mut responses = Vec::new();

        let algorithm = DigestAlgorithm::resolve_proto_value(request.digest_function)
            .map_err(Status::invalid_argument)?;

        for req in request.requests {
            let Some(digest) = req.digest else {
                responses.push(BlobUpdateResponse {
                    digest: None,
                    status: Some(rpc_status(Code::InvalidArgument, "missing digest")),
                });
                continue;
            };

            let Some(encoding) = StorageEncoding::from_proto_value(req.compressor) else {
                responses.push(BlobUpdateResponse {
                    digest: Some(digest),
                    status: Some(rpc_status(
                        Code::InvalidArgument,
                        "unsupported compression method",
                    )),
                });
                continue;
            };

            let key = BlobKey {
                instance: request.instance_name.clone(),
                algorithm,
                hash: digest.hash.clone(),
                kind: CacheKind::ContentAddressableStorage,
            };

            let status = match self.put_verified_blob(key, algorithm, &digest, req.data, encoding) {
                Ok(()) => rpc_status(Code::Ok, String::new()),
                Err(status) => status,
            };

            responses.push(BlobUpdateResponse {
                digest: Some(digest),
                status: Some(status),
            });
        }

        Ok(Response::new(BatchUpdateBlobsResponse { responses }))
    }

    async fn get_tree(
        &self,
        _request: Request<GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        Err(Status::unimplemented("not implemented yet"))
    }

    async fn split_blob(
        &self,
        _request: Request<SplitBlobRequest>,
    ) -> Result<Response<SplitBlobResponse>, Status> {
        Err(Status::unimplemented("not implemented yet"))
    }

    async fn splice_blob(
        &self,
        _request: Request<SpliceBlobRequest>,
    ) -> Result<Response<SpliceBlobResponse>, Status> {
        Err(Status::unimplemented("not implemented yet"))
    }
}
