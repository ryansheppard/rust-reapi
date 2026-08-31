use std::{pin::Pin, sync::Arc};

use remote_execution_proto::build::bazel::remote::execution::v2::{
    BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
    BatchUpdateBlobsResponse, FindMissingBlobsRequest, FindMissingBlobsResponse, GetTreeRequest,
    GetTreeResponse, SpliceBlobRequest, SpliceBlobResponse, SplitBlobRequest, SplitBlobResponse,
    batch_read_blobs_response::Response as BlobReadResponse,
    batch_update_blobs_response::Response as BlobUpdateResponse,
    content_addressable_storage_server::ContentAddressableStorage,
};
use status_proto::google::rpc::Status as RpcStatus;
use tonic::{Request, Response, Status};

use crate::storage::{BlobKey, BlobStore, CacheKind};

pub struct CasService {
    store: Arc<dyn BlobStore + Send + Sync>,
}

impl CasService {
    pub fn new(store: Arc<dyn BlobStore + Send + Sync>) -> Self {
        Self { store }
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

        for digest in request.blob_digests {
            let key = BlobKey {
                instance: request.instance_name.clone(),
                // TODO: Handle/infer digest functions
                algorithm: "sha256".to_string(),
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

        for compressor in request.acceptable_compressors {
            if compressor != 0 {
                return Err(Status::invalid_argument("unsupported compression method"));
            }
        }

        for digest in request.digests {
            let key = BlobKey {
                instance: request.instance_name.clone(),
                algorithm: "sha256".to_string(),
                hash: digest.hash.clone(),
                kind: CacheKind::ContentAddressableStorage,
            };

            let ret = self.store.get(&key);

            let resp = match ret {
                Ok(value) => match value {
                    Some(blob) => BlobReadResponse {
                        digest: Some(digest),
                        data: blob,
                        compressor: 0,
                        status: Some(RpcStatus {
                            code: 0,
                            message: String::new(),
                            details: vec![],
                        }),
                    },
                    None => BlobReadResponse {
                        digest: Some(digest),
                        data: Vec::new(),
                        compressor: 0,
                        status: Some(RpcStatus {
                            code: 5,
                            message: String::new(),
                            details: vec![],
                        }),
                    },
                },
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

        for req in request.requests {
            let Some(digest) = req.digest else {
                return Err(Status::invalid_argument("missing digest"));
            };

            // TODO: Handle compressor field: https://github.com/bazelbuild/remote-apis/blob/becdd8f9ff811df88a22d3eadd6341753d51d167/build/bazel/remote/execution/v2/remote_execution.proto#L2261
            if req.compressor != 0 {
                return Err(Status::invalid_argument("unsupported compression method"));
            }

            let key = BlobKey {
                instance: request.instance_name.clone(),
                algorithm: "sha256".to_string(),
                hash: digest.hash.clone(),
                kind: CacheKind::ContentAddressableStorage,
            };

            let status = match self.store.put(key, req.data) {
                Ok(()) => RpcStatus {
                    code: 0,
                    message: String::new(),
                    details: vec![],
                },
                Err(err) => RpcStatus {
                    code: 13,
                    message: err.to_string(),
                    details: vec![],
                },
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
