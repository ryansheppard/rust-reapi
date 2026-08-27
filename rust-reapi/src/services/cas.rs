use std::pin::Pin;

use remote_execution_proto::build::bazel::remote::execution::v2::{
    BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
    BatchUpdateBlobsResponse, FindMissingBlobsRequest, FindMissingBlobsResponse, GetTreeRequest,
    GetTreeResponse, SpliceBlobRequest, SpliceBlobResponse, SplitBlobRequest, SplitBlobResponse,
    content_addressable_storage_server::ContentAddressableStorage,
};
use tonic::{Request, Response, Status};

pub struct CasService;

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
        _request: Request<FindMissingBlobsRequest>,
    ) -> Result<Response<FindMissingBlobsResponse>, Status> {
        Err(Status::unimplemented("not implemented yet"))
    }

    async fn batch_read_blobs(
        &self,
        _request: Request<BatchReadBlobsRequest>,
    ) -> Result<Response<BatchReadBlobsResponse>, Status> {
        Err(Status::unimplemented("not implemented yet"))
    }

    async fn batch_update_blobs(
        &self,
        _request: Request<BatchUpdateBlobsRequest>,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
        Err(Status::unimplemented("not implemented yet"))
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
