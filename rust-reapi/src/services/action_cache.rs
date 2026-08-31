use std::sync::Arc;

use prost::Message;
use remote_execution_proto::build::bazel::remote::execution::v2::{
    ActionResult, Digest, GetActionResultRequest, Tree, UpdateActionResultRequest,
    action_cache_server::ActionCache,
};
use tonic::{Request, Response, Status};

use crate::storage::{BlobKey, BlobStore, CacheKind};

pub struct ActionCacheService {
    store: Arc<dyn BlobStore + Send + Sync>,
}

impl ActionCacheService {
    pub fn new(store: Arc<dyn BlobStore + Send + Sync>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl ActionCache for ActionCacheService {
    async fn get_action_result(
        &self,
        request: Request<GetActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        let request = request.into_inner();

        let action_digest = request
            .action_digest
            .ok_or_else(|| Status::invalid_argument("missing action_digest"))?;

        let key = BlobKey {
            instance: request.instance_name.clone(),
            algorithm: "sha256".to_string(),
            hash: action_digest.hash.clone(),
            kind: CacheKind::ActionCache,
        };

        let bytes = self
            .store
            .get(&key)
            .map_err(|err| Status::internal(err.to_string()))?
            .ok_or_else(|| Status::not_found("action result not cached"))?;

        let action_result = ActionResult::decode(bytes.as_slice())
            .map_err(|err| Status::internal(format!("invalid cached ActionResult: {err}")))?;

        let mut digests: Vec<Digest> = Vec::new();

        for output_file in &action_result.output_files {
            let digest = output_file
                .digest
                .as_ref()
                .ok_or_else(|| Status::not_found("action result not cached"))?;

            digests.push(digest.clone());
        }

        if let Some(stdout) = &action_result.stdout_digest {
            digests.push(stdout.clone());
        }
        if let Some(stderr) = &action_result.stderr_digest {
            digests.push(stderr.clone());
        }

        for dir in &action_result.output_directories {
            let digest = dir
                .tree_digest
                .as_ref()
                .ok_or_else(|| Status::not_found("digest not found"))?;

            let cas_key = BlobKey {
                instance: request.instance_name.clone(),
                algorithm: "sha256".to_string(),
                hash: digest.hash.clone(),
                kind: CacheKind::ContentAddressableStorage,
            };

            let tree = self
                .store
                .get(&cas_key)
                .map_err(|err| Status::internal(err.to_string()))?
                .ok_or_else(|| Status::not_found("action result not cached"))?;
            let tree = Tree::decode(tree.as_slice())
                .map_err(|err| Status::internal(format!("invalid cached tree: {err}")))?;
            collect_tree_file_digests(tree, &mut digests)?;
        }

        for digest in digests {
            let key = BlobKey {
                instance: request.instance_name.clone(),
                algorithm: "sha256".to_string(),
                hash: digest.hash.clone(),
                kind: CacheKind::ContentAddressableStorage,
            };
            if !self
                .store
                .contains(&key)
                .map_err(|err| Status::internal(err.to_string()))?
            {
                return Err(Status::not_found("action result not cached"));
            }
        }

        Ok(Response::new(action_result))
    }

    async fn update_action_result(
        &self,
        request: Request<UpdateActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        let request = request.into_inner();
        // blob key
        // contains?
        // store
        // return
        Err(Status::unimplemented("not implemented yet"))
    }
}

fn collect_tree_file_digests(tree: Tree, digests: &mut Vec<Digest>) -> Result<(), Status> {
    let root = tree
        .root
        .as_ref()
        .ok_or_else(|| Status::not_found("cached tree has no root"))?;
    for file in &root.files {
        let digest = file
            .digest
            .as_ref()
            .ok_or_else(|| Status::not_found("file not found"))?;
        digests.push(digest.clone());
    }
    for child in tree.children {
        for file in child.files {
            let digest = file
                .digest
                .as_ref()
                .ok_or_else(|| Status::not_found("file not found"))?;
            digests.push(digest.clone());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use remote_execution_proto::build::bazel::remote::execution::v2::{Digest, OutputFile};
    use tonic::Code;

    use super::*;
    use crate::storage::InMemoryStore;

    const INSTANCE: &str = "test-instance";
    const ACTION_HASH: &str = "action-hash";
    const OUTPUT_HASH: &str = "output-hash";

    fn key(hash: &str, kind: CacheKind) -> BlobKey {
        BlobKey {
            instance: INSTANCE.to_string(),
            algorithm: "sha256".to_string(),
            hash: hash.to_string(),
            kind,
        }
    }

    fn request() -> Request<GetActionResultRequest> {
        Request::new(GetActionResultRequest {
            instance_name: INSTANCE.to_string(),
            action_digest: Some(Digest {
                hash: ACTION_HASH.to_string(),
                size_bytes: 1,
            }),
            ..Default::default()
        })
    }

    fn action_result() -> ActionResult {
        ActionResult {
            output_files: vec![OutputFile {
                path: "out.txt".to_string(),
                digest: Some(Digest {
                    hash: OUTPUT_HASH.to_string(),
                    size_bytes: 3,
                }),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn returns_not_found_when_action_is_not_cached() {
        let store = Arc::new(InMemoryStore::new());
        let service = ActionCacheService::new(store);

        let error = service
            .get_action_result(request())
            .await
            .expect_err("missing action cache entry should fail");

        assert_eq!(error.code(), Code::NotFound);
    }

    #[tokio::test]
    async fn returns_cached_result_when_all_output_blobs_exist() {
        let store = Arc::new(InMemoryStore::new());
        let expected = action_result();
        store
            .put(
                key(ACTION_HASH, CacheKind::ActionCache),
                expected.encode_to_vec(),
            )
            .expect("action cache entry should be stored");
        store
            .put(
                key(OUTPUT_HASH, CacheKind::ContentAddressableStorage),
                b"out".to_vec(),
            )
            .expect("output blob should be stored");
        let service = ActionCacheService::new(store);

        let actual = service
            .get_action_result(request())
            .await
            .expect("cached result should be returned")
            .into_inner();

        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn returns_not_found_when_cached_output_blob_is_missing() {
        let store = Arc::new(InMemoryStore::new());
        store
            .put(
                key(ACTION_HASH, CacheKind::ActionCache),
                action_result().encode_to_vec(),
            )
            .expect("action cache entry should be stored");
        let service = ActionCacheService::new(store);

        let error = service
            .get_action_result(request())
            .await
            .expect_err("missing output blob should invalidate cached result");

        assert_eq!(error.code(), Code::NotFound);
    }
}
