use std::sync::Arc;

use prost::Message;
use remote_execution_proto::build::bazel::remote::execution::v2::{
    Action, ActionResult, Digest, GetActionResultRequest, Tree, UpdateActionResultRequest,
    action_cache_server::ActionCache,
};
use tonic::{Code, Request, Response, Status};

use crate::{
    digest::DigestAlgorithm,
    storage::{BlobCodec, BlobKey, BlobStore, CacheKind, StorageEncoding},
};

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

pub struct ActionCacheService {
    store: Arc<dyn BlobStore + Send + Sync>,
}

impl ActionCacheService {
    pub fn new(store: Arc<dyn BlobStore + Send + Sync>) -> Self {
        Self { store }
    }

    fn cas_key(instance: &str, algorithm: DigestAlgorithm, digest: &Digest) -> BlobKey {
        BlobKey {
            instance: instance.to_string(),
            algorithm,
            hash: digest.hash.clone(),
            kind: CacheKind::ContentAddressableStorage,
        }
    }

    async fn validate_action_result_artifacts(
        &self,
        instance: &str,
        algorithm: DigestAlgorithm,
        action_result: &ActionResult,
        missing_code: Code,
    ) -> Result<(), Status> {
        let mut digests = Vec::new();

        for output_file in &action_result.output_files {
            let digest = output_file
                .digest
                .as_ref()
                .ok_or_else(|| Status::new(missing_code, "output file is missing its digest"))?;
            digests.push(digest.clone());
        }
        if let Some(stdout) = &action_result.stdout_digest {
            digests.push(stdout.clone());
        }
        if let Some(stderr) = &action_result.stderr_digest {
            digests.push(stderr.clone());
        }

        for output_directory in &action_result.output_directories {
            let tree_digest = output_directory.tree_digest.as_ref().ok_or_else(|| {
                Status::new(missing_code, "output directory is missing its tree digest")
            })?;
            let tree_bytes = self
                .store
                .get(&Self::cas_key(instance, algorithm, tree_digest))
                .await
                .map_err(|err| Status::internal(err.to_string()))?
                .ok_or_else(|| Status::new(missing_code, "output tree is missing from CAS"))
                .and_then(|blob| {
                    BlobCodec::into_identity_data(blob).map_err(|err| {
                        Status::internal(format!("failed to decompress output tree: {err}"))
                    })
                })?;
            let tree = Tree::decode(tree_bytes.as_slice())
                .map_err(|err| Status::internal(format!("invalid output tree: {err}")))?;
            collect_tree_file_digests(tree, &mut digests, missing_code)?;
        }

        for digest in digests {
            // NOTE: bazel stores empty stdout or something
            if digest.size_bytes == 0 && digest.hash == EMPTY_SHA256 {
                self.store
                    .put(
                        Self::cas_key(instance, algorithm, &digest),
                        Vec::new().into(),
                    )
                    .await
                    .map_err(|err| Status::internal(err.to_string()))?;
                continue;
            }
            if !self
                .store
                .contains(&Self::cas_key(instance, algorithm, &digest))
                .await
                .map_err(|err| Status::internal(err.to_string()))?
            {
                return Err(Status::new(
                    missing_code,
                    format!(
                        "referenced artifact is missing from CAS: {}/{}",
                        digest.hash, digest.size_bytes
                    ),
                ));
            }
        }

        Ok(())
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

        let algorithm = DigestAlgorithm::resolve_proto_value(request.digest_function)
            .map_err(Status::invalid_argument)?;

        let key = BlobKey {
            instance: request.instance_name.clone(),
            algorithm,
            hash: action_digest.hash.clone(),
            kind: CacheKind::ActionCache,
        };

        let bytes = self
            .store
            .get(&key)
            .await
            .map_err(|err| Status::internal(err.to_string()))?
            .ok_or_else(|| Status::not_found("action result not cached"))
            .and_then(|blob| {
                BlobCodec::into_identity_data(blob).map_err(|err| {
                    Status::internal(format!("failed to decompress action result: {err}"))
                })
            })?;

        let action_result = ActionResult::decode(bytes.as_slice())
            .map_err(|err| Status::internal(format!("invalid cached ActionResult: {err}")))?;

        self.validate_action_result_artifacts(
            &request.instance_name,
            algorithm,
            &action_result,
            Code::NotFound,
        )
        .await?;

        Ok(Response::new(action_result))
    }

    async fn update_action_result(
        &self,
        request: Request<UpdateActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        let request = request.into_inner();
        let action_digest = request
            .action_digest
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing action_digest"))?;
        let action_result = request
            .action_result
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing action_result"))?;

        let algorithm = DigestAlgorithm::resolve_proto_value(request.digest_function)
            .map_err(Status::invalid_argument)?;

        let action_bytes = self
            .store
            .get(&Self::cas_key(
                &request.instance_name,
                algorithm,
                action_digest,
            ))
            .await
            .map_err(|err| Status::internal(err.to_string()))?
            .ok_or_else(|| Status::failed_precondition("action is missing from CAS"))
            .and_then(|blob| {
                BlobCodec::into_identity_data(blob).map_err(|err| {
                    Status::failed_precondition(format!(
                        "failed to decompress Action in CAS: {err}"
                    ))
                })
            })?;
        let action = Action::decode(action_bytes.as_slice())
            .map_err(|err| Status::failed_precondition(format!("invalid Action in CAS: {err}")))?;
        let command_digest = action
            .command_digest
            .ok_or_else(|| Status::failed_precondition("Action has no command_digest"))?;

        if !self
            .store
            .contains(&Self::cas_key(
                &request.instance_name,
                algorithm,
                &command_digest,
            ))
            .await
            .map_err(|err| Status::internal(err.to_string()))?
        {
            return Err(Status::failed_precondition("command is missing from CAS"));
        }

        self.validate_action_result_artifacts(
            &request.instance_name,
            algorithm,
            action_result,
            Code::FailedPrecondition,
        )
        .await?;

        let action_key = BlobKey {
            instance: request.instance_name.clone(),
            algorithm,
            hash: request.action_digest.expect("validated above").hash.clone(),
            kind: CacheKind::ActionCache,
        };

        let action_result = &request
            .action_result
            .ok_or_else(|| Status::invalid_argument("missing action result"))?;
        let action_result_blob =
            BlobCodec::from_identity_data(action_result.encode_to_vec(), StorageEncoding::Zstd)
                .map_err(|err| {
                    Status::internal(format!("failed to compress action result: {err}"))
                })?;
        self.store
            .put(action_key, action_result_blob)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        Ok(Response::new(action_result.clone()))
    }
}

fn collect_tree_file_digests(
    tree: Tree,
    digests: &mut Vec<Digest>,
    missing_code: Code,
) -> Result<(), Status> {
    let root = tree
        .root
        .as_ref()
        .ok_or_else(|| Status::new(missing_code, "output tree has no root"))?;
    for file in &root.files {
        let digest = file
            .digest
            .as_ref()
            .ok_or_else(|| Status::new(missing_code, "output tree file is missing its digest"))?;
        digests.push(digest.clone());
    }
    for child in tree.children {
        for file in child.files {
            let digest = file.digest.as_ref().ok_or_else(|| {
                Status::new(missing_code, "output tree file is missing its digest")
            })?;
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
            algorithm: DigestAlgorithm::Sha256,
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
                expected.encode_to_vec().into(),
            )
            .await
            .expect("action cache entry should be stored");
        store
            .put(
                key(OUTPUT_HASH, CacheKind::ContentAddressableStorage),
                b"out".to_vec().into(),
            )
            .await
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
                action_result().encode_to_vec().into(),
            )
            .await
            .expect("action cache entry should be stored");
        let service = ActionCacheService::new(store);

        let error = service
            .get_action_result(request())
            .await
            .expect_err("missing output blob should invalidate cached result");

        assert_eq!(error.code(), Code::NotFound);
    }
}
