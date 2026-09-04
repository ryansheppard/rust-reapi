use prost::Message;
use remote_execution_proto::build::bazel::remote::execution::v2::{
    Action, ActionResult, Digest, GetActionResultRequest, OutputFile, UpdateActionResultRequest,
};
use rust_reapi::{
    digest::DigestAlgorithm,
    storage::{BlobCodec, BlobKey, BlobRead, BlobStore, CacheKind, StorageEncoding, StorageError},
};
use tonic::Request;

mod common;

const INSTANCE: &str = "test-instance";
const ACTION_HASH: &str = "action-hash";
const OUTPUT_HASH: &str = "output-hash";
const COMMAND_HASH: &str = "command-hash";

fn key(hash: &str, kind: CacheKind) -> BlobKey {
    BlobKey {
        instance: INSTANCE.to_string(),
        algorithm: DigestAlgorithm::Sha256,
        hash: hash.to_string(),
        kind,
    }
}

async fn put_blob(
    store: &impl BlobStore,
    key: BlobKey,
    blob: BlobRead,
) -> Result<(), StorageError> {
    store
        .put(key, blob.metadata().clone(), blob.into_body())
        .await
}

async fn put_identity(
    store: &impl BlobStore,
    key: BlobKey,
    data: Vec<u8>,
) -> Result<(), StorageError> {
    put_blob(store, key, BlobRead::identity(data)).await
}

#[tokio::test]
async fn client_gets_a_cached_result_when_referenced_artifacts_exist()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut cache, store, server) = common::action_cache_client().await?;
    let expected = ActionResult {
        output_files: vec![OutputFile {
            path: "out.txt".to_string(),
            digest: Some(Digest {
                hash: OUTPUT_HASH.to_string(),
                size_bytes: 3,
            }),
            ..Default::default()
        }],
        ..Default::default()
    };

    put_identity(
        &*store,
        key(ACTION_HASH, CacheKind::ActionCache),
        expected.encode_to_vec(),
    )
    .await?;
    put_identity(
        &*store,
        key(OUTPUT_HASH, CacheKind::ContentAddressableStorage),
        b"out".to_vec(),
    )
    .await?;

    let actual = cache
        .get_action_result(Request::new(GetActionResultRequest {
            instance_name: INSTANCE.to_string(),
            action_digest: Some(Digest {
                hash: ACTION_HASH.to_string(),
                size_bytes: 1,
            }),
            ..Default::default()
        }))
        .await?
        .into_inner();

    assert_eq!(actual, expected);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn client_updates_and_then_reads_an_action_result() -> Result<(), Box<dyn std::error::Error>>
{
    let (mut cache, store, server) = common::action_cache_client().await?;
    let action_digest = Digest {
        hash: ACTION_HASH.to_string(),
        size_bytes: 1,
    };
    let command_digest = Digest {
        hash: COMMAND_HASH.to_string(),
        size_bytes: 1,
    };
    let expected = ActionResult {
        output_files: vec![OutputFile {
            path: "out.txt".to_string(),
            digest: Some(Digest {
                hash: OUTPUT_HASH.to_string(),
                size_bytes: 3,
            }),
            ..Default::default()
        }],
        ..Default::default()
    };

    let action_blob = BlobCodec::from_identity_data(
        Action {
            command_digest: Some(command_digest.clone()),
            ..Default::default()
        }
        .encode_to_vec(),
        StorageEncoding::Zstd,
    )
    .await?;
    put_blob(
        &*store,
        key(ACTION_HASH, CacheKind::ContentAddressableStorage),
        action_blob,
    )
    .await?;
    put_identity(
        &*store,
        key(COMMAND_HASH, CacheKind::ContentAddressableStorage),
        b"command".to_vec(),
    )
    .await?;
    put_identity(
        &*store,
        key(OUTPUT_HASH, CacheKind::ContentAddressableStorage),
        b"out".to_vec(),
    )
    .await?;

    let updated = cache
        .update_action_result(Request::new(UpdateActionResultRequest {
            instance_name: INSTANCE.to_string(),
            action_digest: Some(action_digest.clone()),
            action_result: Some(expected.clone()),
            ..Default::default()
        }))
        .await?
        .into_inner();
    assert_eq!(updated, expected);
    let stored_result = store
        .get(&key(ACTION_HASH, CacheKind::ActionCache))
        .await?
        .ok_or("missing stored action result")?;
    assert_eq!(stored_result.metadata().encoding(), StorageEncoding::Zstd);

    let fetched = cache
        .get_action_result(Request::new(GetActionResultRequest {
            instance_name: INSTANCE.to_string(),
            action_digest: Some(action_digest),
            ..Default::default()
        }))
        .await?
        .into_inner();
    assert_eq!(fetched, expected);

    server.abort();
    Ok(())
}
