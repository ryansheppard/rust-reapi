use remote_execution_proto::build::bazel::remote::execution::v2::{
    GetCapabilitiesRequest, compressor::Value as Compressor,
    digest_function::Value as DigestFunction,
};
use tonic::Request;

mod common;

#[tokio::test]
async fn client_receives_advertised_capabilities() -> Result<(), Box<dyn std::error::Error>> {
    let (mut capabilities, server) = common::capabilities_client().await?;

    let response = capabilities
        .get_capabilities(Request::new(GetCapabilitiesRequest {
            instance_name: "test".to_owned(),
        }))
        .await?
        .into_inner();

    let cache = response
        .cache_capabilities
        .ok_or("missing cache capabilities")?;
    assert_eq!(cache.digest_functions, vec![DigestFunction::Sha256 as i32]);
    assert!(
        cache
            .action_cache_update_capabilities
            .ok_or("missing action cache capabilities")?
            .update_enabled
    );
    assert_eq!(cache.supported_compressors, vec![Compressor::Zstd as i32]);
    assert_eq!(
        cache.supported_batch_update_compressors,
        vec![Compressor::Zstd as i32]
    );
    assert!(!cache.split_blob_support);
    assert!(!cache.splice_blob_support);

    let execution = response
        .execution_capabilities
        .ok_or("missing execution capabilities")?;
    assert!(!execution.exec_enabled);
    assert_eq!(
        execution.digest_functions,
        vec![DigestFunction::Sha256 as i32]
    );

    let low_api_version = response.low_api_version.ok_or("missing low API version")?;
    let high_api_version = response
        .high_api_version
        .ok_or("missing high API version")?;
    assert_eq!(
        (
            low_api_version.major,
            low_api_version.minor,
            low_api_version.patch
        ),
        (2, 0, 0)
    );
    assert_eq!(
        (
            high_api_version.major,
            high_api_version.minor,
            high_api_version.patch
        ),
        (2, 12, 0)
    );

    server.abort();
    Ok(())
}
