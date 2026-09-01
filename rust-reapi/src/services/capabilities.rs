use remote_execution_proto::build::bazel::remote::execution::v2::{
    ActionCacheUpdateCapabilities, CacheCapabilities, ExecutionCapabilities,
    GetCapabilitiesRequest, ServerCapabilities, capabilities_server::Capabilities,
    digest_function::Value as DigestFunction,
    symlink_absolute_path_strategy::Value as SymlinkAbsolutePathStrategy,
};
use semver_proto::build::bazel::semver::SemVer;
use tonic::{Request, Response, Status};

const REAPI_VERSION: SemVer = SemVer {
    major: 2,
    minor: 12,
    patch: 0,
    prerelease: String::new(),
};

#[derive(Default)]
pub struct CapabilitiesService;

impl CapabilitiesService {
    pub fn new() -> Self {
        Self
    }
}

#[tonic::async_trait]
impl Capabilities for CapabilitiesService {
    async fn get_capabilities(
        &self,
        _request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<ServerCapabilities>, Status> {
        Ok(Response::new(ServerCapabilities {
            cache_capabilities: Some(CacheCapabilities {
                digest_functions: vec![DigestFunction::Sha256 as i32],
                action_cache_update_capabilities: Some(ActionCacheUpdateCapabilities {
                    update_enabled: true,
                }),
                cache_priority_capabilities: None,
                max_batch_total_size_bytes: 0,
                max_cas_blob_size_bytes: 0,
                symlink_absolute_path_strategy: SymlinkAbsolutePathStrategy::Disallowed as i32,
                // TODO: Suppot compressors
                supported_compressors: Vec::new(),
                supported_batch_update_compressors: Vec::new(),
                // TODO: Support splicing
                split_blob_support: false,
                splice_blob_support: false,
                fast_cdc_2020_params: None,
                rep_max_cdc_params: None,
            }),
            execution_capabilities: Some(ExecutionCapabilities {
                digest_function: DigestFunction::Sha256 as i32,
                exec_enabled: false,
                execution_priority_capabilities: None,
                supported_node_properties: Vec::new(),
                digest_functions: vec![DigestFunction::Sha256 as i32],
            }),
            deprecated_api_version: None,
            low_api_version: Some(REAPI_VERSION.clone()),
            high_api_version: Some(REAPI_VERSION.clone()),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reports_supported_cache_features_only() {
        let response = CapabilitiesService::new()
            .get_capabilities(Request::new(GetCapabilitiesRequest::default()))
            .await
            .expect("capabilities request should succeed")
            .into_inner();

        let cache = response.cache_capabilities.expect("cache capabilities");
        assert_eq!(cache.digest_functions, vec![DigestFunction::Sha256 as i32]);
        assert!(!cache.split_blob_support);
        assert!(!cache.splice_blob_support);
        assert!(cache.fast_cdc_2020_params.is_none());
        assert!(cache.rep_max_cdc_params.is_none());

        let execution = response
            .execution_capabilities
            .expect("execution capabilities");
        assert!(!execution.exec_enabled);
        assert_eq!(
            execution.digest_functions,
            vec![DigestFunction::Sha256 as i32]
        );
        assert_eq!(response.low_api_version, response.high_api_version);
    }
}
