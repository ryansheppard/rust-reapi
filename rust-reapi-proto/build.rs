use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?).join("..");
    let remote_apis = workspace.join("vendor/remote-apis");
    let googleapis = workspace.join("vendor/googleapis");

    let protos = [
        remote_apis.join("build/bazel/semver/semver.proto"),
        remote_apis.join("build/bazel/remote/execution/v2/remote_execution.proto"),
        remote_apis.join("build/bazel/remote/asset/v1/remote_asset.proto"),
        googleapis.join("google/api/http.proto"),
        googleapis.join("google/api/annotations.proto"),
        googleapis.join("google/longrunning/operations.proto"),
        googleapis.join("google/rpc/status.proto"),
    ];

    unsafe {
        env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }

    tonic_prost_build::configure()
        .include_file("_protos.rs")
        .compile_protos(
            &protos,
            &[
                remote_apis,
                googleapis,
                protoc_bin_vendored::include_path()?,
            ],
        )?;

    println!("cargo:rerun-if-changed=../vendor/remote-apis/");
    println!("cargo:rerun-if-changed=../vendor/googleapis/");
    Ok(())
}
