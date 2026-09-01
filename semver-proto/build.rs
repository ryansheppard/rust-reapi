use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?).join("..");
    let remote_apis = workspace.join("vendor/remote-apis");
    let protos = [remote_apis.join("build/bazel/semver/semver.proto")];

    unsafe {
        env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }

    tonic_prost_build::configure()
        .include_file("_protos.rs")
        .compile_protos(
            &protos,
            &[remote_apis, protoc_bin_vendored::include_path()?],
        )?;

    println!("cargo:rerun-if-changed=../vendor/remote-apis/");
    Ok(())
}
