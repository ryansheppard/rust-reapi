use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?).join("..");
    let googleapis = workspace.join("vendor/googleapis");

    let protos = [googleapis.join("google/bytestream/bytestream.proto")];

    unsafe {
        env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }

    tonic_prost_build::configure()
        .include_file("_protos.rs")
        .compile_protos(&protos, &[googleapis, protoc_bin_vendored::include_path()?])?;

    println!("cargo:rerun-if-changed=../vendor/googleapis/");
    Ok(())
}
