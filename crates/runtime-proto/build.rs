use std::env;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../../proto/runtime.proto");
    println!("cargo:rerun-if-changed=../../proto");

    let mut includes = vec!["../../proto".to_string(), "../../".to_string()];
    if let Ok(include) = env::var("PROTOC_INCLUDE") {
        includes.push(include);
    } else if let Ok(include) = env::var("PROTOC_INCLUDE_DIR") {
        includes.push(include);
    } else {
        for candidate in [
            "/usr/include",
            "/usr/local/include",
            "/opt/homebrew/include",
            "/opt/local/include",
        ] {
            if Path::new(candidate)
                .join("google/protobuf/timestamp.proto")
                .exists()
            {
                includes.push(candidate.to_string());
                break;
            }
        }
    }
    let include_refs: Vec<&str> = includes.iter().map(String::as_str).collect();

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["../../proto/runtime.proto"], &include_refs)
        .map_err(|err| {
            format!(
                "failed to compile runtime.proto; protobuf includes missing? searched include roots: {}. \
set PROTOC_INCLUDE (or PROTOC_INCLUDE_DIR) to the directory that contains google/protobuf/timestamp.proto. original error: {err}",
                includes.join(", ")
            )
        })?;

    Ok(())
}
