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
    } else if Path::new("/usr/include/google/protobuf/timestamp.proto").exists() {
        includes.push("/usr/include".to_string());
    }
    let include_refs: Vec<&str> = includes.iter().map(String::as_str).collect();

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["../../proto/runtime.proto"], &include_refs)?;

    Ok(())
}
