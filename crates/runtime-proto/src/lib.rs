#![forbid(unsafe_code)]

pub mod runtime {
    pub mod v1 {
        tonic::include_proto!("runtime.v1");
    }
}
