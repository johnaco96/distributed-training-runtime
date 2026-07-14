#![forbid(unsafe_code)]

pub mod runtime {
    pub mod v1 {
        tonic::include_proto!("runtime.v1");
    }
}

pub const PROTOCOL_MAJOR: u32 = 1;
pub const PROTOCOL_MINOR: u32 = 0;
pub const PROTOCOL_PATCH: u32 = 0;

pub fn protocol_version() -> runtime::v1::ProtocolVersion {
    runtime::v1::ProtocolVersion {
        major: PROTOCOL_MAJOR,
        minor: PROTOCOL_MINOR,
        patch: PROTOCOL_PATCH,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_version_constants_are_1_0_0() {
        assert_eq!(PROTOCOL_MAJOR, 1);
        assert_eq!(PROTOCOL_MINOR, 0);
        assert_eq!(PROTOCOL_PATCH, 0);
    }

    #[test]
    fn protocol_version_helper_returns_constants() {
        let version = protocol_version();
        assert_eq!(version.major, PROTOCOL_MAJOR);
        assert_eq!(version.minor, PROTOCOL_MINOR);
        assert_eq!(version.patch, PROTOCOL_PATCH);
    }
}
