use std::str::FromStr;

use remote_execution_proto::build::bazel::remote::execution::v2::digest_function::Value as ProtoDigestFunction;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum DigestAlgorithm {
    Sha256,
}

impl DigestAlgorithm {
    pub const SUPPORTED: &[Self] = &[Self::Sha256];

    pub fn to_proto(self) -> ProtoDigestFunction {
        match self {
            Self::Sha256 => ProtoDigestFunction::Sha256,
        }
    }

    pub fn supported_digest_functions() -> Vec<i32> {
        Self::SUPPORTED
            .iter()
            .map(|algorithm| algorithm.to_proto() as i32)
            .collect()
    }

    pub fn resolve_proto_value(value: i32) -> Result<Self, &'static str> {
        let proto = ProtoDigestFunction::try_from(value).map_err(|_| "unknown digest function")?;

        match proto {
            ProtoDigestFunction::Unknown => Ok(Self::Sha256),
            value => Self::try_from(value),
        }
    }
}

impl TryFrom<ProtoDigestFunction> for DigestAlgorithm {
    type Error = &'static str;

    fn try_from(value: ProtoDigestFunction) -> Result<Self, Self::Error> {
        match value {
            ProtoDigestFunction::Sha256 => Ok(Self::Sha256),
            _ => Err("unsupported digest function"),
        }
    }
}

impl FromStr for DigestAlgorithm {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "sha256" => Ok(Self::Sha256),
            _ => Err("unsupported digest function"),
        }
    }
}
