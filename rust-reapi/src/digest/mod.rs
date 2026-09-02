use std::str::FromStr;

use remote_execution_proto::build::bazel::remote::execution::v2::digest_function::Value as ProtoDigestFunction;
use sha2::{Digest as ShaDigest, Sha256};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DigestValidationError {
    #[error("digest size must not be negative")]
    NegativeSize,

    #[error("size mismatch, expected: {expected}, actual: {actual}")]
    SizeMismatch { expected: u64, actual: u64 },

    #[error("hash mismatch, expected: {expected}, actual: {actual}")]
    HashMismatch { expected: String, actual: String },
}

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

    pub fn hash_bytes(self, data: &[u8]) -> String {
        match self {
            Self::Sha256 => format!("{:x}", Sha256::digest(data)),
        }
    }

    pub fn validate(
        self,
        expected_hash: &str,
        expected_size: i64,
        data: &[u8],
    ) -> Result<(), DigestValidationError> {
        let expected_size =
            u64::try_from(expected_size).map_err(|_| DigestValidationError::NegativeSize)?;
        let actual_size = data.len() as u64;
        if actual_size != expected_size {
            return Err(DigestValidationError::SizeMismatch {
                expected: expected_size,
                actual: actual_size,
            });
        }

        let actual_hash = self.hash_bytes(data);
        if actual_hash != expected_hash {
            return Err(DigestValidationError::HashMismatch {
                expected: expected_hash.to_owned(),
                actual: actual_hash,
            });
        }

        Ok(())
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
