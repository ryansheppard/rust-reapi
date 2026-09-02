use remote_execution_proto::build::bazel::remote::execution::v2::compressor;
use thiserror::Error;

use crate::digest::DigestAlgorithm;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("storage unavailable: {0}")]
    Unavailable(String),
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum CacheKind {
    ContentAddressableStorage,
    ActionCache,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BlobKey {
    pub instance: String,
    pub algorithm: DigestAlgorithm,
    pub hash: String,
    pub kind: CacheKind,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum StorageEncoding {
    Identity,
    Zstd,
}

impl StorageEncoding {
    pub fn from_proto_value(value: i32) -> Option<Self> {
        match compressor::Value::try_from(value).ok()? {
            compressor::Value::Identity => Some(Self::Identity),
            compressor::Value::Zstd => Some(Self::Zstd),
            compressor::Value::Deflate | compressor::Value::Brotli => None,
        }
    }

    pub fn as_proto_value(self) -> i32 {
        match self {
            Self::Identity => compressor::Value::Identity as i32,
            Self::Zstd => compressor::Value::Zstd as i32,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct StoredBlobMetadata {
    pub encoding: StorageEncoding,
    pub uncompressed_size: u64,
    pub stored_size: u64,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct StoredBlob {
    pub data: Vec<u8>,
    pub metadata: StoredBlobMetadata,
}

impl StoredBlob {
    pub fn identity(data: Vec<u8>) -> Self {
        let size = data.len() as u64;

        Self {
            data,
            metadata: StoredBlobMetadata {
                encoding: StorageEncoding::Identity,
                uncompressed_size: size,
                stored_size: size,
            },
        }
    }
}

impl From<Vec<u8>> for StoredBlob {
    fn from(data: Vec<u8>) -> Self {
        let size = data.len() as u64;
        Self {
            data,
            metadata: StoredBlobMetadata {
                encoding: StorageEncoding::Identity,
                uncompressed_size: size,
                stored_size: size,
            },
        }
    }
}

pub trait BlobStore {
    fn put(&self, key: BlobKey, data: StoredBlob) -> Result<(), StorageError>;
    fn get(&self, key: &BlobKey) -> Result<Option<StoredBlob>, StorageError>;
    fn contains(&self, key: &BlobKey) -> Result<bool, StorageError>;
}
