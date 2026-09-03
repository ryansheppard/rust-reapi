use async_trait::async_trait;
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
    pub(crate) encoding: StorageEncoding,
    pub(crate) uncompressed_size: u64,
    pub(crate) stored_size: u64,
}

impl StoredBlobMetadata {
    pub fn encoding(&self) -> StorageEncoding {
        self.encoding
    }

    pub fn uncompressed_size(&self) -> u64 {
        self.uncompressed_size
    }

    pub fn stored_size(&self) -> u64 {
        self.stored_size
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct StoredBlob {
    pub(crate) data: Vec<u8>,
    pub(crate) metadata: StoredBlobMetadata,
}

impl StoredBlob {
    pub fn identity(data: Vec<u8>) -> Self {
        let size = data.len() as u64;
        Self::encoded(data, StorageEncoding::Identity, size)
    }

    pub fn encoded(data: Vec<u8>, encoding: StorageEncoding, uncompressed_size: u64) -> Self {
        let stored_size = data.len() as u64;
        Self {
            data,
            metadata: StoredBlobMetadata {
                encoding,
                uncompressed_size,
                stored_size,
            },
        }
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn into_data(self) -> Vec<u8> {
        self.data
    }

    pub fn metadata(&self) -> &StoredBlobMetadata {
        &self.metadata
    }
}

impl From<Vec<u8>> for StoredBlob {
    fn from(data: Vec<u8>) -> Self {
        Self::identity(data)
    }
}

#[async_trait]
pub trait BlobStore {
    async fn put(&self, key: BlobKey, data: StoredBlob) -> Result<(), StorageError>;
    async fn get(&self, key: &BlobKey) -> Result<Option<StoredBlob>, StorageError>;
    async fn contains(&self, key: &BlobKey) -> Result<bool, StorageError>;
}
