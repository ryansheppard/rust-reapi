use std::{io::Cursor, pin::Pin};

use async_trait::async_trait;
use remote_execution_proto::build::bazel::remote::execution::v2::compressor;
use thiserror::Error;
use tokio::io::AsyncRead;

use crate::digest::DigestAlgorithm;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("storage unavailable: {0}")]
    Unavailable(String),

    #[error("storage I/O error: {0}")]
    Io(#[from] std::io::Error),
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

pub type BlobReader = Pin<Box<dyn AsyncRead + Send + 'static>>;

// #[derive(Debug)]
pub struct BlobRead {
    metadata: StoredBlobMetadata,
    body: BlobReader,
}

impl BlobRead {
    pub fn new(metadata: StoredBlobMetadata, body: BlobReader) -> Self {
        Self { metadata, body }
    }

    pub fn metadata(&self) -> &StoredBlobMetadata {
        &self.metadata
    }

    pub fn into_body(self) -> BlobReader {
        self.body
    }

    pub fn identity(data: Vec<u8>) -> Self {
        let size = data.len() as u64;
        Self::encoded(data, StorageEncoding::Identity, size)
    }

    pub fn encoded(data: Vec<u8>, encoding: StorageEncoding, uncompressed_size: u64) -> Self {
        let stored_size = data.len() as u64;

        let body = Box::pin(Cursor::new(data));

        Self {
            body,
            metadata: StoredBlobMetadata {
                encoding,
                uncompressed_size,
                stored_size,
            },
        }
    }
}

#[async_trait]
pub trait BlobStore {
    async fn get(&self, key: &BlobKey) -> Result<Option<BlobRead>, StorageError>;
    async fn put(
        &self,
        key: BlobKey,
        metadata: StoredBlobMetadata,
        body: BlobReader,
    ) -> Result<(), StorageError>;
    async fn contains(&self, key: &BlobKey) -> Result<bool, StorageError>;
}
