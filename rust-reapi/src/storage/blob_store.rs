use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("storage unavailable: {0}")]
    Unavailable(String),
}

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct BlobKey {
    pub instance: String,
    pub algorithm: String,
    pub hash: String,
}

pub trait BlobStore {
    fn put(&self, key: BlobKey, data: Vec<u8>) -> Result<(), StorageError>;
    fn get(&self, key: &BlobKey) -> Result<Option<Vec<u8>>, StorageError>;
    fn contains(&self, key: &BlobKey) -> Result<bool, StorageError>;
}
