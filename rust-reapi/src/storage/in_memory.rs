use async_trait::async_trait;
use std::{collections::HashMap, sync::RwLock};

use crate::storage::blob_store::{BlobKey, BlobStore, StorageError, StoredBlob};

#[derive(Default)]
pub struct InMemoryStore {
    blobs: RwLock<HashMap<BlobKey, StoredBlob>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self {
            blobs: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl BlobStore for InMemoryStore {
    async fn get(&self, key: &BlobKey) -> Result<Option<StoredBlob>, StorageError> {
        let blobs = self
            .blobs
            .read()
            .map_err(|_| StorageError::Unavailable("lock poisoned".into()))?;
        Ok((blobs.get(key)).cloned())
    }
    async fn put(&self, key: BlobKey, data: StoredBlob) -> Result<(), StorageError> {
        let mut blobs = self
            .blobs
            .write()
            .map_err(|_| StorageError::Unavailable("lock poisoned".into()))?;
        blobs.insert(key, data);
        Ok(())
    }
    async fn contains(&self, key: &BlobKey) -> Result<bool, StorageError> {
        let blobs = self
            .blobs
            .read()
            .map_err(|_| StorageError::Unavailable("lock poisoned".into()))?;
        Ok(blobs.contains_key(key))
    }
}

#[cfg(test)]
mod tests {
    use crate::{digest::DigestAlgorithm, storage::CacheKind};

    use super::*;

    #[tokio::test]
    async fn test_get_does_not_exist() {
        let in_memory = InMemoryStore::new();
        let test_key = BlobKey {
            instance: "test".to_string(),
            algorithm: DigestAlgorithm::Sha256,
            hash: "123abc".to_string(),
            kind: CacheKind::ContentAddressableStorage,
        };

        let result = in_memory.get(&test_key).await.expect("get should not fail");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_does_not_contain() {
        let in_memory = InMemoryStore::new();
        let test_key = BlobKey {
            instance: "test".to_string(),
            algorithm: DigestAlgorithm::Sha256,
            hash: "123abc".to_string(),
            kind: CacheKind::ContentAddressableStorage,
        };

        let result = in_memory
            .contains(&test_key)
            .await
            .expect("contains should not fail");
        assert!(!result);
    }

    #[tokio::test]
    async fn test_put_contains_get_chain() {
        let in_memory = InMemoryStore::new();
        let test_key = BlobKey {
            instance: "test".to_string(),
            algorithm: DigestAlgorithm::Sha256,
            hash: "123abc".to_string(),
            kind: CacheKind::ContentAddressableStorage,
        };

        in_memory
            .put(test_key.clone(), StoredBlob::identity(vec![1, 2, 3]))
            .await
            .expect("put should work");

        assert!(
            in_memory
                .contains(&test_key)
                .await
                .expect("contains should succeed")
        );

        assert_eq!(
            in_memory.get(&test_key).await.expect("get should succeed"),
            Some(StoredBlob::identity(vec![1, 2, 3])),
        );
    }
}
