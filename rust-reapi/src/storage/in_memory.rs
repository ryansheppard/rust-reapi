use std::{collections::HashMap, sync::RwLock};

use crate::storage::blob_store::{BlobKey, BlobStore, StorageError};

#[derive(Default)]
pub struct InMemoryStore {
    blobs: RwLock<HashMap<BlobKey, Vec<u8>>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self {
            blobs: RwLock::new(HashMap::new()),
        }
    }
}

impl BlobStore for InMemoryStore {
    fn get(&self, key: &BlobKey) -> Result<Option<Vec<u8>>, StorageError> {
        let blobs = self
            .blobs
            .read()
            .map_err(|_| StorageError::Unavailable("lock poisoned".into()))?;
        Ok((blobs.get(key)).cloned())
    }
    fn put(&self, key: BlobKey, data: Vec<u8>) -> Result<(), StorageError> {
        let mut blobs = self
            .blobs
            .write()
            .map_err(|_| StorageError::Unavailable("lock poisoned".into()))?;
        blobs.insert(key, data);
        Ok(())
    }
    fn contains(&self, key: &BlobKey) -> Result<bool, StorageError> {
        let blobs = self
            .blobs
            .read()
            .map_err(|_| StorageError::Unavailable("lock poisoned".into()))?;
        Ok(blobs.contains_key(key))
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::CacheKind;

    use super::*;

    #[test]
    fn test_get_does_not_exist() {
        let in_memory = InMemoryStore::new();
        let test_key = BlobKey {
            instance: "test".to_string(),
            algorithm: "sha256".to_string(),
            hash: "123abc".to_string(),
            kind: CacheKind::ContentAddressableStorage,
        };

        let result = in_memory.get(&test_key).expect("get should not fail");
        assert!(result.is_none());
    }

    #[test]
    fn test_does_not_contain() {
        let in_memory = InMemoryStore::new();
        let test_key = BlobKey {
            instance: "test".to_string(),
            algorithm: "sha256".to_string(),
            hash: "123abc".to_string(),
            kind: CacheKind::ContentAddressableStorage,
        };

        let result = in_memory
            .contains(&test_key)
            .expect("contains should not fail");
        assert!(!result);
    }

    #[test]
    fn test_put_contains_get_chain() {
        let in_memory = InMemoryStore::new();
        let test_key = BlobKey {
            instance: "test".to_string(),
            algorithm: "sha256".to_string(),
            hash: "123abc".to_string(),
            kind: CacheKind::ContentAddressableStorage,
        };

        in_memory
            .put(test_key.clone(), vec![1, 2, 3])
            .expect("put should work");

        assert!(
            in_memory
                .contains(&test_key)
                .expect("contains should suceed")
        );

        assert_eq!(
            in_memory.get(&test_key).expect("get should suceed"),
            Some(vec![1, 2, 3]),
        );
    }
}
