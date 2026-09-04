use async_trait::async_trait;
use std::io::Cursor;
use std::{collections::HashMap, sync::RwLock};
use tokio::io::AsyncReadExt;

use crate::storage::{
    StoredBlobMetadata,
    blob_store::{BlobKey, BlobRead, BlobReader, BlobStore, StorageError},
};

// #[derive(Debug, PartialEq, Eq, Hash, Clone)]
#[derive(Debug, Clone)]
struct StoredBlob {
    data: Vec<u8>,
    metadata: StoredBlobMetadata,
}

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
    async fn get(&self, key: &BlobKey) -> Result<Option<BlobRead>, StorageError> {
        let blob = {
            let blobs = self
                .blobs
                .read()
                .map_err(|_| StorageError::Unavailable("lock poisoned".into()))?;

            blobs.get(key).cloned()
        };

        Ok(blob.map(|blob| BlobRead::new(blob.metadata, Box::pin(Cursor::new(blob.data)))))
    }

    async fn put(
        &self,
        key: BlobKey,
        metadata: StoredBlobMetadata,
        mut body: BlobReader,
    ) -> Result<(), StorageError> {
        let mut data = Vec::new();
        body.read_to_end(&mut data).await?;

        let blob = StoredBlob { metadata, data };

        let mut blobs = self
            .blobs
            .write()
            .map_err(|_| StorageError::Unavailable("lock poisoned".into()))?;

        blobs.insert(key, blob);

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
    use crate::{
        digest::DigestAlgorithm,
        storage::{CacheKind, StorageEncoding},
    };

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

        let blob = BlobRead::identity(vec![1, 2, 3]);
        in_memory
            .put(test_key.clone(), blob.metadata().clone(), blob.into_body())
            .await
            .expect("put should work");

        assert!(
            in_memory
                .contains(&test_key)
                .await
                .expect("contains should succeed")
        );

        let blob = in_memory
            .get(&test_key)
            .await
            .expect("get should succeed")
            .expect("blob should be present");
        assert_eq!(blob.metadata().encoding(), StorageEncoding::Identity);
        assert_eq!(blob.metadata().uncompressed_size(), 3);
        let mut body = blob.into_body();
        let mut data = Vec::new();
        body.read_to_end(&mut data)
            .await
            .expect("blob should be readable");
        assert_eq!(data, vec![1, 2, 3]);
    }
}
