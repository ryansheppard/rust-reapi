use std::io::Cursor;

use crate::storage::{
    StoredBlobMetadata,
    blob_store::{BlobRead, StorageEncoding},
};
use thiserror::Error;
use tokio::io::AsyncReadExt;

/// Maximum uncompressed size accepted by the in-process blob codec (100 MiB).
pub const MAX_DECOMPRESSED_BLOB_SIZE: u64 = 100 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum CompressionError {
    #[error("unsupported compression method")]
    Unsupported,

    #[error("compression failed: {0}")]
    Io(#[from] std::io::Error),

    #[error("size mismatch, expected: {0}, actual: {1}")]
    SizeMismatch(u64, u64),

    #[error("decompressed size {actual} exceeds limit of {limit} bytes")]
    DecompressedSizeLimitExceeded { actual: u64, limit: u64 },
}

pub struct BlobCodec;

impl BlobCodec {
    const RESPONSE_PRIORITY: &'static [StorageEncoding] =
        &[StorageEncoding::Zstd, StorageEncoding::Identity];

    pub async fn into_identity_data(blob: BlobRead) -> Result<Vec<u8>, CompressionError> {
        let mut body = Self::transcode(blob, StorageEncoding::Identity)
            .await?
            .into_body();
        let mut data = Vec::new();
        body.read_to_end(&mut data).await?;
        Ok(data)
    }

    pub async fn from_identity_data(
        data: Vec<u8>,
        target: StorageEncoding,
    ) -> Result<BlobRead, CompressionError> {
        Self::transcode(BlobRead::identity(data), target).await
    }

    pub async fn transcode(
        blob: BlobRead,
        target: StorageEncoding,
    ) -> Result<BlobRead, CompressionError> {
        let metadata = blob.metadata().clone();
        if metadata.uncompressed_size > MAX_DECOMPRESSED_BLOB_SIZE {
            return Err(CompressionError::DecompressedSizeLimitExceeded {
                actual: metadata.uncompressed_size,
                limit: MAX_DECOMPRESSED_BLOB_SIZE,
            });
        }

        if metadata.encoding == target {
            return Ok(blob);
        }

        let mut body = blob.into_body();
        let mut data = Vec::new();
        body.read_to_end(&mut data).await?;

        data = match (metadata.encoding, target) {
            (StorageEncoding::Identity, StorageEncoding::Zstd) => {
                if data.len() as u64 != metadata.uncompressed_size {
                    return Err(CompressionError::SizeMismatch(
                        metadata.uncompressed_size,
                        data.len() as u64,
                    ));
                }
                compress_zstd(&data)?
            }
            (StorageEncoding::Zstd, StorageEncoding::Identity) => {
                decompress_zstd(&data, metadata.uncompressed_size)?
            }
            _ => return Err(CompressionError::Unsupported),
        };

        Ok(BlobRead::new(
            StoredBlobMetadata {
                encoding: target,
                stored_size: data.len() as u64,
                uncompressed_size: metadata.uncompressed_size,
            },
            Box::pin(Cursor::new(data)),
        ))
    }

    pub fn select_batch_read_response_encoding(
        stored: StorageEncoding,
        acceptable: &[StorageEncoding],
    ) -> StorageEncoding {
        if stored != StorageEncoding::Identity && acceptable.contains(&stored) {
            return stored;
        }

        Self::RESPONSE_PRIORITY
            .iter()
            .copied()
            .find(|encoding| {
                *encoding == StorageEncoding::Identity || acceptable.contains(encoding)
            })
            .unwrap_or(StorageEncoding::Identity)
    }
}

fn compress_zstd(data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    Ok(zstd::stream::encode_all(
        data,
        zstd::DEFAULT_COMPRESSION_LEVEL,
    )?)
}

fn decompress_zstd(data: &[u8], expected_size: u64) -> Result<Vec<u8>, CompressionError> {
    use std::io::Read;

    let mut output = Vec::new();
    let decoder = zstd::stream::read::Decoder::new(data)?;

    decoder
        .take(expected_size.saturating_add(1))
        .read_to_end(&mut output)?;

    if output.len() as u64 != expected_size {
        return Err(CompressionError::SizeMismatch(
            expected_size,
            output.len() as u64,
        ));
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn identity_data_round_trips_through_zstd() -> Result<(), CompressionError> {
        let expected = b"action result payload".to_vec();

        let blob = BlobCodec::from_identity_data(expected.clone(), StorageEncoding::Zstd).await?;
        assert_eq!(blob.metadata().encoding(), StorageEncoding::Zstd);
        assert_eq!(blob.metadata().uncompressed_size(), expected.len() as u64);
        assert!(blob.metadata().stored_size() > 0);

        let actual = BlobCodec::into_identity_data(blob).await?;
        assert_eq!(actual, expected);
        Ok(())
    }

    #[tokio::test]
    async fn identity_helpers_preserve_identity_data() -> Result<(), CompressionError> {
        let expected = b"uncompressed payload".to_vec();

        let blob =
            BlobCodec::from_identity_data(expected.clone(), StorageEncoding::Identity).await?;
        assert_eq!(blob.metadata().encoding(), StorageEncoding::Identity);
        assert_eq!(blob.metadata().uncompressed_size(), expected.len() as u64);
        assert_eq!(blob.metadata().stored_size(), expected.len() as u64);

        let actual = BlobCodec::into_identity_data(blob).await?;
        assert_eq!(actual, expected);
        Ok(())
    }

    #[tokio::test]
    async fn transcode_is_a_no_op_when_encoding_matches() -> Result<(), CompressionError> {
        let expected = b"already compressed".to_vec();
        let blob = BlobCodec::from_identity_data(expected.clone(), StorageEncoding::Zstd).await?;

        let actual = BlobCodec::transcode(blob, StorageEncoding::Zstd).await?;
        assert_eq!(actual.metadata().encoding(), StorageEncoding::Zstd);
        assert_eq!(BlobCodec::into_identity_data(actual).await?, expected);
        Ok(())
    }

    #[tokio::test]
    async fn decompression_rejects_incorrect_uncompressed_size() -> Result<(), CompressionError> {
        let compressed = compress_zstd(b"size checked")?;
        let blob = BlobRead::encoded(compressed, StorageEncoding::Zstd, 3);

        assert!(matches!(
            BlobCodec::into_identity_data(blob).await,
            Err(CompressionError::SizeMismatch(3, 4))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn decompression_rejects_declared_size_above_limit_before_decoding() {
        let blob = BlobRead::encoded(
            b"not a zstd frame".to_vec(),
            StorageEncoding::Zstd,
            MAX_DECOMPRESSED_BLOB_SIZE + 1,
        );

        assert!(matches!(
            BlobCodec::into_identity_data(blob).await,
            Err(CompressionError::DecompressedSizeLimitExceeded {
                actual,
                limit: MAX_DECOMPRESSED_BLOB_SIZE,
            }) if actual == MAX_DECOMPRESSED_BLOB_SIZE + 1
        ));
    }

    #[tokio::test]
    async fn decompression_rejects_truncated_zstd() -> Result<(), CompressionError> {
        let mut compressed = compress_zstd(b"truncated payload")?;
        compressed.truncate(compressed.len() / 2);
        let blob = BlobRead::encoded(compressed, StorageEncoding::Zstd, 17);

        assert!(BlobCodec::into_identity_data(blob).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn compression_rejects_identity_data_with_wrong_declared_size() {
        let blob = BlobRead::encoded(b"size checked".to_vec(), StorageEncoding::Identity, 3);

        assert!(matches!(
            BlobCodec::transcode(blob, StorageEncoding::Zstd).await,
            Err(CompressionError::SizeMismatch(3, 12))
        ));
    }

    #[test]
    fn batch_read_selection_prefers_acceptable_stored_encoding() {
        assert_eq!(
            BlobCodec::select_batch_read_response_encoding(
                StorageEncoding::Zstd,
                &[StorageEncoding::Zstd],
            ),
            StorageEncoding::Zstd,
        );
    }

    #[test]
    fn batch_read_selection_falls_back_to_identity() {
        assert_eq!(
            BlobCodec::select_batch_read_response_encoding(StorageEncoding::Zstd, &[]),
            StorageEncoding::Identity,
        );
    }
}
