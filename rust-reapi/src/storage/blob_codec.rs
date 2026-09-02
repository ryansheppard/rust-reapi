use crate::storage::blob_store::{StorageEncoding, StoredBlob};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CompressionError {
    #[error("unsupported compression method")]
    Unsupported,

    #[error("compression failed: {0}")]
    Io(#[from] std::io::Error),

    #[error("size mismatch, expected: {0}, actual: {1}")]
    SizeMismatch(u64, u64),
}

pub struct BlobCodec;

impl BlobCodec {
    const RESPONSE_PRIORITY: &'static [StorageEncoding] =
        &[StorageEncoding::Zstd, StorageEncoding::Identity];

    pub fn into_identity_data(blob: StoredBlob) -> Result<Vec<u8>, CompressionError> {
        Self::transcode(blob, StorageEncoding::Identity).map(|blob| blob.data)
    }

    pub fn from_identity_data(
        data: Vec<u8>,
        target: StorageEncoding,
    ) -> Result<StoredBlob, CompressionError> {
        Self::transcode(StoredBlob::identity(data), target)
    }

    pub fn transcode(
        mut blob: StoredBlob,
        target: StorageEncoding,
    ) -> Result<StoredBlob, CompressionError> {
        if blob.metadata.encoding == target {
            return Ok(blob);
        }

        blob.data = match (blob.metadata.encoding, target) {
            (StorageEncoding::Identity, StorageEncoding::Zstd) => compress_zstd(&blob.data)?,
            (StorageEncoding::Zstd, StorageEncoding::Identity) => {
                decompress_zstd(&blob.data, blob.metadata.uncompressed_size)?
            }
            _ => return Err(CompressionError::Unsupported),
        };

        blob.metadata.encoding = target;
        blob.metadata.stored_size = blob.data.len() as u64;
        Ok(blob)
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

    #[test]
    fn identity_data_round_trips_through_zstd() -> Result<(), CompressionError> {
        let expected = b"action result payload".to_vec();

        let blob = BlobCodec::from_identity_data(expected.clone(), StorageEncoding::Zstd)?;
        assert_eq!(blob.metadata.encoding, StorageEncoding::Zstd);
        assert_eq!(blob.metadata.uncompressed_size, expected.len() as u64);
        assert_eq!(blob.metadata.stored_size, blob.data.len() as u64);

        let actual = BlobCodec::into_identity_data(blob)?;
        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn identity_helpers_do_not_reencode_identity_data() -> Result<(), CompressionError> {
        let expected = b"uncompressed payload".to_vec();

        let blob = BlobCodec::from_identity_data(expected.clone(), StorageEncoding::Identity)?;
        assert_eq!(blob, StoredBlob::identity(expected.clone()));

        let actual = BlobCodec::into_identity_data(blob)?;
        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn transcode_is_a_no_op_when_encoding_matches() -> Result<(), CompressionError> {
        let expected =
            BlobCodec::from_identity_data(b"already compressed".to_vec(), StorageEncoding::Zstd)?;

        let actual = BlobCodec::transcode(expected.clone(), StorageEncoding::Zstd)?;
        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn decompression_rejects_incorrect_uncompressed_size() -> Result<(), CompressionError> {
        let compressed =
            BlobCodec::from_identity_data(b"size checked".to_vec(), StorageEncoding::Zstd)?;
        let blob = StoredBlob::encoded(compressed.into_data(), StorageEncoding::Zstd, 3);

        assert!(matches!(
            BlobCodec::into_identity_data(blob),
            Err(CompressionError::SizeMismatch(3, 4))
        ));
        Ok(())
    }

    #[test]
    fn decompression_rejects_truncated_zstd() -> Result<(), CompressionError> {
        let mut compressed =
            BlobCodec::from_identity_data(b"truncated payload".to_vec(), StorageEncoding::Zstd)?;
        compressed.data.truncate(compressed.data.len() / 2);
        compressed.metadata.stored_size = compressed.data.len() as u64;

        assert!(BlobCodec::into_identity_data(compressed).is_err());
        Ok(())
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
