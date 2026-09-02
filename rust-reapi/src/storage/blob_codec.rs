use crate::storage::blob_store::{StorageEncoding, StoredBlob};
use thiserror::Error;
// TODO: add some tests

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
