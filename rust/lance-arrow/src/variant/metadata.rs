// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_schema::{ArrowError, DataType, Field, Fields, IntervalUnit, Schema};
use crate::variant::metadata::OffsetSizeBytes::{Four, One, Three, Two};
use crate::variant::utils::{array_from_slice, overflow_error, slice_from_slice};

/// Used to unpack offset array entries such as metadata dictionary offsets, object/array value
/// offsets and object field ids. These are derived from a two-bit `XXX_size_minus_one` field.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum OffsetSizeBytes {
    One = 1,
    Two = 2,
    Three = 3,
    Four = 4,
}

impl OffsetSizeBytes {
    pub(crate) fn try_new(offset_size_minus_one: u8) -> Result<Self, ArrowError> {
        use OffsetSizeBytes::*;
        let result = match offset_size_minus_one {
            0 => One,
            1 => Two,
            2 => Three,
            3 => Four,
            _ => {
                return Err(ArrowError::InvalidArgumentError(
                    "offset_size_minus_one must be 0–3".to_string(),
                ));
            }
        };
        Ok(result)
    }

    /// Return one unsigned little-endian value from `bytes`.
    ///
    /// * `bytes` – the byte buffer to index
    /// * `byte_offset` – number of bytes to skip **before** reading the first
    ///   value (e.g. `1` to move past a header byte).
    /// * `offset_index` – 0-based index **after** the skipped bytes
    ///   (`0` is the first value, `1` the next, …).
    ///
    /// Each value is `self as u32` bytes wide (1, 2, 3 or 4), zero-extended to 32 bits as needed.
    pub(crate) fn unpack_u32_at_offset(
        &self,
        bytes: &[u8],
        byte_offset: usize,  // how many bytes to skip
        offset_index: usize, // which offset in an array of offsets
    ) -> Result<u32, ArrowError> {
        // Index into the byte array:
        // byte_offset + (*self as usize) * offset_index
        let offset = offset_index
            .checked_mul(*self as usize)
            .and_then(|n| n.checked_add(byte_offset))
            .ok_or_else(|| overflow_error("unpacking offset array value"))?;
        let value = match self {
            One => u8::from_le_bytes(array_from_slice(bytes, offset)?).into(),
            Two => u16::from_le_bytes(array_from_slice(bytes, offset)?).into(),
            Three => {
                // Let's grab the three byte le-chunk first
                let b3_chunks: [u8; 3] = array_from_slice(bytes, offset)?;
                // Let's pad it and construct a padded u32 from it.
                let mut buf = [0u8; 4];
                buf[..3].copy_from_slice(&b3_chunks);
                u32::from_le_bytes(buf)
            }
            Four => u32::from_le_bytes(array_from_slice(bytes, offset)?),
        };
        Ok(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum VariantVersion {
    V1 = 1,
}

impl TryFrom<u8> for VariantVersion {
    type Error = ArrowError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1u8 => Ok(Self::V1),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Variant version {value} is not supported",
            ))),
        }
    }
}

/// The variant metadata header layout.
///
///          7     6  5   4  3             0
///         +-------+---+---+---------------+
/// header  |       |   |   |    version    |
///         +-------+---+---+---------------+
///           ^           ^
///           |           +-- sorted_strings
///           +-- offset_size_minus_one
///
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct VariantMetadataHeader {
    version: VariantVersion,
    is_sorted: bool,
    /// This is `offset_size_minus_one` + 1
    offset_size: OffsetSizeBytes,
}

impl VariantMetadataHeader {
    pub(crate) fn try_new(header_byte: &u8) -> Result<Self, ArrowError> {
        let version = VariantVersion::try_from(header_byte & 0x0F)?;
        let is_sorted = (header_byte & 0x10) != 0;
        let offset_size = OffsetSizeBytes::try_new(header_byte >> 6)?;

        Ok(Self {
            version,
            is_sorted,
            offset_size,
        })
    }
}

/// The variant meta layout.
///
///             7                     0
///             +-----------------------+
///  metadata   |        header         |
///             +-----------------------+
///             |                       |
///             :    dictionary_size    :  <-- unsigned little-endian, `offset_size` bytes
///             |                       |
///             +-----------------------+
///             |                       |
///             :        offset         :  <-- unsigned little-endian, `offset_size` bytes
///             |                       |
///             +-----------------------+
///                         :
///             +-----------------------+
///             |                       |
///             :        offset         :  <-- unsigned little-endian, `offset_size` bytes
///             |                       |      (`dictionary_size + 1` offsets)
///             +-----------------------+
///             |                       |
///             :         bytes         :
///             |                       |
///             +-----------------------+
///
#[derive(Debug, Clone, PartialEq)]
pub struct VariantMetadata<'m> {
    /// (Only) the bytes that make up this metadata instance.
    pub(crate) bytes: &'m [u8],
    header: VariantMetadataHeader,
    dictionary_size: u32,
    first_value_byte: u32,
}

impl<'m> VariantMetadata<'m> {
    // TODO: add unit tests
    pub(crate) fn try_new(bytes: &'m [u8]) -> Result<Self, ArrowError> {
        // Parse header
        let header_byte = bytes.first().ok_or(ArrowError::InvalidArgumentError("Variant metadata must have at least one byte".to_string()))?;
        let header = VariantMetadataHeader::try_new(header_byte)?;

        // Dictionary size is the first element after header.
        let dictionary_size =
            header
                .offset_size
                .unpack_u32_at_offset(bytes, 1, 0)?;

        // first_value_byte = header size(1Byte) + offset_size + (dict_size + 1) * offset_size
        //
        // The max value of offset_size is u32, so the offsets in metadata should never exceed
        // u32::MAX.
        let first_value_byte = dictionary_size
            .checked_add(1)
            .and_then(|n| n.checked_mul(header.offset_size as u32))
            .and_then(|n| n.checked_add(1 + header.offset_size as u32))
            .ok_or_else(|| overflow_error("offset of variant metadata dictionary"))?;

        Ok(Self {
            bytes,
            header,
            dictionary_size,
            first_value_byte,
        })
    }
}