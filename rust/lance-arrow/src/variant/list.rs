// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_schema::ArrowError;
use bytes::Bytes;
use crate::variant::metadata::VariantMetadata;
use crate::variant::utils::{overflow_error, slice_from_slice};
use crate::variant::value::{VariantListHeader, VariantValueHeader, VariantValueMeta};
use crate::variant::{AbstractVariantList, Variant};

#[derive(Debug, Clone)]
pub struct VariantList {
    pub metadata: VariantMetadata,
    pub value: Bytes,
    header: VariantListHeader,
    num_elements: u32,
    first_value_byte: u32,
}

impl AbstractVariantList for VariantList {
    /// Get a field's value by index.
    fn try_field_with_index(&self, i: usize) -> Result<Variant, ArrowError> {
        if i < self.num_elements as usize {
            let byte_range = self.get_offset(i)? as _..self.get_offset(i + 1)? as _;

            let start_byte = self.first_value_byte
                .checked_add(byte_range.start)
                .ok_or_else(|| overflow_error("slice start"))? as usize;
            // TODO: 不需要end_byte，直接截到尾巴就可以。
            // let end_byte = self.first_value_byte
            //     .checked_add(byte_range.end)
            //     .ok_or_else(|| overflow_error("slice end"))? as usize;

            let value_bytes =
                slice_from_slice(&self.value, start_byte..self.value.len())?;
            Variant::try_new(self.metadata.clone(), value_bytes)
        } else {
            Err(ArrowError::InvalidArgumentError(format!("Variant list index {} out of bounds", i)))
        }
    }
}

impl VariantList {
    pub fn try_new(metadata: VariantMetadata, header: Option<VariantListHeader>, value: &Bytes) -> Result<Self, ArrowError> {
        let header = match header {
            Some(header) => header,
            None => {
                let first_byte = value.first().ok_or(ArrowError::InvalidArgumentError("Variant value must have at least one byte".to_string()))?;
                let value_meta = VariantValueMeta::try_from(first_byte)?;
                match value_meta.header {
                    VariantValueHeader::List(header) => header,
                    _ => return Err(ArrowError::InvalidArgumentError(format!("Variant value must be an object, but got {:?}", value_meta.header))),
                }
            }
        };

        // Determine num_elements size
        let num_elements =
            header
                .num_elements_size
                .unpack_u32_at_offset(value, 1, 0)?;

        // first_value_byte = 1 + num_elements_size + (num_elements + 1) * field_offset_size
        let first_value_byte = num_elements
            .checked_add(1)
            .and_then(|n| n.checked_mul(header.field_offset_size as u32))
            .and_then(|n| n.checked_add(1 + header.num_elements_size as u32))
            .ok_or_else(|| overflow_error("offset of variant list values"))?;

        Ok(Self {
            metadata,
            value: value.clone(),
            header,
            num_elements,
            first_value_byte,
        })
    }

    fn get_offset(&self, index: usize) -> Result<u32, ArrowError> {
        let first_offset_byte = 1 + self.header.num_elements_size as usize;
        let byte_range = first_offset_byte..self.first_value_byte as _;
        let offset_bytes = slice_from_slice(&self.value, byte_range)?;
        self.header.field_offset_size.unpack_u32_at_offset(&offset_bytes, 0, index)
    }
}