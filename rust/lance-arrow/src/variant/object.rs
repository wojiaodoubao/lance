// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_schema::ArrowError;
use crate::variant::metadata::VariantMetadata;
use crate::variant::utils::{overflow_error, slice_from_slice};
use crate::variant::value::{VariantObjectHeader, VariantValueHeader, VariantValueMeta};

#[derive(Debug, Clone)]
pub struct VariantObject<'m, 'v> {
    pub metadata: VariantMetadata<'m>,
    pub value: &'v [u8],
    header: VariantObjectHeader,
    num_elements: u32,
    first_field_offset_byte: u32,
    first_value_byte: u32,
}

impl<'m, 'v> VariantObject<'m, 'v> {
    pub fn try_new(metadata: VariantMetadata<'m>, header: Option<VariantObjectHeader>, value: &'v [u8]) -> Result<Self, ArrowError> {
        let header = match header {
            Some(h) => h,
            None => {
                let first_byte = value.first().ok_or(ArrowError::InvalidArgumentError("Variant value must have at least one byte".to_string()))?;
                let value_meta = VariantValueMeta::try_from(first_byte)?;
                match value_meta.header {
                    VariantValueHeader::Object(header) => header,
                    _ => return Err(ArrowError::InvalidArgumentError(format!("Variant value must be an object, but got {:?}", value_meta.header))),
                }
            }
        };

        // Determine num_elements size
        let num_elements =
            header
                .num_elements_size
                .unpack_u32_at_offset(value, 1, 0)?;

        // first_field_offset_byte = 1 + num_elements_size + field_id_size * num_elements
        let first_field_offset_byte = num_elements
            .checked_mul(header.field_id_size as u32)
            .and_then(|n| n.checked_add(1 + header.num_elements_size as u32))
            .ok_or_else(|| overflow_error("offset of variant object field offsets"))?;

        // first_value_byte = first_field_offset_byte + field_offset_size * (num_elements + 1)
        let first_value_byte = num_elements
            .checked_add(1)
            .and_then(|n| n.checked_mul(header.field_offset_size as u32))
            .and_then(|n| n.checked_add(first_field_offset_byte))
            .ok_or_else(|| overflow_error("offset of variant object field values"))?;

        Ok(Self {
            metadata,
            value,
            header,
            num_elements,
            first_field_offset_byte,
            first_value_byte,
        })
    }
}