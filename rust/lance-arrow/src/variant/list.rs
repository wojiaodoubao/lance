// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_schema::ArrowError;
use crate::variant::metadata::VariantMetadata;
use crate::variant::utils::overflow_error;
use crate::variant::value::{VariantArrayHeader, VariantValueHeader, VariantValueMeta};

#[derive(Debug, Clone)]
pub struct VariantArray<'m, 'v> {
    pub metadata: VariantMetadata<'m>,
    pub value: &'v [u8],
    header: VariantArrayHeader,
    num_elements: u32,
    first_value_byte: u32,
}

impl<'m, 'v> VariantArray<'m, 'v> {
    pub fn try_new(metadata: VariantMetadata<'m>, header: Option<VariantArrayHeader>, value: &'v [u8]) -> Result<Self, ArrowError> {
        let header = match header {
            Some(header) => header,
            None => {
                let first_byte = value.first().ok_or(ArrowError::InvalidArgumentError("Variant value must have at least one byte".to_string()))?;
                let value_meta = VariantValueMeta::try_from(first_byte)?;
                match value_meta.header {
                    VariantValueHeader::Array(header) => header,
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
            value,
            header,
            num_elements,
            first_value_byte,
        })
    }
}