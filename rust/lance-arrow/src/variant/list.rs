// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::variant::metadata::VariantMetadata;
use crate::variant::utils::{first_byte_from_slice, overflow_error, slice_from_slice};
use crate::variant::value::{VariantListHeader, VariantValueHeader, VariantValueMeta};
use crate::variant::{Buffer, Variant};
use arrow_array::cast::as_list_array;
use arrow_array::{Array, ArrayRef, ListArray};
use arrow_schema::{ArrowError, DataType};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum VariantList {
    Encoded(EncodedList),
    Typed(TypedList),
}

impl VariantList {
    pub fn try_field_with_index(&self, i: usize) -> Result<Variant, ArrowError> {
        match self {
            Self::Encoded(encoded) => encoded.try_field_with_index(i),
            Self::Typed(typed) => typed.try_field_with_index(i),
        }
    }

    pub fn num_elements(&self) -> usize {
        match self {
            Self::Encoded(encoded) => encoded.num_elements as usize,
            Self::Typed(typed) => typed.num_elements(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EncodedList {
    pub metadata: Arc<VariantMetadata>,
    pub value: Buffer,
    header: VariantListHeader,
    num_elements: u32,
    first_value_byte: u32,
}

impl EncodedList {
    pub fn try_new(
        metadata: Arc<VariantMetadata>,
        header: Option<VariantListHeader>,
        value: &Buffer,
    ) -> Result<Self, ArrowError> {
        let header = match header {
            Some(header) => header,
            None => {
                let first_byte = first_byte_from_slice(value.as_ref())?;
                let value_meta = VariantValueMeta::try_from(first_byte)?;
                match value_meta.header {
                    VariantValueHeader::List(header) => header,
                    _ => {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Variant value must be an object, but got {:?}",
                            value_meta.header
                        )))
                    }
                }
            }
        };

        // Determine num_elements size
        let num_elements = header.num_elements_size.unpack_u32_at_offset(value, 1, 0)?;

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

    /// Get a field's value by index.
    pub fn try_field_with_index(&self, i: usize) -> Result<Variant, ArrowError> {
        if i < self.num_elements as usize {
            let byte_range = self.get_offset(i)? as _..self.get_offset(i + 1)? as _;

            let start_byte =
                self.first_value_byte
                    .checked_add(byte_range.start)
                    .ok_or_else(|| overflow_error("slice start"))? as usize;

            let value_bytes = slice_from_slice(&self.value, start_byte..self.value.len())?;
            Variant::try_new_encoded(self.metadata.clone(), value_bytes)
        } else {
            Err(ArrowError::InvalidArgumentError(format!(
                "Variant list index {} out of bounds",
                i
            )))
        }
    }

    fn get_offset(&self, index: usize) -> Result<u32, ArrowError> {
        let first_offset_byte = 1 + self.header.num_elements_size as usize;
        let byte_range = first_offset_byte..self.first_value_byte as _;
        let offset_bytes = slice_from_slice(&self.value, byte_range)?;
        self.header
            .field_offset_size
            .unpack_u32_at_offset(&offset_bytes, 0, index)
    }
}

#[derive(Debug, Clone)]
pub struct TypedList {
    pub value: ListArray,
}

impl TypedList {
    pub fn try_new(value: &ArrayRef) -> Result<Self, ArrowError> {
        if !matches!(value.data_type(), &DataType::List(_)) {
            return Err(ArrowError::InvalidArgumentError(
                "ShreddingList must be a list array".to_string(),
            ));
        }

        let value = as_list_array(value);
        if value.len() != 1 {
            return Err(ArrowError::InvalidArgumentError(
                "ShreddingList must be a list array with only one element".to_string(),
            ));
        }

        Ok(Self {
            value: value.clone(),
        })
    }

    pub fn try_field_with_index(&self, i: usize) -> Result<Variant, ArrowError> {
        Variant::try_new_typed(&self.value.value(0))
    }

    pub fn num_elements(&self) -> usize {
        self.value.len()
    }
}
