// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_array::{ArrayRef, StructArray};
use arrow_array::cast::AsArray;
use arrow_schema::ArrowError;
use bytes::Bytes;
use crate::DataTypeExt;
use crate::variant::metadata::VariantMetadata;
use crate::variant::utils::{overflow_error, slice_from_slice, try_binary_search_range_by};
use crate::variant::value::{VariantObjectHeader, VariantValueHeader, VariantValueMeta};
use crate::variant::{VariantObject, Variant};

#[derive(Debug, Clone)]
pub struct EncodedObject {
    pub metadata: VariantMetadata,
    pub value: Bytes,
    header: VariantObjectHeader,
    num_elements: u32,
    first_field_offset_byte: u32,
    first_value_byte: u32,
}

impl VariantObject for EncodedObject {
    /// Get a field's value by name.
    fn try_field_with_name(&self, name: &str) -> Result<Variant, ArrowError> {
        let cmp = |i| {
            match self.try_field_name(i) {
                Ok(f) => Some(f.cmp(name)),
                Err(_) => None,
            }
        };

        match try_binary_search_range_by(0..self.num_elements as usize, cmp) {
            Some(Ok(index)) => Ok(self.try_field_with_index(index)?),
            _ => Err(ArrowError::InvalidArgumentError(format!("Variant object field name {} not found", name))),
        }
    }
}

impl EncodedObject {
    pub fn try_new(metadata: VariantMetadata, header: Option<VariantObjectHeader>, value: Bytes) -> Result<Self, ArrowError> {
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
                .unpack_u32_at_offset(&value, 1, 0)?;

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

    /// Get a field's value by index.
    fn try_field_with_index(&self, i: usize) -> Result<Variant, ArrowError> {
        if i < self.num_elements as usize {
            let value_bytes = slice_from_slice(&self.value, self.first_value_byte as _..self.value.len())?;
            let value_bytes = slice_from_slice(&value_bytes, self.get_offset(i)? as _..value_bytes.len())?;
            Variant::try_new_encoded(self.metadata.clone(), value_bytes)
        } else {
            Err(ArrowError::InvalidArgumentError(format!("Variant object index {} out of bounds", i)))
        }
    }

    // Returns field name by index
    fn try_field_name(&self, i: usize) -> Result<&str, ArrowError> {
        let field_id_bytes = self.field_id_bytes()?;
        let field_id = self.header.field_id_size.unpack_u32_at_offset(&field_id_bytes, 0, i)?;
        self.metadata.get_name(field_id as _)
    }

    // Attempts to retrieve the ith offset from the field offset region of the byte buffer.
    fn get_offset(&self, i: usize) -> Result<u32, ArrowError> {
        self.header.field_offset_size.unpack_u32_at_offset(&self.field_offset_bytes()?, 0, i)
    }

    // Returns field id bytes.
    fn field_id_bytes(&self) -> Result<Bytes, ArrowError> {
        let field_id_start = 1 + self.num_elements as usize;
        let byte_range = field_id_start..self.first_field_offset_byte as usize;
        slice_from_slice(&self.value, byte_range)
    }

    // Returns field offset bytes.
    fn field_offset_bytes(&self) -> Result<Bytes, ArrowError> {
        let byte_range = self.first_field_offset_byte as _..self.first_value_byte as _;
        slice_from_slice(&self.value, byte_range)
    }
}

pub struct TypedObject {
    pub value: StructArray,
}

impl TypedObject {
    pub fn try_new(value: &ArrayRef) -> Result<Self, ArrowError> {
        if !value.data_type().is_struct() || value.len() != 1 {
            return Err(ArrowError::InvalidArgumentError(
                "ShreddingObject must be a struct array with only one row".to_string(),
            ));
        }

        let value = value.as_struct();
        Ok(Self { value: value.clone() })
    }
}

impl VariantObject for TypedObject {
    fn try_field_with_name(&self, name: &str) -> Result<Variant, ArrowError> {
        let arr = self.value.column_by_name(name).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!("Variant object field name {} not found", name))
        })?;
        Variant::try_new_typed(arr)
    }
}