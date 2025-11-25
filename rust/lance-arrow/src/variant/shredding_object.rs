// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_array::{Array, ArrayRef, StructArray};
use arrow_array::cast::AsArray;
use arrow_schema::ArrowError;
use crate::DataTypeExt;
use crate::variant::{AbstractVariantObject, Variant};

pub struct ShreddingObject {
    pub value: StructArray,
}

impl ShreddingObject {
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

impl AbstractVariantObject for ShreddingObject {
    fn try_field_with_name(&self, name: &str) -> Result<Variant, ArrowError> {
        let arr = self.value.column_by_name(name).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!("Variant object field name {} not found", name))
        })?;
        Variant::try_new_shredding(arr)
    }
}