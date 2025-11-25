// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_array::{Array, ArrayRef, ListArray};
use arrow_array::cast::as_list_array;
use arrow_schema::{ArrowError, DataType};
use num_traits::ToPrimitive;
use crate::variant::utils::ListArrayExt;
use crate::variant::{AbstractVariantList, Variant};

pub struct ShreddingList {
    pub value: ListArray,
}

impl ShreddingList {
    pub fn try_new(value: &ArrayRef) -> Result<Self, ArrowError> {
        if !matches!(value.data_type(), &DataType::List(_)) {
            return Err(ArrowError::InvalidArgumentError(
                "ShreddingList must be a list array".to_string(),
            ));
        }

        let value = as_list_array(value);
        if value.num_items() != 1 {
            return Err(ArrowError::InvalidArgumentError(
                "ShreddingList must be a list array with only one element".to_string(),
            ));
        }

        Ok(Self { value: value.clone() })
    }
}

impl AbstractVariantList for ShreddingList {
    fn try_field_with_index(&self, i: usize) -> Result<Variant, ArrowError> {
        let start = self.value
            .offsets()
            .get(i)
            .map(|v| v.to_usize().unwrap())
            .ok_or(ArrowError::InvalidArgumentError(format!(
                "ShreddingList index {} is out of bounds",
                i
            )))?;
        let end = self.value
            .offsets()
            .get(i + 1)
            .map(|v| v.to_usize().unwrap())
            .ok_or(ArrowError::InvalidArgumentError(format!(
                "ShreddingList index {} is out of bounds",
                i + 1
            )))?;
        let element_arr = self.value.values().slice(start, end - start);

        Variant::try_new_shredding(&element_arr)
    }
}
