// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::variant::metadata::{OffsetSizeBytes, VariantMetadata};

/// Header structure for [`VariantObject`]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VariantObjectHeader {
    num_elements_size: OffsetSizeBytes,
    field_id_size: OffsetSizeBytes,
    field_offset_size: OffsetSizeBytes,
}

#[derive(Debug, Clone)]
pub struct VariantObject<'m, 'v> {
    pub metadata: VariantMetadata<'m>,
    pub value: &'v [u8],
    header: VariantObjectHeader,
    num_elements: u32,
    first_field_offset_byte: u32,
    first_value_byte: u32,
    validated: bool,
}