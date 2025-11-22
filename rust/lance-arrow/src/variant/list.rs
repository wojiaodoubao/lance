// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::variant::metadata::{OffsetSizeBytes, VariantMetadata};

/// A parsed version of the variant array value header byte.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VariantListHeader {
    num_elements_size: OffsetSizeBytes,
    offset_size: OffsetSizeBytes,
}

#[derive(Debug, Clone)]
pub struct VariantList<'m, 'v> {
    pub metadata: VariantMetadata<'m>,
    pub value: &'v [u8],
    header: VariantListHeader,
    num_elements: u32,
    first_value_byte: u32,
    validated: bool,
}