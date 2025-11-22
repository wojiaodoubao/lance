// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

/// Used to unpack offset array entries such as metadata dictionary offsets, object/array value
/// offsets and object field ids. These are derived from a two-bit `XXX_size_minus_one` field.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum OffsetSizeBytes {
    One = 1,
    Two = 2,
    Three = 3,
    Four = 4,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct VariantMetadataHeader {
    version: u8,
    is_sorted: bool,
    /// This is `offset_size_minus_one` + 1
    offset_size: OffsetSizeBytes,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VariantMetadata<'m> {
    /// (Only) the bytes that make up this metadata instance.
    pub(crate) bytes: &'m [u8],
    header: VariantMetadataHeader,
    dictionary_size: u32,
    first_value_byte: u32,
    validated: bool,
}