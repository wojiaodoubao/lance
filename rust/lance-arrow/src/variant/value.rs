// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_schema::ArrowError;
use crate::variant::metadata::OffsetSizeBytes;
use crate::variant::object::VariantObject;

/// The basic type of [`Variant`] value. This is encoded in 2 bits, including: primitive, object
/// and list.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum VariantBasicType {
    Primitive = 0,
    Object = 1,
    List = 2,
}

impl TryFrom<u8> for VariantBasicType {
    type Error = ArrowError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Primitive),
            1 => Ok(Self::Object),
            2 => Ok(Self::List),
            n => Err(ArrowError::InvalidArgumentError(
                format!("Unsupported variant basic type: {n}"),
            )),
        }
    }
}

#[derive(Debug)]
pub enum VariantPrimitiveType {
    Null = 0,
    BooleanTrue = 1,
    BooleanFalse = 2,
    Int8 = 3,
    Int16 = 4,
    Int32 = 5,
    Int64 = 6,
    Double = 7,
    Decimal4 = 8,
    Decimal8 = 9,
    Decimal16 = 10,
    Float = 11,
    Binary = 12,
    String = 13,
    Uuid = 14,
    Date = 15,
    TimeNTZ = 16,
    TimestampMicros = 17,
    TimestampNtzMicros = 18,
    TimestampNanos = 19,
    TimestampNtzNanos = 20,
}

impl TryFrom<u8> for VariantPrimitiveType {
    type Error = ArrowError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Null),
            1 => Ok(Self::BooleanTrue),
            2 => Ok(Self::BooleanFalse),
            3 => Ok(Self::Int8),
            4 => Ok(Self::Int16),
            5 => Ok(Self::Int32),
            6 => Ok(Self::Int64),
            7 => Ok(Self::Double),
            8 => Ok(Self::Decimal4),
            9 => Ok(Self::Decimal8),
            10 => Ok(Self::Decimal16),
            11 => Ok(Self::Float),
            12 => Ok(Self::Binary),
            13 => Ok(Self::String),
            14 => Ok(Self::Uuid),
            15 => Ok(Self::Date),
            16 => Ok(Self::TimeNTZ),
            17 => Ok(Self::TimestampMicros),
            18 => Ok(Self::TimestampNtzMicros),
            19 => Ok(Self::TimestampNanos),
            20 => Ok(Self::TimestampNtzNanos),
            n => Err(ArrowError::InvalidArgumentError(
                format!("Unsupported variant primitive type: {n}"),
            )),
        }
    }
}

/// Header structure for [`VariantObject`]
#[derive(Debug, Clone, PartialEq)]
pub struct VariantObjectHeader {
    pub(crate) field_offset_size: OffsetSizeBytes,
    pub(crate) field_id_size: OffsetSizeBytes,
    pub(crate) num_elements_size: OffsetSizeBytes, // either 1 or 4 bytes
}

impl TryFrom<u8> for VariantObjectHeader {
    type Error = ArrowError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let field_offset_size = OffsetSizeBytes::try_new(value & 0x03)?; // Last 2 bits
        let field_id_size = OffsetSizeBytes::try_new((value >> 2) & 0x03)?;; // Next 2 bits
        let is_large = (value & 0x10) != 0; // 5th bit
        let num_elements_size = match is_large {
            true => OffsetSizeBytes::Four,
            false => OffsetSizeBytes::One,
        };

        Ok(Self {
            field_offset_size,
            field_id_size,
            num_elements_size,
        })
    }
}

/// A parsed version of the variant list value header byte.
#[derive(Debug, Clone, PartialEq)]
pub struct VariantListHeader {
    pub(crate) field_offset_size: OffsetSizeBytes,
    pub(crate) num_elements_size: OffsetSizeBytes, // either 1 or 4 bytes
}

impl TryFrom<u8> for VariantListHeader {
    type Error = ArrowError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let field_offset_size = OffsetSizeBytes::try_new(value & 0x03)?; // last 2 bits

        let is_large = (value & 0x04) != 0; // 3rd bit from the right
        let num_elements_size = match is_large {
            true => OffsetSizeBytes::Four,
            false => OffsetSizeBytes::One,
        };

        Ok(Self {
            field_offset_size,
            num_elements_size,
        })
    }
}

#[derive(Debug)]
pub enum VariantValueHeader {
    Primitive(VariantPrimitiveType),
    Object(VariantObjectHeader),
    List(VariantListHeader),
}

pub struct VariantValueMeta<'m> {
    pub(crate) raw_byte: &'m u8,
    pub(crate) header: VariantValueHeader,
    pub(crate) basic_type: VariantBasicType,
}

impl <'m> TryFrom<&'m u8> for VariantValueMeta<'m> {
    type Error = ArrowError;

    fn try_from(value: &'m u8) -> Result<Self, Self::Error> {
        let basic_type = VariantBasicType::try_from(value & 0x03)?;
        let header_value = value >> 2;

        let header = match basic_type {
            VariantBasicType::Primitive => {
                let primitive_type = VariantPrimitiveType::try_from(header_value)?;
                VariantValueHeader::Primitive(primitive_type)
            },
            VariantBasicType::Object => VariantValueHeader::Object(VariantObjectHeader::try_from(header_value)?),
            VariantBasicType::List => VariantValueHeader::List(VariantListHeader::try_from(header_value)?),
        };
        Ok(Self {
            raw_byte: value,
            header,
            basic_type,
        })
    }
}