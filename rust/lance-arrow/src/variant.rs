// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance Variant support for Apache Arrow.

pub mod decimal;
pub mod object;
pub mod metadata;
pub mod list;
pub mod utils;
mod value;

use std::sync::LazyLock;
use arrow_schema::{ArrowError, DataType, Field as ArrowField, Field, Fields as ArrowFields};
use arrow_schema::extension::ExtensionType;
use serde::{Deserialize, Serialize};
use crate::variant::decimal::{VariantDecimal16, VariantDecimal4, VariantDecimal8};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
pub use uuid::Uuid;
use crate::variant::list::VariantArray;
use crate::variant::metadata::VariantMetadata;
use crate::variant::object::VariantObject;
use crate::variant::utils::{decode_binary, decode_date, decode_decimal16, decode_decimal4, decode_decimal8, decode_double, decode_float, decode_int16, decode_int32, decode_int64, decode_int8, decode_long_string, decode_time_ntz, decode_timestamp_micros, decode_timestamp_nanos, decode_timestampntz_micros, decode_timestampntz_nanos, decode_uuid, slice_from_slice};
use crate::variant::value::{VariantPrimitiveType, VariantValueHeader, VariantValueMeta};

/// Arrow extension type name for Variant data
pub const VARIANT_EXT_NAME: &str = "lance.variant";

pub static VARIANT_DATA_TYPE: LazyLock<DataType> =
    LazyLock::new(|| DataType::Struct(ArrowFields::from(vec![
        ArrowField::new(
            "meta",
            DataType::Binary,
            true),
        ArrowField::new(
            "value",
            DataType::Binary,
            true),
        ArrowField::new(
            "typed",
            DataType::Struct(ArrowFields::empty()),
            true),
    ])));

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum VariantVersion {
    V1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VariantMeta {
    version: VariantVersion,
}

/// TODO: add doc
struct VariantType(VariantMeta);

impl ExtensionType for VariantType {
    const NAME: &'static str = VARIANT_EXT_NAME;
    type Metadata = VariantMeta;

    fn metadata(&self) -> &Self::Metadata {
        &self.0
    }

    fn serialize_metadata(&self) -> Option<String> {
        Some(serde_json::to_string(&self.0).unwrap())
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        const ERR: &str = "Lance variant extension type metadata is invalid";
        metadata
            .map_or_else(
                || Err(ArrowError::InvalidArgumentError(ERR.to_owned())),
                |metadata| {
                    serde_json::from_str::<VariantMeta>(metadata)
                        .map_err(|_| ArrowError::InvalidArgumentError(ERR.to_owned()))
                },
            )
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        if data_type == &*VARIANT_DATA_TYPE {
            Ok(())
        } else {
            Err(ArrowError::InvalidArgumentError(format!(
                "Json data type mismatch, expected one of Utf8, LargeUtf8, Utf8View, found {data_type}"
            )))
        }
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let variant = Self(metadata);
        variant.supports_data_type(data_type)?;
        Ok(variant)
    }
}

/// TODO: add doc
pub enum Variant<'m, 'v> {
    /// Basic type: Primitive (basic_type_id=0).
    /// Primitive type(type_id=0): NULL
    Null,
    /// Primitive type(type_id=1): BOOLEAN (true)
    BooleanTrue,
    /// Primitive type(type_id=2): BOOLEAN (false)
    BooleanFalse,
    /// Primitive type(type_id=3): INT(8, SIGNED)
    Int8(i8),
    /// Primitive type(type_id=4): INT(16, SIGNED)
    Int16(i16),
    /// Primitive type(type_id=5): INT(32, SIGNED)
    Int32(i32),
    /// Primitive type(type_id=6): INT(64, SIGNED)
    Int64(i64),
    /// Primitive type(type_id=7): DOUBLE
    Double(f64),
    /// Primitive type(type_id=8): DECIMAL(precision, scale) 32-bits
    Decimal4(VariantDecimal4),
    /// Primitive type(type_id=9): DECIMAL(precision, scale) 64-bits
    Decimal8(VariantDecimal8),
    /// Primitive type(type_id=10): DECIMAL(precision, scale) 128-bits
    Decimal16(VariantDecimal16),
    /// Primitive type(type_id=11): FLOAT
    Float(f32),
    /// Primitive type(type_id=12): BINARY
    Binary(&'v [u8]),
    /// Primitive type(type_id=13): STRING
    String(&'v str),
    /// Primitive type(type_id=14): UUID
    Uuid(Uuid),
    /// Primitive type(type_id=15): DATE
    Date(NaiveDate),
    /// Primitive type(type_id=16): TIME(isAdjustedToUTC=false, MICROS)
    Time(NaiveTime),
    /// Primitive type(type_id=17): TIMESTAMP(isAdjustedToUTC=true, MICROS)
    TimestampMicros(DateTime<Utc>),
    /// Primitive type(type_id=18): TIMESTAMP(isAdjustedToUTC=false, MICROS)
    TimestampNtzMicros(NaiveDateTime),
    /// Primitive type(type_id=19): TIMESTAMP(isAdjustedToUTC=true, NANOS)
    TimestampNanos(DateTime<Utc>),
    /// Primitive type(type_id=20): TIMESTAMP(isAdjustedToUTC=false, NANOS)
    TimestampNtzNanos(NaiveDateTime),

    /// Basic type: Array (basic_type_id=1).
    List(VariantArray<'m, 'v>),

    /// Basic type: Object (basic_type_id=2).
    Object(VariantObject<'m, 'v>),
}

impl<'m, 'v> Variant<'m, 'v> {
    pub fn new(metadata: &'m [u8], value: &'v [u8]) -> Self {
        let metadata = VariantMetadata::try_new(metadata)
            .expect("Invalid variant metadata");
        Self::try_new(metadata, value).expect("Invalid variant data")
    }

    fn try_new(
        metadata: VariantMetadata<'m>,
        value: &'v [u8],
    ) -> Result<Self, ArrowError> {
        let first_byte = value.first().ok_or(ArrowError::InvalidArgumentError("Variant value must have at least one byte".to_string()))?;
        let value_meta = VariantValueMeta::try_from(first_byte)?;
        let value_data = slice_from_slice(value, 1..)?;

        match value_meta.header {
            VariantValueHeader::Primitive(primitive_type) => {
                let variant = match primitive_type {
                    VariantPrimitiveType::Null => Variant::Null,
                    VariantPrimitiveType::Int8 => Variant::Int8(decode_int8(value_data)?),
                    VariantPrimitiveType::Int16 => Variant::Int16(decode_int16(value_data)?),
                    VariantPrimitiveType::Int32 => Variant::Int32(decode_int32(value_data)?),
                    VariantPrimitiveType::Int64 => Variant::Int64(decode_int64(value_data)?),
                    VariantPrimitiveType::Decimal4 => {
                        let (integer, scale) = decode_decimal4(value_data)?;
                        Variant::Decimal4(VariantDecimal4::try_new(integer, scale)?)
                    }
                    VariantPrimitiveType::Decimal8 => {
                        let (integer, scale) = decode_decimal8(value_data)?;
                        Variant::Decimal8(VariantDecimal8::try_new(integer, scale)?)
                    }
                    VariantPrimitiveType::Decimal16 => {
                        let (integer, scale) = decode_decimal16(value_data)?;
                        Variant::Decimal16(VariantDecimal16::try_new(integer, scale)?)
                    }
                    VariantPrimitiveType::Float => Variant::Float(decode_float(value_data)?),
                    VariantPrimitiveType::Double => {
                        Variant::Double(decode_double(value_data)?)
                    }
                    VariantPrimitiveType::BooleanTrue => Variant::BooleanTrue,
                    VariantPrimitiveType::BooleanFalse => Variant::BooleanFalse,
                    VariantPrimitiveType::Date => Variant::Date(decode_date(value_data)?),
                    VariantPrimitiveType::TimestampMicros => {
                        Variant::TimestampMicros(decode_timestamp_micros(value_data)?)
                    }
                    VariantPrimitiveType::TimestampNtzMicros => {
                        Variant::TimestampNtzMicros(decode_timestampntz_micros(value_data)?)
                    }
                    VariantPrimitiveType::TimestampNanos => {
                        Variant::TimestampNanos(decode_timestamp_nanos(value_data)?)
                    }
                    VariantPrimitiveType::TimestampNtzNanos => {
                        Variant::TimestampNtzNanos(decode_timestampntz_nanos(value_data)?)
                    }
                    VariantPrimitiveType::Uuid => Variant::Uuid(decode_uuid(value_data)?),
                    VariantPrimitiveType::Binary => {
                        Variant::Binary(decode_binary(value_data)?)
                    }
                    VariantPrimitiveType::String => {
                        Variant::String(decode_long_string(value_data)?)
                    }
                    VariantPrimitiveType::TimeNTZ => Variant::Time(decode_time_ntz(value_data)?),
                };
                Ok(variant)
            },
            VariantValueHeader::Object(header) => {
                let object = VariantObject::try_new(metadata, Some(header), value_data)?;
                Ok(Variant::Object(object))
            },
            VariantValueHeader::Array(header) => {
                let array = VariantArray::try_new(metadata, Some(header), value_data)?;
                Ok(Variant::List(array))
            }
        }
    }
}

/// create an arrow variant type field with specified name
/// TODO: do we need this method?
pub fn arrow_variant_field(name: &str) -> Field {
    ArrowField::new(
        name,
        VARIANT_DATA_TYPE.clone(),
        false,
    ).with_extension_type(VariantType(VariantMeta {
        version: VariantVersion::V1,
    }))
}

impl<'m, 'v> From<VariantDecimal4> for Variant<'m, 'v> {
    fn from(decimal: VariantDecimal4) -> Self {
        Variant::Decimal4(decimal)
    }
}

impl<'m, 'v> From<VariantDecimal8> for Variant<'m, 'v> {
    fn from(decimal: VariantDecimal8) -> Self {
        Variant::Decimal8(decimal)
    }
}

impl<'m, 'v> From<VariantDecimal16> for Variant<'m, 'v> {
    fn from(decimal: VariantDecimal16) -> Self {
        Variant::Decimal16(decimal)
    }
}