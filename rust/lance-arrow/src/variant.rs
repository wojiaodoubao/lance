// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance Variant support.

pub mod decimal;
pub mod object;
pub mod metadata;
pub mod list;
pub mod utils;
mod value;
mod variant_array;

use std::sync::LazyLock;
use arrow_array::{Array, ArrayRef, GenericBinaryArray, LargeStringArray, StringArray};
use arrow_array::cast::{as_list_array, AsArray};
use arrow_array::types::{Date32Type, Decimal128Type, Decimal32Type, Decimal64Type, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, Time64MicrosecondType, TimestampMicrosecondType, TimestampNanosecondType};
use arrow_schema::{ArrowError, DataType, Field as ArrowField, Field, Fields as ArrowFields, TimeUnit};
use arrow_schema::extension::ExtensionType;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use crate::variant::decimal::{VariantDecimal16, VariantDecimal4, VariantDecimal8};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
pub use uuid::Uuid;
use crate::variant::list::EncodedList;
use crate::variant::metadata::VariantMetadata;
use crate::variant::object::EncodedObject;
use crate::variant::list::TypedList;
use crate::variant::object::TypedObject;
use crate::variant::utils::{decode_binary, decode_date, decode_decimal16, decode_decimal4, decode_decimal8, decode_double, decode_float, decode_int16, decode_int32, decode_int64, decode_int8, decode_string, decode_time_ntz, decode_timestamp_micros, decode_timestamp_nanos, decode_timestampntz_micros, decode_timestampntz_nanos, decode_uuid, slice_from_slice, ListArrayExt};
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
pub enum Variant {
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
    Binary(VariantBinary),
    /// Primitive type(type_id=13): STRING
    String(VariantString),
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

    /// Basic type: List (basic_type_id=1).
    List(Box<dyn VariantList>),

    /// Basic type: Object (basic_type_id=2).
    Object(Box<dyn VariantObject>),
}

pub trait VariantObject {
    fn try_field_with_name(&self, name: &str) -> Result<Variant, ArrowError>;
}

pub trait VariantList {
    fn try_field_with_index(&self, i: usize) -> Result<Variant, ArrowError>;
}

impl Variant {
    pub fn new_encoded(metadata: Bytes, value: Bytes) -> Self {
        let metadata = VariantMetadata::try_new(metadata)
            .expect("Invalid variant metadata");
        Self::try_new_encoded(metadata, value).expect("Invalid variant data")
    }

    fn try_new_encoded(
        metadata: VariantMetadata,
        value: Bytes,
    ) -> Result<Self, ArrowError> {
        let first_byte = value.first().ok_or(ArrowError::InvalidArgumentError("Variant value must have at least one byte".to_string()))?;
        let value_meta = VariantValueMeta::try_from(first_byte)?;
        let value_data = slice_from_slice(&value, 1..value.len())?;

        match value_meta.header {
            VariantValueHeader::Primitive(primitive_type) => {
                let variant = match primitive_type {
                    VariantPrimitiveType::Null => Self::Null,
                    VariantPrimitiveType::Int8 => Self::Int8(decode_int8(&value_data)?),
                    VariantPrimitiveType::Int16 => Self::Int16(decode_int16(&value_data)?),
                    VariantPrimitiveType::Int32 => Self::Int32(decode_int32(&value_data)?),
                    VariantPrimitiveType::Int64 => Self::Int64(decode_int64(&value_data)?),
                    VariantPrimitiveType::Decimal4 => {
                        let (integer, scale) = decode_decimal4(&value_data)?;
                        Self::Decimal4(VariantDecimal4::try_new(integer, scale)?)
                    }
                    VariantPrimitiveType::Decimal8 => {
                        let (integer, scale) = decode_decimal8(&value_data)?;
                        Self::Decimal8(VariantDecimal8::try_new(integer, scale)?)
                    }
                    VariantPrimitiveType::Decimal16 => {
                        let (integer, scale) = decode_decimal16(&value_data)?;
                        Self::Decimal16(VariantDecimal16::try_new(integer, scale)?)
                    }
                    VariantPrimitiveType::Float => Self::Float(decode_float(&value_data)?),
                    VariantPrimitiveType::Double => {
                        Self::Double(decode_double(&value_data)?)
                    }
                    VariantPrimitiveType::BooleanTrue => Self::BooleanTrue,
                    VariantPrimitiveType::BooleanFalse => Self::BooleanFalse,
                    VariantPrimitiveType::Date => Self::Date(decode_date(&value_data)?),
                    VariantPrimitiveType::TimestampMicros => {
                        Self::TimestampMicros(decode_timestamp_micros(&value_data)?)
                    }
                    VariantPrimitiveType::TimestampNtzMicros => {
                        Self::TimestampNtzMicros(decode_timestampntz_micros(&value_data)?)
                    }
                    VariantPrimitiveType::TimestampNanos => {
                        Self::TimestampNanos(decode_timestamp_nanos(&value_data)?)
                    }
                    VariantPrimitiveType::TimestampNtzNanos => {
                        Self::TimestampNtzNanos(decode_timestampntz_nanos(&value_data)?)
                    }
                    VariantPrimitiveType::Uuid => Self::Uuid(decode_uuid(&value_data)?),
                    VariantPrimitiveType::Binary => {
                        Self::Binary(VariantBinary::from(decode_binary(&value_data)?))
                    }
                    VariantPrimitiveType::String => {
                        Self::String(VariantString::from(decode_string(&value_data)?))
                    }
                    VariantPrimitiveType::TimeNTZ => Self::Time(decode_time_ntz(&value_data)?),
                };
                Ok(variant)
            },
            VariantValueHeader::Object(header) => {
                let object = EncodedObject::try_new(metadata, Some(header), value_data)?;
                Ok(Self::Object(Box::new(object)))
            },
            VariantValueHeader::List(header) => {
                let list = EncodedList::try_new(metadata, Some(header), &value_data)?;
                Ok(Self::List(Box::new(list)))
            }
        }
    }

    /// Create a variant from a shredding array. The shredding array must have exactly one element
    /// and the element must be a struct with exactly one field named "root".
    pub fn new_typed(value: &ArrayRef) -> Result<Self, ArrowError> {
        if value.len() != 1 {
            return Err(ArrowError::InvalidArgumentError("Variant value must have exactly one element".to_string()));
        }

        let data_type = value.data_type();
        match data_type {
            DataType::Struct(fields) if fields.len() == 1 && fields[0].name() == "root" => {
                let struct_array = value.as_struct();
                let root_field = struct_array.column_by_name("root")
                    .ok_or_else(|| ArrowError::InvalidArgumentError("root field not found".to_string()))?;
                Self::try_new_typed(root_field)
            },
            _ => Err(ArrowError::InvalidArgumentError("The root variant type must be a struct with exactly one root field".to_string()))
        }
    }

    fn try_new_typed(value: &ArrayRef) -> Result<Self, ArrowError> {
        if (matches!(value.data_type(), DataType::List(_)) && as_list_array(value).num_items() != 1) || value.len() != 1 {
            return Err(ArrowError::InvalidArgumentError("Variant value must have exactly one element".to_string()));
        }

        let data_type = value.data_type();
        match data_type {
            // object
            DataType::Struct(_) => {
                Ok(Self::Object(Box::new(TypedObject::try_new(value)?)))
            },
            // list
            DataType::List(_) => {
                Ok(Self::List(Box::new(TypedList::try_new(value)?)))
            },
            // primitive
            DataType::Null => Ok(Self::Null),
            DataType::Boolean => Ok(Self::from(value.as_boolean().value(0))),
            // 16-byte FixedSizeBinary always correspond to a UUID; all other sizes are illegal.
            DataType::FixedSizeBinary(16) => {
                let array = value.as_fixed_size_binary();
                let value = array.value(0);
                Ok(Uuid::from_slice(value).unwrap().into()) // unwrap is safe: slice is always 16 bytes
            }
            DataType::Binary => {
                let value = value.as_binary_opt::<i32>().unwrap();
                Ok(Self::Binary(VariantBinary::from(value.clone())))
            },
            DataType::LargeBinary => {
                let value = value.as_binary_opt::<i64>().unwrap();
                Ok(Self::Binary(VariantBinary::from(value.clone())))
            }
            DataType::Utf8 => {
                let value = value.as_string::<i32>();
                Ok(Self::String(VariantString::from(value.clone())))
            },
            DataType::LargeUtf8 => {
                let value = value.as_string::<i64>();
                Ok(Self::String(VariantString::from(value.clone())))
            },
            DataType::Int8 => Ok(Self::from(value.as_primitive::<Int8Type>().value(0))),
            DataType::Int16 => Ok(Self::from(value.as_primitive::<Int16Type>().value(0))),
            DataType::Int32 => Ok(Self::from(value.as_primitive::<Int32Type>().value(0))),
            DataType::Int64 => Ok(Self::from(value.as_primitive::<Int64Type>().value(0))),
            DataType::Float16 => Ok(Self::from(value.as_primitive::<Float16Type>().value(0))),
            DataType::Float32 => Ok(Self::from(value.as_primitive::<Float32Type>().value(0))),
            DataType::Float64 => Ok(Self::from(value.as_primitive::<Float64Type>().value(0))),
            DataType::Decimal32(_, s) => {
                let value = value.as_primitive::<Decimal32Type>().value(0);
                VariantDecimal4::try_new(value, *s as u8).map(Self::from)
            }
            DataType::Decimal64(_, s) => {
                let value = value.as_primitive::<Decimal64Type>().value(0);
                VariantDecimal8::try_new(value, *s as u8).map(Self::from)
            }
            DataType::Decimal128(_, s) => {
                let value = value.as_primitive::<Decimal128Type>().value(0);
                VariantDecimal16::try_new(value, *s as u8).map(Self::from)
            }
            DataType::Date32 => {
                let value = value.as_primitive::<Date32Type>().value(0);
                Ok(Self::from(Date32Type::to_naive_date(value)))
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                let value = value.as_primitive::<Time64MicrosecondType>().value(0);
                NaiveTime::from_num_seconds_from_midnight_opt(
                    (value / 1_000_000) as u32,
                    (value % 1_000_000) as u32 * 1000,
                )
                    .ok_or_else(|| ArrowError::InvalidArgumentError(format!("Invalid microsecond: {}", value)))
                    .map(Self::from)
            }
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
                let value = value.as_primitive::<TimestampMicrosecondType>().value(0);
                DateTime::from_timestamp_micros(value)
                    .ok_or_else(|| ArrowError::InvalidArgumentError(format!("Invalid microsecond: {}", value)))
                    .map(Self::from)
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                let value = value.as_primitive::<TimestampMicrosecondType>().value(0);
                DateTime::from_timestamp_micros(value)
                    .ok_or_else(|| ArrowError::InvalidArgumentError(format!("Invalid microsecond: {}", value)))
                    .map(|v|Self::from(v.naive_utc()))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => {
                let value = value.as_primitive::<TimestampNanosecondType>().value(0);
                Ok(Self::from(DateTime::from_timestamp_nanos(value)))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                let value = value.as_primitive::<TimestampNanosecondType>().value(0);
                Ok(Self::from(DateTime::from_timestamp_nanos(value).naive_utc()))
            }
            _ => Err(ArrowError::InvalidArgumentError(format!("Invalid datatype: {:?}", data_type))),
        }
    }
}

pub enum VariantBinary {
    Bytes(Bytes),
    Binary(GenericBinaryArray<i32>),
    LargeBinary(GenericBinaryArray<i64>),
}

impl From<Bytes> for VariantBinary {
    fn from(bytes: Bytes) -> Self {
        Self::Bytes(bytes)
    }
}

impl From<GenericBinaryArray<i32>> for VariantBinary {
    fn from(arr: GenericBinaryArray<i32>) -> Self {
        Self::Binary(arr)
    }
}

impl From<GenericBinaryArray<i64>> for VariantBinary {
    fn from(arr: GenericBinaryArray<i64>) -> Self {
        Self::LargeBinary(arr)
    }
}

impl AsRef<[u8]> for VariantBinary {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Bytes(bytes) => bytes,
            Self::Binary(arr) => arr.value(0),
            Self::LargeBinary(arr) => arr.value(0),
        }
    }
}

pub enum VariantString {
    Bytes(Bytes),
    String(StringArray),
    LargeString(LargeStringArray),
}

impl From<Bytes> for VariantString {
    fn from(value: Bytes) -> Self {
        Self::Bytes(value)
    }
}

impl From<StringArray> for VariantString {
    fn from(arr: StringArray) -> Self {
        Self::String(arr)
    }
}

impl From<LargeStringArray> for VariantString {
    fn from(arr: LargeStringArray) -> Self {
        Self::LargeString(arr)
    }
}

impl AsRef<str> for VariantString {
    fn as_ref(&self) -> &str {
        match self {
            Self::Bytes(bytes) => str::from_utf8(bytes).unwrap(),
            Self::String(arr) => arr.value(0),
            Self::LargeString(arr) => arr.value(0),
        }
    }
}

impl From<()> for Variant {
    fn from((): ()) -> Self {
        Self::Null
    }
}

impl From<bool> for Variant {
    fn from(value: bool) -> Self {
        match value {
            true => Self::BooleanTrue,
            false => Self::BooleanFalse,
        }
    }
}

impl From<i8> for Variant {
    fn from(value: i8) -> Self {
        Self::Int8(value)
    }
}

impl From<i16> for Variant {
    fn from(value: i16) -> Self {
        Self::Int16(value)
    }
}

impl From<i32> for Variant {
    fn from(value: i32) -> Self {
        Self::Int32(value)
    }
}

impl From<i64> for Variant {
    fn from(value: i64) -> Self {
        Self::Int64(value)
    }
}

impl From<u8> for Variant {
    fn from(value: u8) -> Self {
        // if it fits in i8, use that, otherwise use i16
        if let Ok(value) = i8::try_from(value) {
            Self::Int8(value)
        } else {
            Self::Int16(i16::from(value))
        }
    }
}

impl From<u16> for Variant {
    fn from(value: u16) -> Self {
        // if it fits in i16, use that, otherwise use i32
        if let Ok(value) = i16::try_from(value) {
            Self::Int16(value)
        } else {
            Self::Int32(i32::from(value))
        }
    }
}
impl From<u32> for Variant {
    fn from(value: u32) -> Self {
        // if it fits in i32, use that, otherwise use i64
        if let Ok(value) = i32::try_from(value) {
            Self::Int32(value)
        } else {
            Self::Int64(i64::from(value))
        }
    }
}

impl TryFrom<u64> for Variant {
    type Error = ArrowError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        // if it fits in i64, use that, otherwise use Decimal16
        if let Ok(value) = i64::try_from(value) {
            Ok(Self::Int64(value))
        } else {
            // u64 max is 18446744073709551615, which fits in i128
            let value = VariantDecimal16::try_new(i128::from(value), 0)?;
            Ok(Self::Decimal16(value))
        }
    }
}

impl From<VariantDecimal4> for Variant {
    fn from(value: VariantDecimal4) -> Self {
        Self::Decimal4(value)
    }
}

impl From<VariantDecimal8> for Variant {
    fn from(value: VariantDecimal8) -> Self {
        Self::Decimal8(value)
    }
}

impl From<VariantDecimal16> for Variant {
    fn from(value: VariantDecimal16) -> Self {
        Self::Decimal16(value)
    }
}

impl From<half::f16> for Variant {
    fn from(value: half::f16) -> Self {
        Self::Float(value.into())
    }
}

impl From<f32> for Variant {
    fn from(value: f32) -> Self {
        Self::Float(value)
    }
}

impl From<f64> for Variant {
    fn from(value: f64) -> Self {
        Self::Double(value)
    }
}

impl From<NaiveDate> for Variant {
    fn from(value: NaiveDate) -> Self {
        Self::Date(value)
    }
}

impl From<DateTime<Utc>> for Variant {
    fn from(value: DateTime<Utc>) -> Self {
        if !value.nanosecond().is_multiple_of(1000) {
            Self::TimestampNanos(value)
        } else {
            Self::TimestampMicros(value)
        }
    }
}

impl From<NaiveDateTime> for Variant {
    fn from(value: NaiveDateTime) -> Self {
        if !value.nanosecond().is_multiple_of(1000) {
            Self::TimestampNtzNanos(value)
        } else {
            Self::TimestampNtzMicros(value)
        }
    }
}

impl From<NaiveTime> for Variant {
    fn from(value: NaiveTime) -> Self {
        Self::Time(value)
    }
}

impl From<Uuid> for Variant {
    fn from(value: Uuid) -> Self {
        Self::Uuid(value)
    }
}

impl TryFrom<(i32, u8)> for Variant {
    type Error = ArrowError;

    fn try_from(value: (i32, u8)) -> Result<Self, Self::Error> {
        Ok(Self::Decimal4(VariantDecimal4::try_new(
            value.0, value.1,
        )?))
    }
}

impl TryFrom<(i64, u8)> for Variant {
    type Error = ArrowError;

    fn try_from(value: (i64, u8)) -> Result<Self, Self::Error> {
        Ok(Self::Decimal8(VariantDecimal8::try_new(
            value.0, value.1,
        )?))
    }
}

impl TryFrom<(i128, u8)> for Variant {
    type Error = ArrowError;

    fn try_from(value: (i128, u8)) -> Result<Self, Self::Error> {
        Ok(Self::Decimal16(VariantDecimal16::try_new(
            value.0, value.1,
        )?))
    }
}