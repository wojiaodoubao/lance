// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance Variant support for Apache Arrow.

pub mod decimal;
pub mod object;
pub mod metadata;
pub mod list;

use std::sync::LazyLock;
use arrow_schema::{ArrowError, DataType, Field as ArrowField, Field, Fields as ArrowFields};
use arrow_schema::extension::ExtensionType;
use serde::{Deserialize, Serialize};
use crate::variant::decimal::{VariantDecimal16, VariantDecimal4, VariantDecimal8};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
pub use uuid::Uuid;
use crate::variant::list::VariantList;
use crate::variant::object::VariantObject;

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
    List(VariantList<'m, 'v>),

    /// Basic type: Object (basic_type_id=2).
    Object(VariantObject<'m, 'v>),
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