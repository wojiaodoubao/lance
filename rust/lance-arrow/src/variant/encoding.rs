// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use chrono::Timelike;
use uuid::Uuid;
use crate::variant::decimal::{VariantDecimal16, VariantDecimal4, VariantDecimal8};
use crate::variant::value::{VariantBasicType, VariantPrimitiveType};

pub(crate) const UNIX_EPOCH_DATE: chrono::NaiveDate =
    chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

struct MetaEncoder(Vec<u8>);

struct ValueEncoder(Vec<u8>);

impl MetaEncoder {
    pub fn encode_null(&mut self) {
        self.encode_primitive_header(VariantPrimitiveType::Null);
    }

    pub fn encode_bool(&mut self, value: bool) {
        let primitive_type = if value {
            VariantPrimitiveType::BooleanTrue
        } else {
            VariantPrimitiveType::BooleanFalse
        };
        self.encode_primitive_header(primitive_type);
    }

    pub fn encode_int8(&mut self, value: i8) {
        self.encode_primitive_header(VariantPrimitiveType::Int8);
        self.0.push(value as u8);
    }

    pub fn encode_int16(&mut self, value: i16) {
        self.encode_primitive_header(VariantPrimitiveType::Int16);
        self.0.push(value as u8);
    }

    pub fn encode_int32(&mut self, value: i32) {
        self.encode_primitive_header(VariantPrimitiveType::Int32);
        self.0.push(value as u8);
    }

    pub fn encode_int64(&mut self, value: i64) {
        self.encode_primitive_header(VariantPrimitiveType::Int64);
        self.0.push(value as u8);
    }

    pub fn encode_double(&mut self, value: f64) {
        self.encode_primitive_header(VariantPrimitiveType::Double);
        self.0.push(value as u8);
    }

    pub fn encode_decimal4(&mut self, decimal4: VariantDecimal4) {
        self.encode_primitive_header(VariantPrimitiveType::Decimal4);
        self.encode_bytes(&[decimal4.scale()]);
        self.encode_bytes(&decimal4.integer().to_le_bytes());
    }

    pub fn encode_decimal8(&mut self, decimal8: VariantDecimal8) {
        self.encode_primitive_header(VariantPrimitiveType::Decimal8);
        self.encode_bytes(&[decimal8.scale()]);
        self.encode_bytes(&decimal8.integer().to_le_bytes());
    }

    pub fn encode_decimal16(&mut self, decimal16: VariantDecimal16) {
        self.encode_primitive_header(VariantPrimitiveType::Decimal16);
        self.encode_bytes(&[decimal16.scale()]);
        self.encode_bytes(&decimal16.integer().to_le_bytes());
    }

    pub fn encode_float(&mut self, value: f32) {
        self.encode_primitive_header(VariantPrimitiveType::Float);
        self.0.push(value as u8);
    }

    pub fn encode_binary(&mut self, value: &[u8]) {
        self.encode_primitive_header(VariantPrimitiveType::Binary);
        self.encode_bytes(&(value.len() as u32).to_le_bytes());
        self.encode_bytes(value);
    }

    pub fn encode_string(&mut self, value: &str) {
        self.encode_primitive_header(VariantPrimitiveType::String);
        self.encode_bytes(&(value.len() as u32).to_le_bytes());
        self.encode_bytes(value.as_bytes());
    }

    pub fn encode_uuid(&mut self, value: Uuid) {
        self.encode_primitive_header(VariantPrimitiveType::Uuid);
        self.encode_bytes(&value.into_bytes());
    }

    pub fn encode_date(&mut self, value: chrono::NaiveDate) {
        self.encode_primitive_header(VariantPrimitiveType::Date);
        let days_since_epoch = value.signed_duration_since(UNIX_EPOCH_DATE).num_days() as i32;
        self.encode_bytes(&days_since_epoch.to_le_bytes());
    }

    pub fn encode_time_micros(&mut self, value: chrono::NaiveTime) {
        self.encode_primitive_header(VariantPrimitiveType::TimeNTZ);
        let micros_from_midnight = value.num_seconds_from_midnight() as u64 * 1_000_000
            + value.nanosecond() as u64 / 1_000;
        self.encode_bytes(&micros_from_midnight.to_le_bytes());
    }

    pub fn encode_timestamp_micros(&mut self, value: chrono::DateTime<chrono::Utc>) {
        self.encode_primitive_header(VariantPrimitiveType::TimestampMicros);
        let micros = value.timestamp_micros();
        self.encode_bytes(&micros.to_le_bytes());
    }

    pub fn encode_timestamp_ntz_micros(&mut self, value: chrono::NaiveDateTime) {
        self.encode_primitive_header(VariantPrimitiveType::TimestampNtzMicros);
        let micros = value.and_utc().timestamp_micros();
        self.encode_bytes(&micros.to_le_bytes());
    }

    pub fn encode_timestamp_nanos(&mut self, value: chrono::DateTime<chrono::Utc>) {
        self.encode_primitive_header(VariantPrimitiveType::TimestampNanos);
        let nanos = value.timestamp_nanos_opt().unwrap();
        self.encode_bytes(&nanos.to_le_bytes());
    }

    pub fn encode_timestamp_ntz_nanos(&mut self, value: chrono::NaiveDateTime) {
        self.encode_primitive_header(VariantPrimitiveType::TimestampNtzNanos);
        let nanos = value.and_utc().timestamp_nanos_opt().unwrap();
        self.encode_bytes(&nanos.to_le_bytes());
    }

    fn encode_primitive_header(&mut self, primitive_type: VariantPrimitiveType) {
        self.0.push(primitive_header(primitive_type));
    }

    fn encode_bytes(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }
}

fn primitive_header(primitive_type: VariantPrimitiveType) -> u8 {
    (primitive_type as u8) << 2 | VariantBasicType::Primitive as u8
}

