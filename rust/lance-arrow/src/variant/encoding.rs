// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::variant::decimal::{VariantDecimal16, VariantDecimal4, VariantDecimal8};
use crate::variant::list::VariantList;
use crate::variant::metadata::VariantMetadata;
use crate::variant::object::VariantObject;
use crate::variant::utils::int_size;
use crate::variant::value::{VariantBasicType, VariantPrimitiveType};
use crate::variant::Variant;
use arrow_schema::ArrowError;
use chrono::Timelike;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub(crate) const UNIX_EPOCH_DATE: chrono::NaiveDate =
    chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

pub struct MetaEncoder;

impl MetaEncoder {
    /// Encode variant into Variant metadata.
    pub fn encode(v: &Variant) -> Result<Vec<u8>, ArrowError> {
        let mut field_names = HashSet::with_capacity(1000);
        Self::collect_field_names(&mut field_names, v)?;

        let mut field_names = field_names.into_iter().collect::<Vec<_>>();
        field_names.sort();

        // is_sorted is a 1-bit value indicating whether the field names are sorted and unique
        let is_sorted = true;

        // offset_size is a 2-bit value providing the number of bytes for dictionary size and offset field
        let total_dict_bytes = field_names.iter().map(|k| k.len()).sum();
        let offset_size = int_size(std::cmp::max(total_dict_bytes, field_names.len()));

        // Write header: version=1, field names are sorted, with calculated offset_size
        let mut metadata_buffer = Vec::new();
        metadata_buffer.push(0x01 | (is_sorted as u8) << 4 | ((offset_size - 1) << 6));

        // Write dictionary size
        metadata_buffer.extend_from_slice(&field_names.len().to_le_bytes()[..offset_size as usize]);

        // Write offsets
        let mut cur_offset = 0usize;
        for key in field_names.iter() {
            metadata_buffer.extend_from_slice(&cur_offset.to_le_bytes()[..offset_size as usize]);
            cur_offset += key.len();
        }
        // Write final offset
        metadata_buffer.extend_from_slice(&cur_offset.to_le_bytes()[..offset_size as usize]);

        // Write string data
        for key in field_names {
            metadata_buffer.extend_from_slice(key.as_bytes());
        }

        Ok(metadata_buffer)
    }

    fn collect_field_names(names: &mut HashSet<String>, v: &Variant) -> Result<(), ArrowError> {
        if let Variant::Object(object) = v {
            for name in object.names() {
                let v = object.try_field_with_name(&name)?;
                names.insert(name);
                Self::collect_field_names(names, &v)?;
            }
        }
        Ok(())
    }
}

pub struct ValueEncoder(Vec<u8>);

impl ValueEncoder {
    pub fn encode(m: &HashMap<&str, usize>, v: &Variant) -> Result<Vec<u8>, ArrowError> {
        let mut value_encoder = ValueEncoder(Vec::new());
        match v {
            Variant::Null => value_encoder.encode_null(),
            Variant::BooleanTrue => value_encoder.encode_bool(true),
            Variant::BooleanFalse => value_encoder.encode_bool(false),
            Variant::Int8(i) => value_encoder.encode_int8(*i),
            Variant::Int16(i) => value_encoder.encode_int16(*i),
            Variant::Int32(i) => value_encoder.encode_int32(*i),
            Variant::Int64(i) => value_encoder.encode_int64(*i),
            Variant::Double(i) => value_encoder.encode_double(*i),
            Variant::Decimal4(d) => value_encoder.encode_decimal4(*d),
            Variant::Decimal8(d) => value_encoder.encode_decimal8(*d),
            Variant::Decimal16(d) => value_encoder.encode_decimal16(*d),
            Variant::Float(d) => value_encoder.encode_float(*d),
            Variant::Binary(b) => value_encoder.encode_binary(b.as_ref()),
            Variant::String(s) => value_encoder.encode_string(s.as_ref()),
            Variant::Uuid(u) => value_encoder.encode_uuid(*u),
            Variant::Date(d) => value_encoder.encode_date(*d),
            Variant::Time(t) => value_encoder.encode_time_micros(*t),
            Variant::TimestampMicros(t) => value_encoder.encode_timestamp_micros(*t),
            Variant::TimestampNtzMicros(t) => value_encoder.encode_timestamp_ntz_micros(*t),
            Variant::TimestampNanos(t) => value_encoder.encode_timestamp_nanos(*t),
            Variant::TimestampNtzNanos(t) => value_encoder.encode_timestamp_ntz_nanos(*t),
            Variant::List(list) => value_encoder.encode_variant_list(m, list)?,
            Variant::Object(object) => value_encoder.encode_variant_object(m, object)?,
        }
        Ok(value_encoder.0)
    }

    fn encode_null(&mut self) {
        self.encode_primitive_header(VariantPrimitiveType::Null);
    }

    fn encode_bool(&mut self, value: bool) {
        let primitive_type = if value {
            VariantPrimitiveType::BooleanTrue
        } else {
            VariantPrimitiveType::BooleanFalse
        };
        self.encode_primitive_header(primitive_type);
    }

    fn encode_int8(&mut self, value: i8) {
        self.encode_primitive_header(VariantPrimitiveType::Int8);
        self.0.push(value as u8);
    }

    fn encode_int16(&mut self, value: i16) {
        self.encode_primitive_header(VariantPrimitiveType::Int16);
        self.0.push(value as u8);
    }

    fn encode_int32(&mut self, value: i32) {
        self.encode_primitive_header(VariantPrimitiveType::Int32);
        self.0.push(value as u8);
    }

    fn encode_int64(&mut self, value: i64) {
        self.encode_primitive_header(VariantPrimitiveType::Int64);
        self.0.push(value as u8);
    }

    fn encode_double(&mut self, value: f64) {
        self.encode_primitive_header(VariantPrimitiveType::Double);
        self.0.push(value as u8);
    }

    fn encode_decimal4(&mut self, decimal4: VariantDecimal4) {
        self.encode_primitive_header(VariantPrimitiveType::Decimal4);
        self.encode_bytes(&[decimal4.scale()]);
        self.encode_bytes(&decimal4.integer().to_le_bytes());
    }

    fn encode_decimal8(&mut self, decimal8: VariantDecimal8) {
        self.encode_primitive_header(VariantPrimitiveType::Decimal8);
        self.encode_bytes(&[decimal8.scale()]);
        self.encode_bytes(&decimal8.integer().to_le_bytes());
    }

    fn encode_decimal16(&mut self, decimal16: VariantDecimal16) {
        self.encode_primitive_header(VariantPrimitiveType::Decimal16);
        self.encode_bytes(&[decimal16.scale()]);
        self.encode_bytes(&decimal16.integer().to_le_bytes());
    }

    fn encode_float(&mut self, value: f32) {
        self.encode_primitive_header(VariantPrimitiveType::Float);
        self.0.push(value as u8);
    }

    fn encode_binary(&mut self, value: &[u8]) {
        self.encode_primitive_header(VariantPrimitiveType::Binary);
        self.encode_bytes(&(value.len() as u32).to_le_bytes());
        self.encode_bytes(value);
    }

    fn encode_string(&mut self, value: &str) {
        self.encode_primitive_header(VariantPrimitiveType::String);
        self.encode_bytes(&(value.len() as u32).to_le_bytes());
        self.encode_bytes(value.as_bytes());
    }

    fn encode_uuid(&mut self, value: Uuid) {
        self.encode_primitive_header(VariantPrimitiveType::Uuid);
        self.encode_bytes(&value.into_bytes());
    }

    fn encode_date(&mut self, value: chrono::NaiveDate) {
        self.encode_primitive_header(VariantPrimitiveType::Date);
        let days_since_epoch = value.signed_duration_since(UNIX_EPOCH_DATE).num_days() as i32;
        self.encode_bytes(&days_since_epoch.to_le_bytes());
    }

    fn encode_time_micros(&mut self, value: chrono::NaiveTime) {
        self.encode_primitive_header(VariantPrimitiveType::TimeNTZ);
        let micros_from_midnight = value.num_seconds_from_midnight() as u64 * 1_000_000
            + value.nanosecond() as u64 / 1_000;
        self.encode_bytes(&micros_from_midnight.to_le_bytes());
    }

    fn encode_timestamp_micros(&mut self, value: chrono::DateTime<chrono::Utc>) {
        self.encode_primitive_header(VariantPrimitiveType::TimestampMicros);
        let micros = value.timestamp_micros();
        self.encode_bytes(&micros.to_le_bytes());
    }

    fn encode_timestamp_ntz_micros(&mut self, value: chrono::NaiveDateTime) {
        self.encode_primitive_header(VariantPrimitiveType::TimestampNtzMicros);
        let micros = value.and_utc().timestamp_micros();
        self.encode_bytes(&micros.to_le_bytes());
    }

    fn encode_timestamp_nanos(&mut self, value: chrono::DateTime<chrono::Utc>) {
        self.encode_primitive_header(VariantPrimitiveType::TimestampNanos);
        let nanos = value.timestamp_nanos_opt().unwrap();
        self.encode_bytes(&nanos.to_le_bytes());
    }

    fn encode_timestamp_ntz_nanos(&mut self, value: chrono::NaiveDateTime) {
        self.encode_primitive_header(VariantPrimitiveType::TimestampNtzNanos);
        let nanos = value.and_utc().timestamp_nanos_opt().unwrap();
        self.encode_bytes(&nanos.to_le_bytes());
    }

    fn encode_variant_object(
        &mut self,
        m: &HashMap<&str, usize>,
        v: &VariantObject,
    ) -> Result<(), ArrowError> {
        let names = v.names();

        let mut id_seq = Vec::with_capacity(names.len());
        let mut values = Vec::with_capacity(names.len());
        for name in names.as_slice() {
            let index = m.get(name.as_str()).ok_or_else(|| {
                ArrowError::SchemaError(format!("name {} not found in metadata", name))
            })?;
            id_seq.push(*index);
            values.push(Self::encode(m, &v.try_field_with_name(name)?)?);
        }

        // 1. encode object header
        let is_large = names.len() > u8::MAX as usize;

        let max_id = id_seq.iter().max().unwrap();
        let id_size = int_size(*max_id);

        let data_size = values.iter().map(|v| v.len() as u64).sum::<u64>();
        let offset_size = int_size(data_size as usize);

        self.encode_object_header(is_large, id_size, offset_size);

        // 2. num elements
        let num_fields = names.len();
        if is_large {
            self.encode_bytes(&(num_fields as u32).to_le_bytes());
        } else {
            self.0.push(num_fields as u8);
        }

        // 3. field_id
        for id in id_seq.iter() {
            self.encode_binary(&id.to_le_bytes()[..id_size as usize]);
        }

        // 4. field_offsets
        let mut cur_offset = 0usize;
        for value in values.iter() {
            self.encode_binary(&cur_offset.to_le_bytes()[..offset_size as usize]);
            cur_offset += value.len();
        }
        // final offset is the end of the value.
        self.encode_binary(&cur_offset.to_le_bytes()[..offset_size as usize]);

        // 5. value
        for value in values.iter() {
            self.0.extend_from_slice(value);
        }

        Ok(())
    }

    fn encode_variant_list(
        &mut self,
        m: &HashMap<&str, usize>,
        v: &VariantList,
    ) -> Result<(), ArrowError> {
        let num_elements = v.num_elements();
        let mut values = Vec::with_capacity(num_elements);
        for i in 0..num_elements {
            values.push(Self::encode(m, &v.try_field_with_index(i)?)?);
        }

        // 1. encode list header
        let data_size = values.iter().map(|v| v.len() as u64).sum::<u64>();
        let is_large = num_elements > u8::MAX as usize;
        let offset_size = int_size(data_size as usize);
        self.encode_array_header(is_large, offset_size);

        // 2. num elements
        if is_large {
            self.encode_bytes(&(num_elements as u32).to_le_bytes());
        } else {
            self.0.push(num_elements as u8);
        }

        // 3. field_offsets
        let mut cur_offset = 0usize;
        for value in values.iter() {
            self.encode_binary(&cur_offset.to_le_bytes()[..offset_size as usize]);
            cur_offset += value.len();
        }
        // final offset is the end of the value.
        self.encode_binary(&cur_offset.to_le_bytes()[..offset_size as usize]);

        // 4. value
        for value in values.iter() {
            self.0.extend_from_slice(value);
        }

        Ok(())
    }

    fn encode_primitive_header(&mut self, primitive_type: VariantPrimitiveType) {
        self.0.push(primitive_header(primitive_type));
    }

    fn encode_object_header(&mut self, large: bool, id_size: u8, offset_size: u8) {
        let large_bit = if large { 1 } else { 0 };
        let header = (large_bit << 6)
            | ((id_size - 1) << 4)
            | ((offset_size - 1) << 2)
            | VariantBasicType::Object as u8;
        self.0.push(header);
    }

    fn encode_array_header(&mut self, large: bool, offset_size: u8) {
        let large_bit = if large { 1 } else { 0 };
        let header =
            (large_bit << (2 + 2)) | ((offset_size - 1) << 2) | VariantBasicType::List as u8;
        self.0.push(header);
    }

    fn encode_bytes(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }
}

fn primitive_header(primitive_type: VariantPrimitiveType) -> u8 {
    (primitive_type as u8) << 2 | VariantBasicType::Primitive as u8
}
