// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::error::{Error, Result};
use crate::traits::IntoJava;
use crate::utils::{to_java_map, to_rust_map};
use crate::JNIEnvExt;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{Field as ArrowField, Schema as ArrowSchema, TimeUnit, UnionFields};
use jni::objects::{JByteArray, JList, JMap, JObject, JValue};
use jni::sys::{jboolean, jint, jlong};
use jni::JNIEnv;
use lance_core::datatypes::{Dictionary, Encoding, Field, LogicalType, Schema};
use std::collections::HashMap;
use std::sync::Arc;

#[no_mangle]
pub extern "system" fn Java_org_lance_test_JniTestHelper_roundTripLanceField<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    java_field: JObject<'local>,
) -> JObject<'local> {
    ok_or_throw!(env, inner_round_trip_lance_field(&mut env, java_field))
}

fn inner_round_trip_lance_field<'local>(
    env: &mut JNIEnv<'local>,
    java_field: JObject<'local>,
) -> Result<JObject<'local>> {
    let rust_field = convert_lance_field_from_java_field(env, &java_field)?;
    let round_tripped = convert_lance_field_to_java_field(env, &rust_field)?;
    Ok(round_tripped)
}

impl IntoJava for Schema {
    fn into_java<'local>(self, env: &mut JNIEnv<'local>) -> Result<JObject<'local>> {
        let jfield_list = env.new_object("java/util/ArrayList", "()V", &[])?;
        for lance_field in self.fields.iter() {
            let java_field = convert_lance_field_to_java_field(env, lance_field)?;
            env.call_method(
                &jfield_list,
                "add",
                "(Ljava/lang/Object;)Z",
                &[JValue::Object(&java_field)],
            )?;
        }
        let metadata = to_java_map(env, &self.metadata)?;
        Ok(env.new_object(
            "org/lance/schema/LanceSchema",
            "(Ljava/util/List;Ljava/util/Map;)V",
            &[JValue::Object(&jfield_list), JValue::Object(&metadata)],
        )?)
    }
}

pub fn convert_lance_field_to_java_field<'local>(
    env: &mut JNIEnv<'local>,
    lance_field: &Field,
) -> Result<JObject<'local>> {
    let name = env.new_string(&lance_field.name)?;
    let children = convert_children_fields(env, lance_field)?;
    let metadata = to_java_map(env, &lance_field.metadata)?;
    let arrow_type = convert_arrow_type(env, &lance_field.data_type())?;
    let ctor_sig = "(IILjava/lang/String;".to_owned()
        + "ZLorg/apache/arrow/vector/types/pojo/ArrowType;"
        + "Ljava/util/Map;"
        + "Ljava/util/List;"
        + "ZILorg/lance/schema/LanceField$Encoding;"
        + "Lorg/lance/schema/LanceField$LanceDictionary;)V";
    let pk_position = lance_field.unenforced_primary_key_position.unwrap_or(0) as jint;

    // Map Rust Encoding to Java LanceField.Encoding
    let encoding_class = "org/lance/schema/LanceField$Encoding";
    let jencoding = match &lance_field.encoding {
        Some(Encoding::Plain) => Some(
            env.get_static_field(
                encoding_class,
                "PLAIN",
                "Lorg/lance/schema/LanceField$Encoding;",
            )?
            .l()?,
        ),
        Some(Encoding::VarBinary) => Some(
            env.get_static_field(
                encoding_class,
                "VAR_BINARY",
                "Lorg/lance/schema/LanceField$Encoding;",
            )?
            .l()?,
        ),
        Some(Encoding::Dictionary) => Some(
            env.get_static_field(
                encoding_class,
                "DICTIONARY",
                "Lorg/lance/schema/LanceField$Encoding;",
            )?
            .l()?,
        ),
        Some(Encoding::RLE) => Some(
            env.get_static_field(
                encoding_class,
                "RLE",
                "Lorg/lance/schema/LanceField$Encoding;",
            )?
            .l()?,
        ),
        None => None,
    };

    // Dictionary: map Rust Dictionary to Java LanceDictionary
    let jdict = if let Some(dict) = &lance_field.dictionary {
        let offset = dict.offset as jlong;
        let length = dict.length as jlong;

        let values_bytes = if let Some(values_arr) = &dict.values {
            encode_dictionary_values(values_arr)?
        } else {
            Vec::new()
        };

        let jvalues: JByteArray = env.byte_array_from_slice(&values_bytes)?;
        env.new_object(
            "org/lance/schema/LanceField$LanceDictionary",
            "(JJ[B)V",
            &[
                JValue::Long(offset),
                JValue::Long(length),
                JValue::Object(&jvalues),
            ],
        )?
    } else {
        JObject::null()
    };

    let field_obj = env.new_object(
        "org/lance/schema/LanceField",
        ctor_sig.as_str(),
        &[
            JValue::Int(lance_field.id as jint),
            JValue::Int(lance_field.parent_id as jint),
            JValue::Object(&JObject::from(name)),
            JValue::Bool(lance_field.nullable as jboolean),
            JValue::Object(&arrow_type),
            JValue::Object(&metadata),
            JValue::Object(&children),
            JValue::Bool(lance_field.is_unenforced_primary_key() as jboolean),
            JValue::Int(pk_position),
            JValue::Object(&jencoding.unwrap_or(JObject::null())),
            JValue::Object(&jdict),
        ],
    )?;

    Ok(field_obj)
}

fn convert_children_fields<'local>(
    env: &mut JNIEnv<'local>,
    lance_field: &Field,
) -> Result<JObject<'local>> {
    let children_list = env.new_object("java/util/ArrayList", "()V", &[])?;
    for lance_field in lance_field.children.iter() {
        let field = convert_lance_field_to_java_field(env, lance_field)?;
        env.call_method(
            &children_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[JValue::Object(&field)],
        )?;
    }
    Ok(children_list)
}

/// Construct a Rust Field from a Java LanceField.
pub fn convert_lance_field_from_java_field<'local>(
    env: &mut JNIEnv<'local>,
    java_field: &JObject<'local>,
) -> Result<Field> {
    // Basic attributes
    let name = env.get_string_from_method(java_field, "getName")?;
    let id = env.call_method(java_field, "getId", "()I", &[])?.i()?;
    let parent_id = env
        .call_method(java_field, "getParentId", "()I", &[])?
        .i()?;
    let nullable = env.call_method(java_field, "isNullable", "()Z", &[])?.z()?;

    // Metadata Map<String, String>
    let java_metadata = env
        .call_method(java_field, "getMetadata", "()Ljava/util/Map;", &[])?
        .l()?;

    let metadata: HashMap<String, String> = if java_metadata.is_null() {
        HashMap::new()
    } else {
        let jmap = JMap::from_env(env, &java_metadata)?;
        to_rust_map(env, &jmap)?
    };

    // Child fields List<LanceField>
    let java_children = env
        .call_method(java_field, "getChildren", "()Ljava/util/List;", &[])?
        .l()?;

    let mut children = Vec::new();
    if !java_children.is_null() {
        let jlist: JList = env.get_list(&java_children)?;
        let mut iter = jlist.iter(env)?;
        children = Vec::with_capacity(jlist.size(env)? as usize);
        while let Some(child_obj) = iter.next(env)? {
            children.push(convert_lance_field_from_java_field(env, &child_obj)?);
        }
    }

    // Derive LogicalType from the Java ArrowType via a Rust DataType,
    // sharing the same mapping as LogicalType::try_from(&DataType).
    let arrow_type_obj = env
        .call_method(
            java_field,
            "getType",
            "()Lorg/apache/arrow/vector/types/pojo/ArrowType;",
            &[],
        )?
        .l()?;
    let data_type = data_type_from_java_arrow_type(env, &arrow_type_obj, &children)?;
    let logical_type = LogicalType::try_from(&data_type)?;

    // Primary key information
    let is_unenforced_pk = env
        .call_method(java_field, "isUnenforcedPrimaryKey", "()Z", &[])?
        .z()?;

    let java_pk_pos = env
        .call_method(
            java_field,
            "getUnenforcedPrimaryKeyPosition",
            "()Ljava/util/OptionalInt;",
            &[],
        )?
        .l()?;

    let unenforced_primary_key_position = if java_pk_pos.is_null() {
        if is_unenforced_pk {
            Some(0)
        } else {
            None
        }
    } else {
        let has_pos = env
            .call_method(&java_pk_pos, "isPresent", "()Z", &[])?
            .z()?;
        if has_pos {
            let pos = env.call_method(&java_pk_pos, "getAsInt", "()I", &[])?.i()?;
            Some(pos as u32)
        } else if is_unenforced_pk {
            // When only the boolean PK flag is set, use 0 as a compatible sentinel position
            Some(0)
        } else {
            None
        }
    };

    // encoding: map LanceField.Encoding back to Rust Encoding
    let encoding_optional = env
        .call_method(java_field, "getEncoding", "()Ljava/util/Optional;", &[])?
        .l()?;

    let encoding: Option<Encoding> = env.get_optional(&encoding_optional, |env, enc_obj| {
        let name = env.get_string_from_method(&enc_obj, "name")?;
        match name.as_str() {
            "PLAIN" => Ok(Encoding::Plain),
            "VAR_BINARY" => Ok(Encoding::VarBinary),
            "DICTIONARY" => Ok(Encoding::Dictionary),
            "RLE" => Ok(Encoding::RLE),
            other => Err(Error::input_error(format!(
                "Unknown LanceField.Encoding value: {}",
                other
            ))),
        }
    })?;

    // Dictionary: fully reconstruct offset/length/values via LanceDictionary
    let dict_optional = env
        .call_method(java_field, "getDictionary", "()Ljava/util/Optional;", &[])?
        .l()?;

    let dictionary: Option<Dictionary> = env.get_optional(&dict_optional, |env, dict_obj| {
        let offset = env.call_method(&dict_obj, "getOffset", "()J", &[])?.j()? as usize;
        let length = env.call_method(&dict_obj, "getLength", "()J", &[])?.j()? as usize;

        let values_obj = env.call_method(&dict_obj, "getValues", "()[B", &[])?.l()?;

        let values = if values_obj.is_null() {
            None
        } else {
            let jbytes = JByteArray::from(values_obj);
            let bytes = env.convert_byte_array(&jbytes)?;
            Some(decode_dictionary_values(&bytes)?)
        };

        Ok(Dictionary {
            offset,
            length,
            values,
        })
    })?;

    Ok(Field {
        name,
        id,
        parent_id,
        logical_type,
        metadata,
        encoding,
        nullable,
        children,
        dictionary,
        unenforced_primary_key_position,
    })
}

/// Derive a Rust `DataType` from Java `ArrowType`.
fn data_type_from_java_arrow_type(
    env: &mut JNIEnv,
    arrow_type: &JObject,
    children: &[Field],
) -> Result<DataType> {
    // Int
    let int_class = env.find_class("org/apache/arrow/vector/types/pojo/ArrowType$Int")?;
    if env.is_instance_of(arrow_type, int_class)? {
        let bit_width = env
            .call_method(arrow_type, "getBitWidth", "()I", &[])?
            .i()?;
        let is_signed = env
            .call_method(arrow_type, "getIsSigned", "()Z", &[])?
            .z()?;

        let ty = match (bit_width, is_signed) {
            (8, true) => DataType::Int8,
            (8, false) => DataType::UInt8,
            (16, true) => DataType::Int16,
            (16, false) => DataType::UInt16,
            (32, true) => DataType::Int32,
            (32, false) => DataType::UInt32,
            (64, true) => DataType::Int64,
            (64, false) => DataType::UInt64,
            _ => {
                return Err(Error::input_error(format!(
                    "Unsupported Arrow Int type: bit_width={}, signed={}",
                    bit_width, is_signed
                )))
            }
        };
        return Ok(ty);
    }

    // Floating point
    let fp_class = env.find_class("org/apache/arrow/vector/types/pojo/ArrowType$FloatingPoint")?;
    if env.is_instance_of(arrow_type, fp_class)? {
        let precision_obj = env
            .call_method(
                arrow_type,
                "getPrecision",
                "()Lorg/apache/arrow/vector/types/FloatingPointPrecision;",
                &[],
            )?
            .l()?;
        let precision_str = env.get_string_from_method(&precision_obj, "name")?;
        let ty = match precision_str.as_str() {
            "HALF" => DataType::Float16,
            "SINGLE" => DataType::Float32,
            "DOUBLE" => DataType::Float64,
            other => {
                return Err(Error::input_error(format!(
                    "Unsupported FloatingPoint precision: {}",
                    other
                )))
            }
        };
        return Ok(ty);
    }

    // Utf8 / LargeUtf8 / Binary / LargeBinary
    let utf8_class = env.find_class("org/apache/arrow/vector/types/pojo/ArrowType$Utf8")?;
    if env.is_instance_of(arrow_type, utf8_class)? {
        return Ok(DataType::Utf8);
    }

    let large_utf8_class =
        env.find_class("org/apache/arrow/vector/types/pojo/ArrowType$LargeUtf8")?;
    if env.is_instance_of(arrow_type, large_utf8_class)? {
        return Ok(DataType::LargeUtf8);
    }

    let binary_class = env.find_class("org/apache/arrow/vector/types/pojo/ArrowType$Binary")?;
    if env.is_instance_of(arrow_type, binary_class)? {
        return Ok(DataType::Binary);
    }

    let large_binary_class =
        env.find_class("org/apache/arrow/vector/types/pojo/ArrowType$LargeBinary")?;
    if env.is_instance_of(arrow_type, large_binary_class)? {
        return Ok(DataType::LargeBinary);
    }

    // List.
    let list_class = env.find_class("org/apache/arrow/vector/types/pojo/ArrowType$List")?;
    if env.is_instance_of(arrow_type, list_class)? {
        let elem_field = if let Some(child) = children.first() {
            ArrowField::from(child)
        } else {
            ArrowField::new("item", DataType::Null, true)
        };
        return Ok(DataType::List(Arc::new(elem_field)));
    }

    // LargeList.
    let large_list_class =
        env.find_class("org/apache/arrow/vector/types/pojo/ArrowType$LargeList")?;
    if env.is_instance_of(arrow_type, large_list_class)? {
        let elem_field = if let Some(child) = children.first() {
            ArrowField::from(child)
        } else {
            ArrowField::new("item", DataType::Null, true)
        };
        return Ok(DataType::LargeList(Arc::new(elem_field)));
    }

    // Struct.
    let struct_class = env.find_class("org/apache/arrow/vector/types/pojo/ArrowType$Struct")?;
    if env.is_instance_of(arrow_type, struct_class)? {
        let struct_fields: Vec<ArrowField> = children.iter().map(ArrowField::from).collect();
        return Ok(DataType::Struct(struct_fields.into()));
    }

    Err(Error::input_error(
        "Unsupported ArrowType for LogicalType conversion in JNI schema".to_string(),
    ))
}

/// Construct a Rust Schema from a Java LanceSchema.
pub fn convert_lance_schema_from_java<'local>(
    env: &mut JNIEnv<'local>,
    java_schema: &JObject<'local>,
) -> Result<Schema> {
    // Top-level fields List<LanceField>
    let java_fields_list = env
        .call_method(java_schema, "fields", "()Ljava/util/List;", &[])?
        .l()?;

    let jlist: JList = env.get_list(&java_fields_list)?;
    let mut iter = jlist.iter(env)?;
    let mut fields = Vec::with_capacity(jlist.size(env)? as usize);
    while let Some(field_obj) = iter.next(env)? {
        let field = convert_lance_field_from_java_field(env, &field_obj)?;
        fields.push(field);
    }

    // Schema metadata Map<String, String>
    let java_metadata = env
        .call_method(java_schema, "metadata", "()Ljava/util/Map;", &[])?
        .l()?;

    let metadata = if java_metadata.is_null() {
        HashMap::new()
    } else {
        let jmap = JMap::from_env(env, &java_metadata)?;
        to_rust_map(env, &jmap)?
    };

    Ok(Schema { fields, metadata })
}

fn encode_dictionary_values(values: &ArrayRef) -> Result<Vec<u8>> {
    use std::sync::Arc;

    let field = ArrowField::new("value", values.data_type().clone(), true);
    let schema = ArrowSchema::new(vec![field]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![values.clone()])
        .map_err(|e| Error::input_error(format!("Failed to build dictionary batch: {e}")))?;

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).map_err(|e| {
            Error::input_error(format!("Failed to create dictionary StreamWriter: {e}",))
        })?;
        writer
            .write(&batch)
            .map_err(|e| Error::input_error(format!("Failed to write dictionary batch: {e}",)))?;
        writer
            .finish()
            .map_err(|e| Error::input_error(format!("Failed to finish dictionary stream: {e}",)))?;
    }
    Ok(buf)
}

fn decode_dictionary_values(bytes: &[u8]) -> Result<ArrayRef> {
    use std::io::Cursor;

    let cursor = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None).map_err(|e| {
        Error::input_error(format!("Failed to create dictionary StreamReader: {e}",))
    })?;
    let batch = reader
        .next()
        .transpose()
        .map_err(|e| Error::input_error(format!("Failed to read dictionary batch: {e}",)))?
        .ok_or_else(|| Error::input_error("Empty dictionary IPC stream".to_string()))?;

    if batch.num_columns() != 1 {
        return Err(Error::input_error(format!(
            "Dictionary batch expected 1 column, got {}",
            batch.num_columns()
        )));
    }

    Ok(batch.column(0).clone())
}

pub fn convert_arrow_type<'local>(
    env: &mut JNIEnv<'local>,
    arrow_type: &DataType,
) -> Result<JObject<'local>> {
    match arrow_type {
        DataType::Null => convert_null_type(env),
        DataType::Boolean => convert_boolean_type(env),
        DataType::Int8 => convert_int_type(env, 8, true),
        DataType::Int16 => convert_int_type(env, 16, true),
        DataType::Int32 => convert_int_type(env, 32, true),
        DataType::Int64 => convert_int_type(env, 64, true),
        DataType::UInt8 => convert_int_type(env, 8, false),
        DataType::UInt16 => convert_int_type(env, 16, false),
        DataType::UInt32 => convert_int_type(env, 32, false),
        DataType::UInt64 => convert_int_type(env, 64, false),
        DataType::Float16 => convert_floating_point_type(env, "HALF"),
        DataType::Float32 => convert_floating_point_type(env, "SINGLE"),
        DataType::Float64 => convert_floating_point_type(env, "DOUBLE"),
        DataType::Utf8 => convert_utf8_type(env, false),
        DataType::LargeUtf8 => convert_utf8_type(env, true),
        DataType::Binary => convert_binary_type(env, false),
        DataType::LargeBinary => convert_binary_type(env, true),
        DataType::FixedSizeBinary(len) => convert_fixed_size_binary_type(env, *len),
        DataType::Date32 => convert_date_type(env, "DAY"),
        DataType::Date64 => convert_date_type(env, "MILLISECOND"),
        DataType::Time32(unit) => convert_time_type(env, *unit, 32),
        DataType::Time64(unit) => convert_time_type(env, *unit, 64),
        DataType::Timestamp(unit, tz) => convert_timestamp_type(env, *unit, tz.as_deref()),
        DataType::Duration(unit) => convert_duration_type(env, *unit),
        DataType::Decimal128(precision, scale) => {
            convert_decimal_type(env, *precision, *scale, 128)
        }
        DataType::Decimal256(precision, scale) => {
            convert_decimal_type(env, *precision, *scale, 256)
        }
        DataType::List(..) => convert_list_type(env, false),
        DataType::LargeList(..) => convert_list_type(env, true),
        DataType::FixedSizeList(.., len) => convert_fixed_size_list_type(env, *len),
        DataType::Struct(..) => convert_struct_type(env),
        DataType::Union(fields, mode) => convert_union_type(env, fields, *mode),
        DataType::Map(.., keys_sorted) => convert_map_type(env, *keys_sorted),
        _ => Err(Error::input_error(
            "ArrowSchema conversion error".to_string(),
        )),
    }
}

fn convert_null_type<'local>(env: &mut JNIEnv<'local>) -> Result<JObject<'local>> {
    Ok(env
        .get_static_field(
            "org/apache/arrow/vector/types/pojo/ArrowType$Null",
            "INSTANCE",
            "Lorg/apache/arrow/vector/types/pojo/ArrowType$Null;",
        )?
        .l()?)
}

fn convert_boolean_type<'local>(env: &mut JNIEnv<'local>) -> Result<JObject<'local>> {
    Ok(env
        .get_static_field(
            "org/apache/arrow/vector/types/pojo/ArrowType$Bool",
            "INSTANCE",
            "Lorg/apache/arrow/vector/types/pojo/ArrowType$Bool;",
        )?
        .l()?)
}

fn convert_int_type<'local>(
    env: &mut JNIEnv<'local>,
    bit_width: i32,
    is_signed: bool,
) -> Result<JObject<'local>> {
    Ok(env.new_object(
        "org/apache/arrow/vector/types/pojo/ArrowType$Int",
        "(IZ)V",
        &[
            JValue::Int(bit_width as jint),
            JValue::Bool(is_signed as jboolean),
        ],
    )?)
}

fn convert_floating_point_type<'local>(
    env: &mut JNIEnv<'local>,
    precision: &str,
) -> Result<JObject<'local>> {
    let precision_enum = env
        .get_static_field(
            "org/apache/arrow/vector/types/FloatingPointPrecision",
            precision,
            "Lorg/apache/arrow/vector/types/FloatingPointPrecision;",
        )?
        .l()?;

    Ok(env.new_object(
        "org/apache/arrow/vector/types/pojo/ArrowType$FloatingPoint",
        "(Lorg/apache/arrow/vector/types/FloatingPointPrecision;)V",
        &[JValue::Object(&precision_enum)],
    )?)
}

fn convert_utf8_type<'local>(env: &mut JNIEnv<'local>, is_large: bool) -> Result<JObject<'local>> {
    let class_name = if is_large {
        "org/apache/arrow/vector/types/pojo/ArrowType$LargeUtf8"
    } else {
        "org/apache/arrow/vector/types/pojo/ArrowType$Utf8"
    };

    convert_arrow_type_by_class_name(env, class_name)
}

fn convert_binary_type<'local>(
    env: &mut JNIEnv<'local>,
    is_large: bool,
) -> Result<JObject<'local>> {
    let class_name = if is_large {
        "org/apache/arrow/vector/types/pojo/ArrowType$LargeBinary"
    } else {
        "org/apache/arrow/vector/types/pojo/ArrowType$Binary"
    };

    convert_arrow_type_by_class_name(env, class_name)
}

fn convert_arrow_type_by_class_name<'local>(
    env: &mut JNIEnv<'local>,
    class_name: &str,
) -> Result<JObject<'local>> {
    let class = env.find_class(class_name)?;
    let field_sig = format!("L{};", class_name);
    let instance = env.get_static_field(class, "INSTANCE", &field_sig)?.l()?;
    Ok(instance)
}

fn convert_fixed_size_binary_type<'local>(
    env: &mut JNIEnv<'local>,
    byte_width: i32,
) -> Result<JObject<'local>> {
    let class = env.find_class("org/apache/arrow/vector/types/pojo/ArrowType$FixedSizeBinary")?;
    Ok(env.new_object(class, "(I)V", &[JValue::Int(byte_width)])?)
}

fn convert_date_type<'local>(env: &mut JNIEnv<'local>, unit: &str) -> Result<JObject<'local>> {
    let class = env.find_class("org/apache/arrow/vector/types/pojo/ArrowType$Date")?;
    let unit_enum = env
        .get_static_field(
            "org/apache/arrow/vector/types/DateUnit",
            unit,
            "Lorg/apache/arrow/vector/types/DateUnit;",
        )?
        .l()?;

    Ok(env.new_object(
        class,
        "(Lorg/apache/arrow/vector/types/DateUnit;)V",
        &[JValue::Object(&unit_enum)],
    )?)
}

fn convert_time_type<'local>(
    env: &mut JNIEnv<'local>,
    unit: TimeUnit,
    bit_width: i32,
) -> Result<JObject<'local>> {
    let class = env.find_class("org/apache/arrow/vector/types/pojo/ArrowType$Time")?;
    let unit_str = match unit {
        TimeUnit::Second => "SECOND",
        TimeUnit::Millisecond => "MILLISECOND",
        TimeUnit::Microsecond => "MICROSECOND",
        TimeUnit::Nanosecond => "NANOSECOND",
    };

    let unit_enum = env
        .get_static_field(
            "org/apache/arrow/vector/types/TimeUnit",
            unit_str,
            "Lorg/apache/arrow/vector/types/TimeUnit;",
        )?
        .l()?;

    Ok(env.new_object(
        class,
        "(Lorg/apache/arrow/vector/types/TimeUnit;I)V",
        &[JValue::Object(&unit_enum), JValue::Int(bit_width)],
    )?)
}

fn convert_timestamp_type<'local>(
    env: &mut JNIEnv<'local>,
    unit: TimeUnit,
    timezone: Option<&str>,
) -> Result<JObject<'local>> {
    let class = env.find_class("org/apache/arrow/vector/types/pojo/ArrowType$Timestamp")?;
    let unit_str = match unit {
        TimeUnit::Second => "SECOND",
        TimeUnit::Millisecond => "MILLISECOND",
        TimeUnit::Microsecond => "MICROSECOND",
        TimeUnit::Nanosecond => "NANOSECOND",
    };

    let unit_enum = env
        .get_static_field(
            "org/apache/arrow/vector/types/TimeUnit",
            unit_str,
            "Lorg/apache/arrow/vector/types/TimeUnit;",
        )?
        .l()?;

    let timezone_str = timezone.unwrap_or("-");
    let j_timezone = env.new_string(timezone_str)?;

    Ok(env.new_object(
        class,
        "(Lorg/apache/arrow/vector/types/TimeUnit;Ljava/lang/String;)V",
        &[JValue::Object(&unit_enum), JValue::Object(&j_timezone)],
    )?)
}

fn convert_duration_type<'local>(
    env: &mut JNIEnv<'local>,
    unit: TimeUnit,
) -> Result<JObject<'local>> {
    let class = env.find_class("org/apache/arrow/vector/types/pojo/ArrowType$Duration")?;
    let unit_str = match unit {
        TimeUnit::Second => "SECOND",
        TimeUnit::Millisecond => "MILLISECOND",
        TimeUnit::Microsecond => "MICROSECOND",
        TimeUnit::Nanosecond => "NANOSECOND",
    };

    let unit_enum = env
        .get_static_field(
            "org/apache/arrow/vector/types/TimeUnit",
            unit_str,
            "Lorg/apache/arrow/vector/types/TimeUnit;",
        )?
        .l()?;

    Ok(env.new_object(
        class,
        "(Lorg/apache/arrow/vector/types/TimeUnit;)V",
        &[JValue::Object(&unit_enum)],
    )?)
}

fn convert_decimal_type<'local>(
    env: &mut JNIEnv<'local>,
    precision: u8,
    scale: i8,
    bit_width: i32,
) -> Result<JObject<'local>> {
    let class = env.find_class("org/apache/arrow/vector/types/pojo/ArrowType$Decimal")?;
    Ok(env.new_object(
        class,
        "(III)V",
        &[
            JValue::Int(precision as jint),
            JValue::Int(scale as jint),
            JValue::Int(bit_width),
        ],
    )?)
}

fn convert_list_type<'local>(env: &mut JNIEnv<'local>, is_large: bool) -> Result<JObject<'local>> {
    let class_name = if is_large {
        "org/apache/arrow/vector/types/pojo/ArrowType$LargeList"
    } else {
        "org/apache/arrow/vector/types/pojo/ArrowType$List"
    };

    convert_arrow_type_by_class_name(env, class_name)
}

fn convert_fixed_size_list_type<'local>(
    env: &mut JNIEnv<'local>,
    list_size: i32,
) -> Result<JObject<'local>> {
    Ok(env.new_object(
        "org/apache/arrow/vector/types/pojo/ArrowType$FixedSizeList",
        "(I)V",
        &[JValue::Int(list_size)],
    )?)
}

fn convert_struct_type<'local>(env: &mut JNIEnv<'local>) -> Result<JObject<'local>> {
    Ok(env
        .get_static_field(
            "org/apache/arrow/vector/types/pojo/ArrowType$Struct",
            "INSTANCE",
            "Lorg/apache/arrow/vector/types/pojo/ArrowType$Struct;",
        )?
        .l()?)
}

fn convert_union_type<'local>(
    env: &mut JNIEnv<'local>,
    fields: &UnionFields,
    mode: arrow_schema::UnionMode,
) -> Result<JObject<'local>> {
    let class = env.find_class("org/apache/arrow/vector/types/pojo/ArrowType$Union")?;

    let mode_str = match mode {
        arrow_schema::UnionMode::Sparse => "SPARSE",
        arrow_schema::UnionMode::Dense => "DENSE",
    };
    let mode_enum = env
        .get_static_field(
            "org/apache/arrow/vector/types/UnionMode",
            mode_str,
            "Lorg/apache/arrow/vector/types/UnionMode;",
        )?
        .l()?;

    let jarray = env.new_int_array(fields.size() as jint)?;

    let mut rust_array = vec![0; fields.size()];
    for (i, (type_id, _)) in fields.iter().enumerate() {
        rust_array[i] = type_id as i32;
    }
    env.set_int_array_region(&jarray, 0, &rust_array)?;

    Ok(env.new_object(
        class,
        "(Lorg/apache/arrow/vector/types/UnionMode;[I)V",
        &[JValue::Object(&mode_enum), JValue::Object(&jarray)],
    )?)
}

fn convert_map_type<'local>(
    env: &mut JNIEnv<'local>,
    keys_sorted: bool,
) -> Result<JObject<'local>> {
    Ok(env.new_object(
        "org/apache/arrow/vector/types/pojo/ArrowType$Map",
        "(Z)V",
        &[JValue::Bool(keys_sorted as jboolean)],
    )?)
}
