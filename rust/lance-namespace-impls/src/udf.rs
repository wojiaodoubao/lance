// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

/// DataFusion UDFs used by partitioned namespace implementation.
use arrow::array::{
    Array, ArrayRef, BinaryArray, Date32Array, Date64Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int32Builder, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::util::display::array_value_to_string;
use arrow_schema::DataType;
use datafusion_functions::utils::make_scalar_function;
use lance::deps::datafusion::error::DataFusionError;
use lance::deps::datafusion::logical_expr::{ScalarUDF, Signature, SimpleScalarUDF, Volatility};
use lance_core::Error;
use snafu::location;
use std::sync::{Arc, LazyLock};

/// A variadic murmur3 UDF.
///
/// - Accepts any number of arguments (>= 1)
/// - Accepts any argument types (bytes are derived via `scalar_to_bytes`)
/// - Skips NULL arguments; returns NULL if all arguments are NULL for a row
fn murmur3_multi() -> ScalarUDF {
    let function = Arc::new(make_scalar_function(
        |args: &[ArrayRef]| {
            if args.is_empty() {
                return Err(DataFusionError::Execution(
                    "murmur3_multi expects at least 1 argument".to_string(),
                ));
            }

            let len = args[0].len();
            for a in args.iter().skip(1) {
                if a.len() != len {
                    return Err(DataFusionError::Execution(
                        "All arguments to murmur3_multi must have the same length".to_string(),
                    ));
                }
            }

            let mut builder = Int32Builder::new();
            for row in 0..len {
                let mut buf = Vec::new();
                let mut has_value = false;

                for col in args {
                    let array = col.as_ref();
                    if array.is_null(row) {
                        continue;
                    }
                    has_value = true;
                    let value_bytes = scalar_to_bytes(array, row)
                        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                    buf.extend_from_slice(&value_bytes);
                }

                if !has_value {
                    builder.append_null();
                    continue;
                }

                let hash = murmur3::murmur3_32(&mut std::io::Cursor::new(&buf), 0)? as i32;
                builder.append_value(hash);
            }

            Ok(Arc::new(builder.finish()) as ArrayRef)
        },
        vec![],
    ));

    ScalarUDF::from(SimpleScalarUDF::new_with_signature(
        "murmur3_multi",
        Signature::variadic_any(Volatility::Immutable),
        DataType::Int32,
        function,
    ))
}

pub(crate) fn scalar_to_bytes(array: &dyn Array, row: usize) -> lance_core::Result<Vec<u8>> {
    if array.is_null(row) {
        return Ok(Vec::new());
    }

    macro_rules! to_bytes_primitive {
        ($array_ty:ty, $array:expr, $row:expr) => {{
            let a =
                $array
                    .as_any()
                    .downcast_ref::<$array_ty>()
                    .ok_or_else(|| Error::InvalidInput {
                        source: format!(
                            "Expected array type '{}' but got '{:?}'",
                            stringify!($array_ty),
                            $array.data_type()
                        )
                        .into(),
                        location: location!(),
                    })?;
            a.value($row).to_le_bytes().to_vec()
        }};
    }

    macro_rules! to_bytes_utf8_like {
        ($array_ty:ty, $array:expr, $row:expr) => {{
            let a =
                $array
                    .as_any()
                    .downcast_ref::<$array_ty>()
                    .ok_or_else(|| Error::InvalidInput {
                        source: format!(
                            "Expected array type '{}' but got '{:?}'",
                            stringify!($array_ty),
                            $array.data_type()
                        )
                        .into(),
                        location: location!(),
                    })?;
            a.value($row).as_bytes().to_vec()
        }};
    }

    macro_rules! to_bytes_binary_like {
        ($array_ty:ty, $array:expr, $row:expr) => {{
            let a =
                $array
                    .as_any()
                    .downcast_ref::<$array_ty>()
                    .ok_or_else(|| Error::InvalidInput {
                        source: format!(
                            "Expected array type '{}' but got '{:?}'",
                            stringify!($array_ty),
                            $array.data_type()
                        )
                        .into(),
                        location: location!(),
                    })?;
            a.value($row).to_vec()
        }};
    }

    let dt = array.data_type();
    let bytes = match dt {
        DataType::Int8 => to_bytes_primitive!(Int8Array, array, row),
        DataType::Int16 => to_bytes_primitive!(Int16Array, array, row),
        DataType::Int32 => to_bytes_primitive!(Int32Array, array, row),
        DataType::Int64 => to_bytes_primitive!(Int64Array, array, row),
        DataType::UInt8 => to_bytes_primitive!(UInt8Array, array, row),
        DataType::UInt16 => to_bytes_primitive!(UInt16Array, array, row),
        DataType::UInt32 => to_bytes_primitive!(UInt32Array, array, row),
        DataType::UInt64 => to_bytes_primitive!(UInt64Array, array, row),
        DataType::Float32 => to_bytes_primitive!(Float32Array, array, row),
        DataType::Float64 => to_bytes_primitive!(Float64Array, array, row),
        DataType::Date32 => to_bytes_primitive!(Date32Array, array, row),
        DataType::Date64 => to_bytes_primitive!(Date64Array, array, row),
        DataType::Timestamp(arrow_schema::TimeUnit::Second, _) => {
            to_bytes_primitive!(TimestampSecondArray, array, row)
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
            to_bytes_primitive!(TimestampMillisecondArray, array, row)
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
            to_bytes_primitive!(TimestampMicrosecondArray, array, row)
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
            to_bytes_primitive!(TimestampNanosecondArray, array, row)
        }
        DataType::Utf8 => to_bytes_utf8_like!(StringArray, array, row),
        DataType::LargeUtf8 => to_bytes_utf8_like!(LargeStringArray, array, row),
        DataType::Binary => to_bytes_binary_like!(BinaryArray, array, row),
        DataType::LargeBinary => to_bytes_binary_like!(LargeBinaryArray, array, row),
        _ => {
            let s = array_value_to_string(array, row).map_err(lance_core::Error::from)?;
            s.into_bytes()
        }
    };

    Ok(bytes)
}

pub static MURMUR3_MULTI_UDF: LazyLock<ScalarUDF> = LazyLock::new(murmur3_multi);
