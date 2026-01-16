// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::partition::scalar_to_bytes;
/// DataFusion UDFs used by partitioned namespace implementation.
use arrow::array::{Array, ArrayRef, Int32Builder};
use arrow_schema::DataType;
use datafusion_functions::utils::make_scalar_function;
use lance::deps::datafusion::error::DataFusionError;
use lance::deps::datafusion::logical_expr::{ScalarUDF, Signature, SimpleScalarUDF, Volatility};
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

pub static MURMUR3_MULTI_UDF: LazyLock<ScalarUDF> = LazyLock::new(murmur3_multi);
