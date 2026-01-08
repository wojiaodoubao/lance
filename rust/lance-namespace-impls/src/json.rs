// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_namespace::models::JsonArrowDataType;
use serde::{Deserialize, Serialize};

// TODO: remove this after https://github.com/lance-format/lance-namespace/pull/297 is merged
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonPartitionField {
    field_id: String,
    source_ids: Vec<i32>,
    transform: JsonTransform,
    expression: String,
    result_type: JsonArrowDataType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonTransform {
    #[serde(rename = "type")]
    r#type: String, // The transform type
    #[serde(rename = "num_buckets", skip_serializing_if = "Option::is_none")]
    num_buckets: Option<i32>, // Number of buckets N
    #[serde(rename = "width", skip_serializing_if = "Option::is_none")]
    width: Option<i32>, // Truncation width W
}
