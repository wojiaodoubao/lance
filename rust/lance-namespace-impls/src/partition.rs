// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
// NOTE: Keep this module warning-clean; avoid `#![allow(unused)]`.

use crate::dir::manifest::{ManifestObject, EXTENDED_PREFIX};
use crate::dir::manifest_ext::{
    CreateMultiNamespacesRequestBuilder, ManifestNamespaceExt as ManifestNamespaceCreateExt,
};
use crate::udf::MURMUR3_MULTI_UDF;
use crate::{context::DynamicContextProvider, DirectoryNamespace, ManifestNamespace};
use arrow::array::{
    new_null_array, Array, ArrayRef, BinaryArray, Date32Array, Date64Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use arrow::util::display::array_value_to_string;
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use bytes::Bytes;
use lance::deps::datafusion::logical_expr::{Expr, Operator};
use lance::deps::datafusion::prelude::{col, lit, SessionContext};
use lance::deps::datafusion::scalar::ScalarValue;
use lance::io::exec::Planner;
use lance_core::datatypes::{Field, Schema};
use lance_core::{Error, Result};
use lance_namespace::models::{
    AlterTableAddColumnsRequest, AlterTableAddColumnsResponse, AlterTableAlterColumnsRequest,
    AlterTableAlterColumnsResponse, AlterTableDropColumnsRequest, AlterTableDropColumnsResponse,
    AlterTransactionRequest, AlterTransactionResponse, AnalyzeTableQueryPlanRequest,
    CountTableRowsRequest, CreateEmptyTableRequest, CreateEmptyTableResponse,
    CreateNamespaceRequest, CreateNamespaceResponse, CreateTableIndexRequest,
    CreateTableIndexResponse, CreateTableRequest, CreateTableResponse,
    CreateTableScalarIndexResponse, CreateTableTagRequest, CreateTableTagResponse,
    DeclareTableRequest, DeclareTableResponse, DeleteFromTableRequest, DeleteFromTableResponse,
    DeleteTableTagRequest, DeleteTableTagResponse, DeregisterTableRequest, DeregisterTableResponse,
    DescribeNamespaceRequest, DescribeNamespaceResponse, DescribeTableIndexStatsRequest,
    DescribeTableIndexStatsResponse, DescribeTableRequest, DescribeTableResponse,
    DescribeTransactionRequest, DescribeTransactionResponse, DropNamespaceRequest,
    DropNamespaceResponse, DropTableIndexRequest, DropTableIndexResponse, DropTableRequest,
    DropTableResponse, ExplainTableQueryPlanRequest, GetTableStatsRequest, GetTableStatsResponse,
    GetTableTagVersionRequest, GetTableTagVersionResponse, InsertIntoTableRequest,
    InsertIntoTableResponse, JsonArrowSchema, ListNamespacesRequest, ListNamespacesResponse,
    ListTableIndicesRequest, ListTableIndicesResponse, ListTableTagsRequest, ListTableTagsResponse,
    ListTableVersionsRequest, ListTableVersionsResponse, ListTablesRequest, ListTablesResponse,
    MergeInsertIntoTableRequest, MergeInsertIntoTableResponse, NamespaceExistsRequest,
    QueryTableRequest, RegisterTableRequest, RegisterTableResponse, RenameTableRequest,
    RenameTableResponse, RestoreTableRequest, RestoreTableResponse, TableExistsRequest,
    UpdateTableRequest, UpdateTableResponse, UpdateTableSchemaMetadataRequest,
    UpdateTableSchemaMetadataResponse, UpdateTableTagRequest, UpdateTableTagResponse,
};
use lance_namespace::schema::{arrow_schema_to_json, convert_json_arrow_schema};
use lance_namespace::LanceNamespace;
use lance_namespace_reqwest_client::models::PartitionField as JsonPartitionField;
use lance_namespace_reqwest_client::models::PartitionSpec as JsonPartitionSpec;
use lance_namespace_reqwest_client::models::PartitionTransform as JsonPartitionTransform;
use snafu::location;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// A PartitionedNamespace is a directory namespace containing a collection of tables that share a
/// common schema. These tables are physically separated and independent, but logically related
/// through partition fields definition.
pub struct PartitionedNamespace {
    /// Underlying directory namespace used for physical storage.
    directory: DirectoryNamespace,
    /// Underlying manifest namespace used for metadata and table discovery.
    ///
    /// This is derived from `directory.manifest_ns`.
    manifest: Arc<ManifestNamespace>,
    /// Root location URI of this partitioned namespace.
    location: String,
    /// Shared logical schema enforced across all partition tables.
    schema: Schema,
}

impl Debug for PartitionedNamespace {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PartitionedNamespace({})", self.namespace_id())
    }
}

impl PartitionedNamespace {
    /// Partition pruning for the given filter expression.
    ///
    /// # Arguments
    ///
    /// * `filter` - The filter expression to be applied.
    ///
    /// Returns the list of (partition table, refine expr) that are required to scan.
    pub async fn plan_scan(
        &self,
        filter: &Expr,
    ) -> lance_core::Result<Vec<(PartitionTable, Expr)>> {
        // 1) Convert to CDF (OR-of-ANDs), then perform partition prune for each partition field.
        let cdf = expr_to_cdf(filter);
        let mut manifest_pred = lit(true);
        for field in self.all_partition_fields().await? {
            let expr = field.partition_prune(self.schema(), &cdf).await?;
            manifest_pred = manifest_pred.and(expr);
        }

        // 2) Query manifest to get candidate tables.
        let table_filter = col("object_type").eq(lit("table"));
        let objects = self
            .manifest
            .query_manifest(table_filter.and(manifest_pred))
            .await?;

        // 3) Build the scan plan. For now, refine expr is always the original filter.
        let tables = extract_tables(objects)?;
        let mut table_expr_tuples = vec![];
        for t in tables.into_iter() {
            table_expr_tuples.push((t, filter.clone()));
        }
        Ok(table_expr_tuples)
    }

    /// Resolve the target partition table for the input row. Create it (empty table) if not exists.
    ///
    /// # Arguments
    ///
    /// * `record` - The record batch to be resolved, it should contain only one row.
    ///
    /// Returns the partition table that the input row belongs to.
    pub async fn resolve_or_create_partition_table(
        &self,
        record: &RecordBatch,
    ) -> Result<PartitionTable> {
        let spec = self.current_partition_spec().await?;
        let partition_values = partition_values(&spec.fields, record).await?;

        if let Some(table) = self
            .resolve_partition_table(&spec, &partition_values)
            .await?
        {
            Ok(table)
        } else {
            self.create_partition_table(&spec, &partition_values).await
        }
    }

    async fn create_partition_table(
        &self,
        spec: &PartitionSpec,
        partition_values: &[ScalarValue],
    ) -> Result<PartitionTable> {
        self.ensure_namespace_exists(vec![spec.spec_id_str()])
            .await?;

        // Create partition namespace for each level.
        // Format: <spec_id_str>$<id1>$...$<idn>
        let mut namespace_path: Vec<String> = vec![spec.spec_id_str()];
        for _ in 0..partition_values.len() {
            namespace_path.push(random_partition_namespace_id());
        }

        // Create namespace rows
        if !partition_values.is_empty() {
            let mut ns_reqs: Vec<CreateNamespaceRequest> =
                Vec::with_capacity(partition_values.len());
            let mut ns_records: Vec<Option<RecordBatch>> =
                Vec::with_capacity(partition_values.len());

            for level in 0..partition_values.len() {
                let id: Vec<String> = namespace_path[..(level + 2)].to_vec();
                ns_reqs.push(CreateNamespaceRequest {
                    id: Some(id),
                    ..Default::default()
                });

                let batch =
                    build_partition_extended_record(&spec.fields, partition_values, Some(level))?;
                ns_records.push(Some(batch));
            }

            let on_columns: Vec<String> = spec
                .fields
                .iter()
                .map(|f| format!("partition_field_{}", f.field_id))
                .collect();
            let create_req = CreateMultiNamespacesRequestBuilder::new()
                .namespaces(ns_reqs)
                .on(on_columns)
                .build();
            self.manifest
                .create_multi_namespaces_extended(create_req, ns_records)
                .await?;
        }

        // Declare the leaf table
        let mut table_id: Vec<String> = vec![];
        let partition_expr = partition_expressions(&spec.fields, partition_values)?;
        let mut ns_expr = col("object_type").eq(lit("namespace"));
        for expr in partition_expr {
            ns_expr = ns_expr.and(expr);
        }
        let objects = self.manifest.query_manifest(ns_expr).await?;
        for object in objects.into_iter() {
            if let ManifestObject::Namespace(ns) = object {
                if ns.namespace.len() == 0 {
                    continue;
                }
                if ns.namespace.get(0).unwrap() != &format!("v{}", spec.id) {
                    continue
                }
                table_id.extend(ns.namespace);
                table_id.push(ns.name);
                break;
            } else {
                continue;
            }
        }
        if table_id.is_empty() {
            return Err(Error::Internal {
                message: "Couldn't find partitioned namespace of table".into(),
                location: location!(),
            })
        }
        table_id.push("dataset".to_string());

        let table_record = build_partition_extended_record(&spec.fields, partition_values, None)?;

        let declare_req = DeclareTableRequest {
            id: Some(table_id.clone()),
            ..Default::default()
        };

        // Handle concurrent creation: if table already exists, resolve and return it.
        if self
            .manifest
            .declare_table_extended(declare_req, Some(table_record))
            .await
            .is_err()
        {
            if let Some(table) = self.resolve_partition_table(spec, partition_values).await? {
                return Ok(table);
            }
            return Err(Error::Internal {
                message: "Failed to declare partition table".to_string(),
                location: location!(),
            });
        }

        Ok(PartitionTable {
            id: table_id,
            read_version: None,
        })
    }

    async fn resolve_partition_table(
        &self,
        spec: &PartitionSpec,
        partition_values: &[ScalarValue],
    ) -> Result<Option<PartitionTable>> {
        let partition_expr = partition_expressions(&spec.fields, partition_values)?;
        let mut table_expr = col("object_type").eq(lit("table"));
        for expr in partition_expr {
            table_expr = table_expr.and(expr);
        }

        let objects = self.manifest.query_manifest(table_expr).await?;
        let tables = extract_tables(objects)?;
        for table in tables.into_iter() {
            if table.id.first() == Some(&spec.spec_id_str()) {
                return Ok(Some(table));
            }
        }
        Ok(None)
    }

    /// Commit the partition table changes.
    ///
    /// If ACID is disabled, commit does nothing.
    /// Otherwise, if the partition namespace is changed after read version, this method will
    /// auto-detect the conflicts.
    ///
    /// # Arguments
    ///
    /// * `read_version` - The partition tables that are read in the transaction.
    /// * `new_version` - The partition tables that are written in the transaction.
    ///
    /// Returns the new version of the partitioned namespace.
    pub fn commit(
        &self,
        read_version: Option<Vec<PartitionTable>>,
        new_version: Option<Vec<PartitionTable>>,
    ) -> lance_core::Result<Option<Vec<PartitionTable>>> {
        let _ = (read_version, new_version);
        Err(Error::Internal {
            message: "PartitionedNamespace.commit is not implemented".to_string(),
            location: location!(),
        })
    }

    /// Schema of the partitioned namespace.
    pub fn schema(&self) -> Schema {
        self.schema.clone()
    }

    /// All partition tables of the partitioned namespace.
    pub async fn tables(&self) -> Result<Vec<PartitionTable>> {
        let objects = self
            .manifest
            .query_manifest(col("object_type").eq(lit("table")))
            .await?;
        extract_tables(objects)
    }

    /// Partitioning of the partitioned namespace.
    pub async fn partitioning(&self) -> Result<Partitioning> {
        let metadata = self.manifest.get_metadata().await?;
        let mut partitioning: Vec<PartitionSpec> = vec![];
        for (k, v) in metadata.iter() {
            if k.starts_with("partition_spec_v") {
                let json_partition_spec: JsonPartitionSpec = serde_json::from_str(v.as_str())
                    .map_err(|e| Error::Internal {
                        message: format!("Failed to parse schema from _manifest metadata: {}", e),
                        location: location!(),
                    })?;

                let partition_spec = PartitionSpec::from_json(&json_partition_spec)?;
                let expected_key = partition_spec_key(&partition_spec);
                if k != &expected_key {
                    return Err(Error::Internal {
                        message: format!(
                            "Inconsistent __manifest metadata key: expected '{}' but got '{}'",
                            expected_key, k
                        ),
                        location: location!(),
                    });
                }
                partitioning.push(partition_spec);
            }
        }
        Ok(Partitioning::new(partitioning))
    }

    // Partition Evolution.

    /// Update the partition spec.
    ///
    /// # Arguments
    ///
    /// * `partition_spec` - The new partition spec.
    ///
    /// Returns the new partition spec.
    pub async fn update_partition_spec(
        &self,
        partition_spec: Vec<PartitionField>,
    ) -> lance_core::Result<PartitionSpec> {
        // Sanity check
        let mut all_sigs = HashSet::new();
        let mut all_field_ids = HashSet::new();
        for f in &partition_spec {
            if f.source_ids.is_empty() {
                return Err(Error::InvalidInput {
                    source: "partition field source_ids must not be empty".into(),
                    location: location!(),
                });
            }
            let has_transform = f.transform.is_some();
            let has_expression = f
                .expression
                .as_ref()
                .map(|e| !e.trim().is_empty())
                .unwrap_or(false);
            if has_transform == has_expression {
                return Err(Error::InvalidInput {
                    source: "Exactly one of transform or expression must be set".into(),
                    location: location!(),
                });
            }
            if !all_sigs.insert(f.signature()) || !all_field_ids.insert(f.field_id.clone()) {
                return Err(Error::InvalidInput {
                    source: "Partition fields signature and field_id should be unique.".into(),
                    location: location!(),
                });
            }
        }

        // Build the new spec fields, reusing existing field_id where possible.
        let partitioning = self.partitioning().await?;
        let new_spec_id =
            partitioning
                .current()
                .map(|s| s.id + 1)
                .ok_or_else(|| Error::Internal {
                    message: "Partition spec doesn't exist".to_string(),
                    location: location!(),
                })?;

        let mut new_fields: Vec<PartitionField> = Vec::with_capacity(partition_spec.len());
        for mut f in partition_spec.into_iter() {
            if let Some(existing_id) = partitioning.get_field_id(&f) {
                // Reuse field_id for the same signature.
                f.field_id = existing_id.clone();
            } else if let Some(existing_sig) = partitioning.get_signature(&f) {
                // Field IDs must never be reused for a different meaning.
                if existing_sig != &f.signature() {
                    return Err(Error::InvalidInput {
                        source: format!(
                            "Partition field_id '{}' is already used by another field; cannot reuse it",
                            f.field_id
                        )
                        .into(),
                        location: location!(),
                    });
                }
            }
            new_fields.push(f);
        }

        let new_spec = PartitionSpec {
            id: new_spec_id,
            fields: new_fields,
        };
        self.force_sink_partition_spec(&new_spec).await?;

        Ok(new_spec)
    }

    pub(crate) async fn force_sink_partition_spec(
        &self,
        new_spec: &PartitionSpec,
    ) -> lance_core::Result<()> {
        self.ensure_namespace_exists(vec![new_spec.spec_id_str()])
            .await?;
        self.ensure_partition_fields_exists(&new_spec.fields)
            .await?;

        // Persist the new spec in __manifest table metadata.
        let json = serde_json::to_string(&new_spec.to_json()).map_err(|e| Error::Internal {
            message: format!("Failed to serialize partition spec: {}", e),
            location: location!(),
        })?;
        let key = partition_spec_key(new_spec);
        self.manifest
            .update_metadata([(key.as_str(), json.as_str())])
            .await?;

        Ok(())
    }

    // Schema Evolution.

    /// Add a new column to the partitioned namespace.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to be added.
    ///
    /// Returns the new schema.
    pub fn add_column(&self, _column: &Field) -> lance_core::Result<Schema> {
        Err(Error::Internal {
            message: "PartitionedNamespace.add_column is not implemented".to_string(),
            location: location!(),
        })
    }

    /// Drop the given column from the partitioned namespace.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to be dropped.
    ///
    /// Returns the new schema.
    pub fn drop_column(&self, _column: &str) -> lance_core::Result<Schema> {
        Err(Error::Internal {
            message: "PartitionedNamespace.drop_column is not implemented".to_string(),
            location: location!(),
        })
    }

    /// Rename the given column in the partitioned namespace.
    ///
    /// # Arguments
    ///
    /// * `old_name` - The old name of the column.
    /// * `new_name` - The new name of the column.
    ///
    /// Returns the new schema.
    pub fn rename_column(&self, _old_name: &str, _new_name: &str) -> lance_core::Result<Schema> {
        Err(Error::Internal {
            message: "PartitionedNamespace.rename_column is not implemented".to_string(),
            location: location!(),
        })
    }

    /// Promote the type of the given column to the new type in the partitioned namespace.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to be promoted.
    /// * `new_type` - The new type of the column.
    ///
    /// Returns the new schema.
    pub fn type_promotion(
        &self,
        _column: &str,
        _new_type: &DataType,
    ) -> lance_core::Result<Schema> {
        Err(Error::Internal {
            message: "PartitionedNamespace.type_promotion is not implemented".to_string(),
            location: location!(),
        })
    }
}

/// Convert boolean expression into a conservative CDF (OR-of-ANDs) form.
///
/// The returned structure is `Vec<Vec<Expr>>`, where outer `Vec` is OR, and
/// inner `Vec` is AND of atomic predicates.
fn expr_to_cdf(expr: &Expr) -> Vec<Vec<Expr>> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            let left = expr_to_cdf(&binary.left);
            let right = expr_to_cdf(&binary.right);
            let mut out = Vec::new();
            for l in left {
                for r in &right {
                    let mut clause = Vec::with_capacity(l.len() + r.len());
                    clause.extend(l.iter().cloned());
                    clause.extend(r.iter().cloned());
                    out.push(clause);
                }
            }
            out
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            let mut left = expr_to_cdf(&binary.left);
            let mut right = expr_to_cdf(&binary.right);
            left.append(&mut right);
            left
        }
        _ => vec![vec![expr.clone()]],
    }
}

fn extract_tables(objects: Vec<ManifestObject>) -> Result<Vec<PartitionTable>> {
    let mut tables: Vec<PartitionTable> = Vec::new();
    for obj in objects {
        let ManifestObject::Table(t) = obj else {
            continue;
        };
        // Only consider partitioned namespace leaf tables.
        if t.name != "dataset" {
            continue;
        }
        if t.namespace.is_empty() {
            continue;
        }
        if !t.namespace[0].starts_with('v') {
            continue;
        }

        let mut id = t.namespace;
        id.push(t.name);
        tables.push(PartitionTable {
            id,
            read_version: None,
        });
    }
    Ok(tables)
}

/// Parse a SQL filter expression into a DataFusion [`Expr`].
pub async fn parse_filter_expr_from_sql(filter: &str, arrow_schema: &ArrowSchema) -> Result<Expr> {
    let filter = filter.trim();
    if filter.is_empty() {
        return Ok(lit(true));
    }
    let planner = Planner::new(Arc::new(arrow_schema.clone()));
    planner.parse_filter(filter)
}

fn is_comparison_op(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
    )
}

/// Transform the first row in record into partition values.
async fn partition_values(
    fields: &[PartitionField],
    record: &RecordBatch,
) -> Result<Vec<ScalarValue>> {
    let mut values: Vec<ScalarValue> = Vec::with_capacity(fields.len());
    for field in fields.iter() {
        let partition_value = field.value(record).await?;
        let scalar =
            ScalarValue::try_from_array(&partition_value, 0).map_err(|e| Error::Internal {
                message: format!(
                    "Failed to convert partition value for field '{}' to scalar: {}",
                    field.field_id, e
                ),
                location: location!(),
            })?;
        values.push(scalar);
    }
    Ok(values)
}

/// Transform partition values into manifest filter expressions.
///
/// Partition values are stored in `__manifest` as extended columns named
/// `partition_field_{field_id}`.
fn partition_expressions(fields: &[PartitionField], values: &[ScalarValue]) -> Result<Vec<Expr>> {
    if fields.len() != values.len() {
        return Err(Error::InvalidInput {
            source: format!(
                "fields len {} must be equal to values len {}",
                fields.len(),
                values.len()
            )
            .into(),
            location: location!(),
        });
    }

    let mut expr_vec: Vec<Expr> = Vec::with_capacity(fields.len());
    for (field, value) in fields.iter().zip(values.iter()) {
        let col_name = format!("partition_field_{}", field.field_id);
        let expr = col(col_name).eq(lit(value.clone()));
        expr_vec.push(expr);
    }
    Ok(expr_vec)
}

/// The key of partition spec saved in metadata.
fn partition_spec_key(spec: &PartitionSpec) -> String {
    format!("partition_spec_v{}", spec.id)
}

/// Build extended record for partition fields. If `partition_values_effective_idx` is some,
/// partition values greater than it would be NULL.
fn build_partition_extended_record(
    fields: &[PartitionField],
    partition_values: &[ScalarValue],
    partition_values_effective_idx: Option<usize>,
) -> Result<RecordBatch> {
    let mut arrow_fields: Vec<ArrowField> = Vec::with_capacity(fields.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(fields.len());

    for (idx, f) in fields.iter().enumerate() {
        let col_name = format!("partition_field_{}", f.field_id);
        arrow_fields.push(ArrowField::new(&col_name, f.result_type.clone(), true));

        let scalar = match partition_values_effective_idx {
            Some(max) if idx > max => {
                ScalarValue::try_from(&f.result_type).map_err(|e| Error::Internal {
                    message: format!(
                        "Failed to create null scalar for partition field '{}': {}",
                        f.field_id, e
                    ),
                    location: location!(),
                })?
            }
            _ => partition_values
                .get(idx)
                .cloned()
                .ok_or_else(|| Error::InvalidInput {
                    source: format!(
                        "partition_values length {} is smaller than required index {}",
                        partition_values.len(),
                        idx
                    )
                    .into(),
                    location: location!(),
                })?,
        };

        let arr = scalar.to_array().map_err(|e| Error::Internal {
            message: format!(
                "Failed to convert scalar for manifest column '{}' to array: {}",
                col_name, e
            ),
            location: location!(),
        })?;
        arrays.push(arr);
    }

    let schema = Arc::new(ArrowSchema::new(arrow_fields));
    RecordBatch::try_new(schema, arrays).map_err(|e| Error::Internal {
        message: format!("Failed to create extended record batch: {}", e),
        location: location!(),
    })
}

// Manifest dataset related methods
impl PartitionedNamespace {
    async fn current_partition_spec(&self) -> Result<PartitionSpec> {
        let partitioning = self.partitioning().await?;
        let spec = partitioning.current().ok_or_else(|| Error::Internal {
            message: "PartitionSpec not found in manifest".to_string(),
            location: location!(),
        })?;
        Ok(spec.clone())
    }

    /// Get all unique partition fields
    async fn all_partition_fields(&self) -> Result<Vec<PartitionField>> {
        let mut id_set = HashSet::new();
        let mut partition_fields = vec![];
        let partitioning = self.partitioning().await?;
        for spec in partitioning.all() {
            for pf in spec.fields.iter() {
                if id_set.insert(pf.field_id.clone()) {
                    partition_fields.push(pf.clone());
                }
            }
        }
        Ok(partition_fields)
    }

    /// Ensure __manifest has columns for all partition fields.
    async fn ensure_partition_fields_exists(
        &self,
        partition_fields: &[PartitionField],
    ) -> Result<()> {
        let full_schema = self.manifest.full_manifest_schema().await?;
        let existing_fields: HashMap<String, DataType> = full_schema
            .fields()
            .iter()
            .map(|f| (f.name().to_string(), f.data_type().clone()))
            .collect();

        let mut to_add: Vec<(String, DataType)> = Vec::new();
        for f in partition_fields.iter() {
            let col_name = format!("partition_field_{}", f.field_id);
            if let Some(existing_ty) = existing_fields.get(&col_name) {
                if existing_ty != &f.result_type {
                    return Err(Error::InvalidInput {
                        source: format!(
                            "Manifest column '{}' already exists with type {:?}, expected {:?}",
                            col_name, existing_ty, f.result_type
                        )
                        .into(),
                        location: location!(),
                    });
                }
                continue;
            }

            // add_extended_properties requires keys to have EXTENDED_PREFIX and strips it
            // to form the physical column name.
            to_add.push((
                format!("{}{}", EXTENDED_PREFIX, col_name),
                f.result_type.clone(),
            ));
        }
        if !to_add.is_empty() {
            let to_add_param: Vec<(&str, DataType)> = to_add
                .iter()
                .map(|(k, t)| (k.as_str(), t.clone()))
                .collect();
            self.manifest.add_extended_properties(&to_add_param).await?;
        }
        Ok(())
    }

    /// Ensure the spec version namespace exists (vN).
    async fn ensure_namespace_exists(&self, id: Vec<String>) -> Result<()> {
        let exists_req = NamespaceExistsRequest {
            id: Some(id.clone()),
            ..Default::default()
        };

        if self.namespace_exists(exists_req.clone()).await.is_err() {
            let create_req = CreateNamespaceRequest {
                id: Some(id),
                ..Default::default()
            };
            if self.create_namespace(create_req).await.is_err() {
                // Maybe the namespace has been created by competitor, retry namespace exists.
                return self.namespace_exists(exists_req).await;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl LanceNamespace for PartitionedNamespace {
    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> lance_core::Result<ListNamespacesResponse> {
        self.directory.list_namespaces(request).await
    }

    async fn describe_namespace(
        &self,
        request: DescribeNamespaceRequest,
    ) -> lance_core::Result<DescribeNamespaceResponse> {
        self.directory.describe_namespace(request).await
    }

    async fn create_namespace(
        &self,
        request: CreateNamespaceRequest,
    ) -> lance_core::Result<CreateNamespaceResponse> {
        self.directory.create_namespace(request).await
    }

    async fn drop_namespace(
        &self,
        request: DropNamespaceRequest,
    ) -> lance_core::Result<DropNamespaceResponse> {
        self.directory.drop_namespace(request).await
    }

    async fn namespace_exists(&self, request: NamespaceExistsRequest) -> Result<()> {
        self.directory.namespace_exists(request).await
    }

    async fn list_tables(
        &self,
        request: ListTablesRequest,
    ) -> lance_core::Result<ListTablesResponse> {
        self.directory.list_tables(request).await
    }

    async fn describe_table(
        &self,
        request: DescribeTableRequest,
    ) -> lance_core::Result<DescribeTableResponse> {
        // Delegate to DirectoryNamespace to reuse credential vending and any future context behavior.
        self.directory.describe_table(request).await
    }

    async fn register_table(
        &self,
        request: RegisterTableRequest,
    ) -> lance_core::Result<RegisterTableResponse> {
        self.directory.register_table(request).await
    }

    async fn table_exists(&self, request: TableExistsRequest) -> lance_core::Result<()> {
        self.directory.table_exists(request).await
    }

    async fn drop_table(&self, request: DropTableRequest) -> lance_core::Result<DropTableResponse> {
        self.directory.drop_table(request).await
    }

    async fn deregister_table(
        &self,
        request: DeregisterTableRequest,
    ) -> lance_core::Result<DeregisterTableResponse> {
        self.directory.deregister_table(request).await
    }

    async fn count_table_rows(&self, request: CountTableRowsRequest) -> lance_core::Result<i64> {
        self.directory.count_table_rows(request).await
    }

    async fn create_table(
        &self,
        request: CreateTableRequest,
        request_data: Bytes,
    ) -> lance_core::Result<CreateTableResponse> {
        self.directory.create_table(request, request_data).await
    }

    async fn declare_table(
        &self,
        request: DeclareTableRequest,
    ) -> lance_core::Result<DeclareTableResponse> {
        self.directory.declare_table(request).await
    }

    async fn create_empty_table(
        &self,
        request: CreateEmptyTableRequest,
    ) -> lance_core::Result<CreateEmptyTableResponse> {
        #[allow(deprecated)]
        self.directory.create_empty_table(request).await
    }

    async fn insert_into_table(
        &self,
        request: InsertIntoTableRequest,
        request_data: Bytes,
    ) -> lance_core::Result<InsertIntoTableResponse> {
        self.directory
            .insert_into_table(request, request_data)
            .await
    }

    async fn merge_insert_into_table(
        &self,
        request: MergeInsertIntoTableRequest,
        request_data: Bytes,
    ) -> lance_core::Result<MergeInsertIntoTableResponse> {
        self.directory
            .merge_insert_into_table(request, request_data)
            .await
    }

    async fn update_table(
        &self,
        request: UpdateTableRequest,
    ) -> lance_core::Result<UpdateTableResponse> {
        self.directory.update_table(request).await
    }

    async fn delete_from_table(
        &self,
        request: DeleteFromTableRequest,
    ) -> lance_core::Result<DeleteFromTableResponse> {
        self.directory.delete_from_table(request).await
    }

    async fn query_table(&self, request: QueryTableRequest) -> lance_core::Result<Bytes> {
        self.directory.query_table(request).await
    }

    async fn create_table_index(
        &self,
        request: CreateTableIndexRequest,
    ) -> lance_core::Result<CreateTableIndexResponse> {
        self.directory.create_table_index(request).await
    }

    async fn list_table_indices(
        &self,
        request: ListTableIndicesRequest,
    ) -> lance_core::Result<ListTableIndicesResponse> {
        self.directory.list_table_indices(request).await
    }

    async fn describe_table_index_stats(
        &self,
        request: DescribeTableIndexStatsRequest,
    ) -> lance_core::Result<DescribeTableIndexStatsResponse> {
        self.directory.describe_table_index_stats(request).await
    }

    async fn describe_transaction(
        &self,
        request: DescribeTransactionRequest,
    ) -> lance_core::Result<DescribeTransactionResponse> {
        self.directory.describe_transaction(request).await
    }

    async fn alter_transaction(
        &self,
        request: AlterTransactionRequest,
    ) -> lance_core::Result<AlterTransactionResponse> {
        self.directory.alter_transaction(request).await
    }

    async fn create_table_scalar_index(
        &self,
        request: CreateTableIndexRequest,
    ) -> lance_core::Result<CreateTableScalarIndexResponse> {
        self.directory.create_table_scalar_index(request).await
    }

    async fn drop_table_index(
        &self,
        request: DropTableIndexRequest,
    ) -> lance_core::Result<DropTableIndexResponse> {
        self.directory.drop_table_index(request).await
    }

    async fn list_all_tables(
        &self,
        request: ListTablesRequest,
    ) -> lance_core::Result<ListTablesResponse> {
        self.directory.list_all_tables(request).await
    }

    async fn restore_table(
        &self,
        request: RestoreTableRequest,
    ) -> lance_core::Result<RestoreTableResponse> {
        self.directory.restore_table(request).await
    }

    async fn rename_table(
        &self,
        request: RenameTableRequest,
    ) -> lance_core::Result<RenameTableResponse> {
        self.directory.rename_table(request).await
    }

    async fn list_table_versions(
        &self,
        request: ListTableVersionsRequest,
    ) -> lance_core::Result<ListTableVersionsResponse> {
        self.directory.list_table_versions(request).await
    }

    async fn update_table_schema_metadata(
        &self,
        request: UpdateTableSchemaMetadataRequest,
    ) -> lance_core::Result<UpdateTableSchemaMetadataResponse> {
        self.directory.update_table_schema_metadata(request).await
    }

    async fn get_table_stats(
        &self,
        request: GetTableStatsRequest,
    ) -> lance_core::Result<GetTableStatsResponse> {
        self.directory.get_table_stats(request).await
    }

    async fn explain_table_query_plan(
        &self,
        request: ExplainTableQueryPlanRequest,
    ) -> lance_core::Result<String> {
        self.directory.explain_table_query_plan(request).await
    }

    async fn analyze_table_query_plan(
        &self,
        request: AnalyzeTableQueryPlanRequest,
    ) -> lance_core::Result<String> {
        self.directory.analyze_table_query_plan(request).await
    }

    async fn alter_table_add_columns(
        &self,
        request: AlterTableAddColumnsRequest,
    ) -> lance_core::Result<AlterTableAddColumnsResponse> {
        self.directory.alter_table_add_columns(request).await
    }

    async fn alter_table_alter_columns(
        &self,
        request: AlterTableAlterColumnsRequest,
    ) -> lance_core::Result<AlterTableAlterColumnsResponse> {
        self.directory.alter_table_alter_columns(request).await
    }

    async fn alter_table_drop_columns(
        &self,
        request: AlterTableDropColumnsRequest,
    ) -> lance_core::Result<AlterTableDropColumnsResponse> {
        self.directory.alter_table_drop_columns(request).await
    }

    async fn list_table_tags(
        &self,
        request: ListTableTagsRequest,
    ) -> lance_core::Result<ListTableTagsResponse> {
        self.directory.list_table_tags(request).await
    }

    async fn get_table_tag_version(
        &self,
        request: GetTableTagVersionRequest,
    ) -> lance_core::Result<GetTableTagVersionResponse> {
        self.directory.get_table_tag_version(request).await
    }

    async fn create_table_tag(
        &self,
        request: CreateTableTagRequest,
    ) -> lance_core::Result<CreateTableTagResponse> {
        self.directory.create_table_tag(request).await
    }

    async fn delete_table_tag(
        &self,
        request: DeleteTableTagRequest,
    ) -> lance_core::Result<DeleteTableTagResponse> {
        self.directory.delete_table_tag(request).await
    }

    async fn update_table_tag(
        &self,
        request: UpdateTableTagRequest,
    ) -> lance_core::Result<UpdateTableTagResponse> {
        self.directory.update_table_tag(request).await
    }

    fn namespace_id(&self) -> String {
        format!("partitioned(root={})", self.location)
    }
}

/// Builder for creating or loading a [`PartitionedNamespace`].
///
/// - If `__manifest` already contains `schema` and `partition_spec_v*` metadata, then
///   [`build`](PartitionedNamespaceBuilder::build) loads the existing namespace.
/// - Otherwise, it creates a new namespace using the provided schema and initial partition spec.
#[derive(Debug, Default)]
pub struct PartitionedNamespaceBuilder {
    location: String,
    schema: Option<Schema>,
    partition_spec: Option<PartitionSpec>,
    directory: Option<DirectoryNamespace>,
    credential_vendor_properties: HashMap<String, String>,
    context_provider: Option<Arc<dyn DynamicContextProvider>>,
}

impl PartitionedNamespaceBuilder {
    pub fn new(location: impl Into<String>) -> Self {
        Self {
            location: location.into().trim_end_matches('/').to_string(),
            schema: None,
            partition_spec: None,
            directory: None,
            credential_vendor_properties: HashMap::new(),
            context_provider: None,
        }
    }

    /// Use an already constructed [`DirectoryNamespace`] when building or loading.
    pub fn directory(mut self, directory: DirectoryNamespace) -> Self {
        self.directory = Some(directory);
        self
    }

    /// Add a credential vendor property.
    pub fn credential_vendor_property(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.credential_vendor_properties
            .insert(key.into(), value.into());
        self
    }

    /// Add multiple credential vendor properties.
    pub fn credential_vendor_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.credential_vendor_properties.extend(properties);
        self
    }

    /// Set a dynamic context provider for per-request context.
    pub fn context_provider(mut self, provider: Arc<dyn DynamicContextProvider>) -> Self {
        self.context_provider = Some(provider);
        self
    }

    pub fn schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn partition_spec(mut self, partition_spec: PartitionSpec) -> Self {
        self.partition_spec = Some(partition_spec);
        self
    }

    /// Build with upsert semantics: load if initialized, otherwise create.
    pub async fn build(self) -> Result<PartitionedNamespace> {
        let (directory, manifest_ns) = Self::open_directory(
            &self.location,
            self.directory,
            &self.credential_vendor_properties,
            self.context_provider,
        )
        .await?;
        let metadata = manifest_ns.get_metadata().await?;

        let has_schema = metadata.contains_key("schema");
        let has_spec = metadata.keys().any(|k| k.starts_with("partition_spec_v"));

        match (has_schema, has_spec) {
            (true, true) => {
                let loaded =
                    Self::load_from_manifest(&self.location, directory, manifest_ns).await?;
                Ok(loaded)
            }
            (false, false) => {
                let schema = self.schema.ok_or_else(|| Error::InvalidInput {
                    source: "schema is required when creating a new partitioned namespace".into(),
                    location: location!(),
                })?;
                let partition = self.partition_spec.ok_or_else(|| Error::InvalidInput {
                    source: "partition_spec is required when creating a new partitioned namespace"
                        .into(),
                    location: location!(),
                })?;

                Self::create_new(&self.location, directory, manifest_ns, schema, partition).await
            }
            _ => Err(Error::Internal {
                message: "Inconsistent __manifest metadata: schema and partition_spec_v* must either both exist or both be absent".to_string(),
                location: location!(),
            }),
        }
    }

    /// Load an existing [`PartitionedNamespace`].
    ///
    /// Returns an error if the namespace has not been initialized yet.
    pub async fn load(self) -> Result<PartitionedNamespace> {
        let (directory, manifest_ns) = Self::open_directory(
            &self.location,
            self.directory,
            &self.credential_vendor_properties,
            self.context_provider,
        )
        .await?;
        let metadata = manifest_ns.get_metadata().await?;

        let has_schema = metadata.contains_key("schema");
        let has_spec = metadata.keys().any(|k| k.starts_with("partition_spec_v"));

        if !has_schema || !has_spec {
            return Err(Error::InvalidInput {
                source: "PartitionedNamespace is not initialized".into(),
                location: location!(),
            });
        }

        Self::load_from_manifest(&self.location, directory, manifest_ns).await
    }

    async fn open_directory(
        location: &str,
        directory: Option<DirectoryNamespace>,
        credential_vendor_properties: &HashMap<String, String>,
        context_provider: Option<Arc<dyn DynamicContextProvider>>,
    ) -> Result<(DirectoryNamespace, Arc<ManifestNamespace>)> {
        if directory.is_some()
            && (!credential_vendor_properties.is_empty() || context_provider.is_some())
        {
            return Err(Error::InvalidInput {
                source: "Cannot set credential_vendor/context_provider when directory is explicitly provided".into(),
                location: location!(),
            });
        }

        let directory = match directory {
            Some(d) => d,
            None => {
                let mut builder = crate::DirectoryNamespaceBuilder::new(location)
                    .manifest_enabled(true)
                    .dir_listing_enabled(false)
                    .inline_optimization_enabled(true);

                for (k, v) in credential_vendor_properties.iter() {
                    builder = builder.credential_vendor_property(k.clone(), v.clone());
                }
                if let Some(provider) = context_provider {
                    builder = builder.context_provider(provider);
                }

                builder.build().await?
            }
        };
        let manifest_ns = directory.manifest_namespace()?;
        Ok((directory, manifest_ns))
    }

    async fn load_from_manifest(
        location: &str,
        directory: DirectoryNamespace,
        manifest_ns: Arc<ManifestNamespace>,
    ) -> Result<PartitionedNamespace> {
        let metadata = manifest_ns.get_metadata().await?;
        let json_schema = metadata.get("schema").ok_or_else(|| Error::Internal {
            message: "Schema not found in __manifest metadata".to_string(),
            location: location!(),
        })?;

        let json_schema: JsonArrowSchema =
            serde_json::from_str(json_schema).map_err(|e| Error::Internal {
                message: format!("Failed to parse schema from __manifest metadata: {}", e),
                location: location!(),
            })?;

        let arrow_schema = convert_json_arrow_schema(&json_schema)?;
        let schema = lance_core::datatypes::Schema::try_from(&arrow_schema)?;

        Ok(PartitionedNamespace {
            directory,
            manifest: manifest_ns,
            location: location.to_string(),
            schema,
        })
    }

    async fn create_new(
        location: &str,
        directory: DirectoryNamespace,
        manifest_ns: Arc<ManifestNamespace>,
        schema: Schema,
        partition: PartitionSpec,
    ) -> Result<PartitionedNamespace> {
        if partition.id != 1 {
            return Err(Error::InvalidInput {
                source: "initial partition spec id must be 1".into(),
                location: location!(),
            });
        }

        // Persist schema metadata
        let arrow_schema: ArrowSchema = (&schema).into();
        let json_schema = arrow_schema_to_json(&arrow_schema)?;
        let schema_json = serde_json::to_string(&json_schema).map_err(|e| Error::Internal {
            message: format!("Failed to serialize schema: {}", e),
            location: location!(),
        })?;
        manifest_ns
            .update_metadata([("schema", schema_json.as_str())])
            .await?;

        // Persist initial partition spec
        let spec_json =
            serde_json::to_string(&partition.to_json()).map_err(|e| Error::Internal {
                message: format!("Failed to serialize partition spec: {}", e),
                location: location!(),
            })?;
        let spec_key = partition_spec_key(&partition);
        manifest_ns
            .update_metadata([(spec_key.as_str(), spec_json.as_str())])
            .await?;
        let ns = PartitionedNamespace {
            directory,
            manifest: manifest_ns,
            location: location.to_string(),
            schema,
        };
        ns.force_sink_partition_spec(&partition).await?;

        Ok(ns)
    }
}

/// Partition table of the partitioned namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionTable {
    /// Namespace id path for this partition table, e.g. ["v1", "abc123", "def456"]
    pub id: Vec<String>,
    /// Optional read version used in strong transaction mode
    pub read_version: Option<u64>,
}

/// Partitioning contains all partition specs of the partitioned namespace.
#[derive(Debug, Clone, Default)]
pub struct Partitioning {
    specs: Vec<PartitionSpec>,
    sig_to_field_id: HashMap<String, String>,
    field_id_to_sig: HashMap<String, String>,
}

impl Partitioning {
    /// Create a new Partitioning from a list of specs.
    pub fn new(specs: Vec<PartitionSpec>) -> Self {
        let mut sig_to_field_id: HashMap<String, String> = HashMap::new();
        let mut field_id_to_sig: HashMap<String, String> = HashMap::new();
        for spec in specs.iter() {
            for f in spec.fields.iter() {
                let sig = f.signature();
                sig_to_field_id
                    .entry(sig.clone())
                    .or_insert_with(|| f.field_id.clone());
                field_id_to_sig
                    .entry(f.field_id.clone())
                    .or_insert_with(|| sig);
            }
        }

        Self {
            specs,
            sig_to_field_id,
            field_id_to_sig,
        }
    }

    /// Return the current (highest id) partition spec if any.
    pub fn current(&self) -> Option<&PartitionSpec> {
        self.specs.iter().max_by_key(|s| s.id)
    }

    /// Get a partition spec by id.
    pub fn by_id(&self, id: i32) -> Option<&PartitionSpec> {
        self.specs.iter().find(|s| s.id == id)
    }

    /// All partition specs in this namespace.
    pub fn all(&self) -> &[PartitionSpec] {
        &self.specs
    }

    /// Mutable access to all specs (used for evolution tests).
    pub fn all_mut(&mut self) -> &mut Vec<PartitionSpec> {
        &mut self.specs
    }

    /// Get field id with the same signature if exists
    pub fn get_field_id(&self, field: &PartitionField) -> Option<&String> {
        let signature = field.signature();
        self.sig_to_field_id.get(&signature)
    }

    /// Get signature with the same field id if exists
    pub fn get_signature(&self, field: &PartitionField) -> Option<&String> {
        self.field_id_to_sig.get(&field.field_id)
    }
}

/// Partition specification defines how to derive partition values from a record in a partitioned
/// namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionSpec {
    /// Spec version id, matching the N in `partition_spec_vN`.
    pub id: i32,
    /// Fields in this spec in evaluation order.
    pub fields: Vec<PartitionField>,
}

impl PartitionSpec {
    /// Convert from JSON representation stored in __manifest metadata.
    pub fn from_json(json: &JsonPartitionSpec) -> lance_core::Result<Self> {
        let mut fields = Vec::with_capacity(json.fields.len());
        for f in &json.fields {
            fields.push(PartitionField::from_json(f)?);
        }
        Ok(Self {
            id: json.id,
            fields,
        })
    }

    /// Convert to JSON representation for storing in __manifest metadata.
    pub fn to_json(&self) -> JsonPartitionSpec {
        JsonPartitionSpec {
            id: self.id,
            fields: self.fields.iter().map(PartitionField::to_json).collect(),
        }
    }

    pub fn spec_id_str(&self) -> String {
        format!("v{}", self.id)
    }
}

/// Supported well-known partition transforms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket { num_buckets: i32 },
    MultiBucket { num_buckets: i32 },
    Truncate { width: i32 },
}

fn first_id_name<'a>(schema: &'a SchemaRef, source_ids: &[i32]) -> Result<&'a str> {
    let id = source_ids.first().ok_or_else(|| Error::InvalidInput {
        source: "source_ids should have at least one element".into(),
        location: location!(),
    })?;
    let id = *id as usize;

    let field = schema.fields().get(id).ok_or_else(|| Error::InvalidInput {
        source: format!(
            "source_id {} is out of bounds for schema with {} fields",
            id,
            schema.fields().len()
        )
        .into(),
        location: location!(),
    })?;
    Ok(field.name())
}

/// Partition field definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionField {
    /// Unique identifier for this partition field
    pub field_id: String,
    /// Field IDs of the source columns in the schema
    pub source_ids: Vec<i32>,
    /// Well-known transform to apply. Exactly one of `transform` or `expression`
    /// should be set.
    pub transform: Option<PartitionTransform>,
    /// Custom SQL expression used when `transform` is not set.
    pub expression: Option<String>,
    /// Result type of the partition value
    pub result_type: DataType,
}

impl PartitionField {
    /// Convert this field into its JSON representation.
    pub fn to_json(&self) -> JsonPartitionField {
        let transform = self.transform.as_ref().map(|t| match t {
            PartitionTransform::Identity => Box::new(JsonPartitionTransform {
                r#type: "identity".to_string(),
                num_buckets: None,
                width: None,
            }),
            PartitionTransform::Year => Box::new(JsonPartitionTransform {
                r#type: "year".to_string(),
                num_buckets: None,
                width: None,
            }),
            PartitionTransform::Month => Box::new(JsonPartitionTransform {
                r#type: "month".to_string(),
                num_buckets: None,
                width: None,
            }),
            PartitionTransform::Day => Box::new(JsonPartitionTransform {
                r#type: "day".to_string(),
                num_buckets: None,
                width: None,
            }),
            PartitionTransform::Hour => Box::new(JsonPartitionTransform {
                r#type: "hour".to_string(),
                num_buckets: None,
                width: None,
            }),
            PartitionTransform::Bucket { num_buckets } => Box::new(JsonPartitionTransform {
                r#type: "bucket".to_string(),
                num_buckets: Some(*num_buckets),
                width: None,
            }),
            PartitionTransform::MultiBucket { num_buckets } => Box::new(JsonPartitionTransform {
                r#type: "multi_bucket".to_string(),
                num_buckets: Some(*num_buckets),
                width: None,
            }),
            PartitionTransform::Truncate { width } => Box::new(JsonPartitionTransform {
                r#type: "truncate".to_string(),
                num_buckets: None,
                width: Some(*width),
            }),
        });

        JsonPartitionField {
            field_id: self.field_id.clone(),
            source_ids: self.source_ids.clone(),
            transform,
            expression: self.expression.clone(),
            result_type: Box::new(datatype_to_json_type(&self.result_type)),
        }
    }

    /// Construct a `PartitionField` from its JSON representation.
    pub fn from_json(json: &JsonPartitionField) -> lance_core::Result<Self> {
        let has_transform = json.transform.is_some();
        let has_expression = json
            .expression
            .as_ref()
            .map(|e| !e.trim().is_empty())
            .unwrap_or(false);

        if has_transform == has_expression {
            return Err(lance_core::Error::Namespace {
                source: "Exactly one of transform or expression must be set".into(),
                location: snafu::location!(),
            });
        }

        let transform = json
            .transform
            .as_ref()
            .map(|t| {
                let result = match t.r#type.as_str() {
                    "identity" => PartitionTransform::Identity,
                    "year" => PartitionTransform::Year,
                    "month" => PartitionTransform::Month,
                    "day" => PartitionTransform::Day,
                    "hour" => PartitionTransform::Hour,
                    "bucket" => PartitionTransform::Bucket {
                        num_buckets: t.num_buckets.unwrap_or(0),
                    },
                    "multi_bucket" => PartitionTransform::MultiBucket {
                        num_buckets: t.num_buckets.unwrap_or(0),
                    },
                    "truncate" => PartitionTransform::Truncate {
                        width: t.width.unwrap_or(0),
                    },
                    other => {
                        return Err(lance_core::Error::Namespace {
                            source: format!("Unsupported partition transform: {}", other).into(),
                            location: snafu::location!(),
                        });
                    }
                };
                Ok(result)
            })
            .transpose()?;

        Ok(Self {
            field_id: json.field_id.clone(),
            source_ids: json.source_ids.clone(),
            transform,
            expression: json.expression.clone(),
            result_type: json_type_to_datatype(&json.result_type)?,
        })
    }

    /// Parse partition value from the record. The record should contain exactly one row.
    pub async fn value(&self, record: &RecordBatch) -> Result<Arc<dyn Array>> {
        if record.num_rows() != 1 {
            return Err(Error::InvalidInput {
                source: "record must contain exactly one row".into(),
                location: location!(),
            });
        }

        let array = match (self.expression.as_ref(), self.transform.as_ref()) {
            (Some(expr), None) => parse_partition_value_from_expr(record, expr).await?,
            (None, Some(transform)) => {
                parse_partition_value_from_transform(&self.source_ids, record, transform).await?
            }
            _ => {
                return Err(Error::Internal {
                    message: "expression and transform can't both be set or unset".to_string(),
                    location: location!(),
                })
            }
        };

        if array.is_empty() {
            return Err(Error::Internal {
                message: "partition expression returned empty array".to_string(),
                location: location!(),
            });
        }
        Ok(array)
    }

    /// Signature of this field
    pub fn signature(&self) -> String {
        let mut out = String::new();
        out.push_str("src=");
        out.push_str(&format!("{:?}", self.source_ids));
        out.push(';');
        match (&self.transform, &self.expression) {
            (Some(t), None) => {
                let t = match t {
                    PartitionTransform::Identity => "identity".to_string(),
                    PartitionTransform::Year => "year".to_string(),
                    PartitionTransform::Month => "month".to_string(),
                    PartitionTransform::Day => "day".to_string(),
                    PartitionTransform::Hour => "hour".to_string(),
                    PartitionTransform::Bucket { num_buckets } => {
                        format!("bucket:{}", num_buckets)
                    }
                    PartitionTransform::MultiBucket { num_buckets } => {
                        format!("multi_bucket:{}", num_buckets)
                    }
                    PartitionTransform::Truncate { width } => format!("truncate:{}", width),
                };
                out.push_str(&format!("t={};", t));
            }
            (None, Some(e)) => out.push_str(&format!("e={};", e.trim())),
            _ => out.push_str("invalid;"),
        }

        let rt = datatype_to_json_type(&self.result_type);
        out.push_str(&format!("rt={}", rt.r#type.to_lowercase()));
        out
    }

    /// Transform the input predicate (in CDF format) into a filter of `__manifest`.
    ///
    /// This is a best-effort partition pruning rewrite for a single [`PartitionField`].
    /// It must be conservative: if we cannot safely rewrite a clause, we return a
    /// literal TRUE for that clause (i.e. keep all partitions).
    ///
    /// Per OR-clause (AND of atoms):
    /// - If we can rewrite atoms directly against the manifest partition column
    ///   (identity transform, or well-known `date_part`/`year`/`month`/`day`/`hour`
    ///   predicates for time transforms), we do so.
    /// - Otherwise, if *all* source columns are constrained by equality to literals,
    ///   we synthesize a single-row [`RecordBatch`], reuse [`PartitionField::value`]
    ///   to compute the partition value, then rewrite into
    ///   `partition_field_{field_id} == <computed_value>`.
    pub async fn partition_prune(&self, schema: Schema, cdf: &Vec<Vec<Expr>>) -> Result<Expr> {
        // Resolve source column names from schema and source_ids.
        let mut source_col_names: Vec<String> = Vec::with_capacity(self.source_ids.len());
        for source_id in &self.source_ids {
            let f = schema
                .field_by_id(*source_id)
                .ok_or_else(|| Error::InvalidInput {
                    source: format!("Field id {} not found in schema", source_id).into(),
                    location: location!(),
                })?;
            source_col_names.push(f.name.clone());
        }
        let source_col_set: HashSet<String> = source_col_names.iter().cloned().collect();

        let manifest_col = format!("partition_field_{}", self.field_id);
        let mut manifest_expr = lit(false);

        for clause in cdf {
            // Collect atoms that are relevant to this field (they reference any source column).
            let mut relevant_atoms: Vec<Expr> = Vec::new();
            for atom in clause {
                if expr_references_any_column(atom, &source_col_set) {
                    relevant_atoms.push(atom.clone());
                }
            }

            // If this OR-clause doesn't restrict this partition field at all, it
            // cannot prune partitions.
            if relevant_atoms.is_empty() {
                manifest_expr = manifest_expr.or(lit(true));
                continue;
            }

            // 1) Try direct rewrites (do not require full equality coverage).
            let mut clause_pred = lit(true);
            let mut rewrote_any = false;
            for atom in &relevant_atoms {
                let rewritten = match (&self.transform, &self.expression) {
                    (Some(PartitionTransform::Identity), None) if source_col_names.len() == 1 => {
                        rewrite_identity_atom(atom, &source_col_names[0], &manifest_col)
                    }
                    (Some(PartitionTransform::Year), None) if source_col_names.len() == 1 => {
                        rewrite_time_transform_atom(
                            atom,
                            &source_col_names[0],
                            &manifest_col,
                            "year",
                        )
                    }
                    (Some(PartitionTransform::Month), None) if source_col_names.len() == 1 => {
                        rewrite_time_transform_atom(
                            atom,
                            &source_col_names[0],
                            &manifest_col,
                            "month",
                        )
                    }
                    (Some(PartitionTransform::Day), None) if source_col_names.len() == 1 => {
                        rewrite_time_transform_atom(
                            atom,
                            &source_col_names[0],
                            &manifest_col,
                            "day",
                        )
                    }
                    (Some(PartitionTransform::Hour), None) if source_col_names.len() == 1 => {
                        rewrite_time_transform_atom(
                            atom,
                            &source_col_names[0],
                            &manifest_col,
                            "hour",
                        )
                    }
                    _ => None,
                };

                // If we cannot rewrite this atom, keep it as TRUE (conservative).
                if let Some(expr) = rewritten {
                    rewrote_any = true;
                    clause_pred = clause_pred.and(expr);
                } else {
                    clause_pred = clause_pred.and(lit(true));
                }
            }

            // If we rewrote at least one atom, we can use it for pruning.
            if rewrote_any {
                manifest_expr = manifest_expr.or(clause_pred);
                continue;
            }

            // 2) Try equality-driven value computation.
            //    This requires that every relevant atom is an equality between a source column
            //    and a literal, and that all source columns are covered.
            let mut eq_map: HashMap<String, ScalarValue> = HashMap::new();
            let mut contradictory = false;
            let mut all_relevant_are_eq = true;

            for atom in &relevant_atoms {
                if let Some((col_name, scalar)) = extract_eq_on_source_column(atom, &source_col_set)
                {
                    if let Some(existing) = eq_map.get(&col_name) {
                        if existing != &scalar {
                            contradictory = true;
                            break;
                        }
                    }
                    eq_map.insert(col_name, scalar);
                } else {
                    all_relevant_are_eq = false;
                    break;
                }
            }

            if contradictory {
                // This AND-clause is unsatisfiable; no partitions can match.
                manifest_expr = manifest_expr.or(lit(false));
                continue;
            }

            let covers_all_sources = source_col_names
                .iter()
                .all(|name| eq_map.contains_key(name));

            if all_relevant_are_eq && covers_all_sources {
                if let Some(rb) = build_single_row_record_batch(&schema, &eq_map) {
                    // Compute the partition value using the same logic as table creation.
                    let arr = self.value(&rb).await?;
                    if let Ok(scalar) = ScalarValue::try_from_array(&arr, 0) {
                        let base = col(&manifest_col).eq(lit(scalar));
                        manifest_expr = manifest_expr.or(base.or(col(&manifest_col).is_null()));
                        continue;
                    }
                }
            }

            // 3) Fallback: cannot safely prune this clause.
            manifest_expr = manifest_expr.or(lit(true));
        }

        Ok(manifest_expr)
    }
}

fn expr_references_any_column(expr: &Expr, cols: &HashSet<String>) -> bool {
    match expr {
        Expr::Column(c) => cols.contains(&c.name),
        Expr::BinaryExpr(b) => {
            expr_references_any_column(&b.left, cols) || expr_references_any_column(&b.right, cols)
        }
        Expr::IsNull(e) | Expr::IsNotNull(e) => expr_references_any_column(e, cols),
        Expr::Cast(c) => expr_references_any_column(&c.expr, cols),
        Expr::TryCast(c) => expr_references_any_column(&c.expr, cols),
        Expr::ScalarFunction(fun) => fun.args.iter().any(|a| expr_references_any_column(a, cols)),
        _ => false,
    }
}

fn extract_eq_on_source_column(
    atom: &Expr,
    source_cols: &HashSet<String>,
) -> Option<(String, ScalarValue)> {
    let Expr::BinaryExpr(binary) = atom else {
        return None;
    };
    if binary.op != Operator::Eq {
        return None;
    }
    match (&*binary.left, &*binary.right) {
        (Expr::Column(c), Expr::Literal(v, _)) if source_cols.contains(&c.name) => {
            Some((c.name.clone(), v.clone()))
        }
        (Expr::Literal(v, _), Expr::Column(c)) if source_cols.contains(&c.name) => {
            Some((c.name.clone(), v.clone()))
        }
        _ => None,
    }
}

fn rewrite_identity_atom(atom: &Expr, source_col: &str, manifest_col: &str) -> Option<Expr> {
    match atom {
        Expr::BinaryExpr(binary) if is_comparison_op(binary.op) => {
            match (&*binary.left, &*binary.right) {
                (Expr::Column(c), Expr::Literal(v, _)) if c.name == source_col => {
                    let base =
                        Expr::BinaryExpr(lance::deps::datafusion::logical_expr::BinaryExpr {
                            left: Box::new(col(manifest_col)),
                            op: binary.op,
                            right: Box::new(Expr::Literal(v.clone(), None)),
                        });
                    Some(base.or(col(manifest_col).is_null()))
                }
                (Expr::Literal(v, _), Expr::Column(c)) if c.name == source_col => {
                    let base =
                        Expr::BinaryExpr(lance::deps::datafusion::logical_expr::BinaryExpr {
                            left: Box::new(Expr::Literal(v.clone(), None)),
                            op: binary.op,
                            right: Box::new(col(manifest_col)),
                        });
                    Some(base.or(col(manifest_col).is_null()))
                }
                _ => None,
            }
        }
        Expr::IsNull(e) if matches!(e.as_ref(), Expr::Column(c) if c.name == source_col) => {
            Some(col(manifest_col).is_null())
        }
        Expr::IsNotNull(e) if matches!(e.as_ref(), Expr::Column(c) if c.name == source_col) => {
            Some(col(manifest_col).is_not_null())
        }
        _ => None,
    }
}

fn rewrite_time_transform_atom(
    atom: &Expr,
    source_col: &str,
    manifest_col: &str,
    unit: &str,
) -> Option<Expr> {
    let Expr::BinaryExpr(binary) = atom else {
        return None;
    };
    if !is_comparison_op(binary.op) {
        return None;
    }

    let is_matching_transform_call = |expr: &Expr| -> bool {
        let Expr::ScalarFunction(fun) = expr else {
            return false;
        };

        // Accept either `year(col)` style or `date_part('year', col)` style.
        let is_unit_fn = fun.name() == unit && fun.args.len() == 1;
        let is_date_part = fun.name() == "date_part"
            && fun.args.len() == 2
            && matches!(&fun.args[0], Expr::Literal(v, _) if matches!(v, ScalarValue::Utf8(Some(s)) if s == unit));

        let col_arg = if is_unit_fn {
            fun.args.first()
        } else if is_date_part {
            fun.args.get(1)
        } else {
            None
        };
        let Some(col_arg) = col_arg else {
            return false;
        };
        matches!(col_arg, Expr::Column(c) if c.name == source_col)
    };

    // func(col) <op> literal
    if is_matching_transform_call(&binary.left) {
        let base = Expr::BinaryExpr(lance::deps::datafusion::logical_expr::BinaryExpr {
            left: Box::new(col(manifest_col)),
            op: binary.op,
            right: Box::new(binary.right.as_ref().clone()),
        });
        return Some(base.or(col(manifest_col).is_null()));
    }

    // literal <op> func(col)
    if is_matching_transform_call(&binary.right) {
        let base = Expr::BinaryExpr(lance::deps::datafusion::logical_expr::BinaryExpr {
            left: Box::new(binary.left.as_ref().clone()),
            op: binary.op,
            right: Box::new(col(manifest_col)),
        });
        return Some(base.or(col(manifest_col).is_null()));
    }

    None
}

fn build_single_row_record_batch(
    schema: &Schema,
    eq_map: &HashMap<String, ScalarValue>,
) -> Option<RecordBatch> {
    // Build an Arrow schema from Lance schema.
    let arrow_fields: Vec<ArrowField> = schema.fields.iter().map(ArrowField::from).collect();
    let rb_schema: SchemaRef = Arc::new(ArrowSchema::new(arrow_fields));

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields.len());
    for f in schema.fields.iter() {
        if let Some(scalar) = eq_map.get(&f.name) {
            // Ensure the literal's array type matches the field type; otherwise,
            // give up pruning for this clause.
            let arr = scalar.to_array_of_size(1).ok()?;
            if arr.data_type() != &f.data_type() {
                return None;
            }
            arrays.push(arr);
        } else {
            arrays.push(new_null_array(&f.data_type(), 1));
        }
    }

    RecordBatch::try_new(rb_schema, arrays).ok()
}

/// Evaluate a partition expression using DataFusion and return the resulting
/// Arrow array (single column) for the given record batch.
async fn parse_partition_value_from_expr(
    record: &RecordBatch,
    expr: &str,
) -> Result<Arc<dyn arrow::array::Array>> {
    let ctx = SessionContext::new();
    ctx.register_udf(MURMUR3_MULTI_UDF.clone());
    ctx.register_batch("record_batch", record.clone())?;
    let df = ctx
        .sql(&format!("SELECT {} FROM record_batch", expr))
        .await?;
    let records = df.collect().await?;
    let partition_batch = records.first().ok_or_else(|| Error::Internal {
        message: "expect one row of partition value but got nothing".to_string(),
        location: location!(),
    })?;
    let partition_col = partition_batch.column(0);
    Ok(Arc::clone(partition_col))
}

/// Compute partition values using the transform description by delegating to
/// the expression-based path. The resulting array type matches whatever
/// `parse_partition_value_from_expr` would return for the equivalent SQL
/// expression.
/// TODO: implement parse logic by code instead of datafusion + expr for better performance.
async fn parse_partition_value_from_transform(
    ids: &[i32],
    record: &RecordBatch,
    transform: &PartitionTransform,
) -> Result<Arc<dyn arrow::array::Array>> {
    if record.num_columns() == 0 {
        return Err(Error::InvalidInput {
            source: "record must contain at least one column".into(),
            location: location!(),
        });
    }

    // Map transform to an equivalent SQL expression over the record batch.
    let expr = match transform {
        PartitionTransform::Identity => first_id_name(&record.schema(), ids)?.to_string(),
        PartitionTransform::Year => format!(
            "date_part('year', {})",
            first_id_name(&record.schema(), ids)?
        ),
        PartitionTransform::Month => format!(
            "date_part('month', {})",
            first_id_name(&record.schema(), ids)?
        ),
        PartitionTransform::Day => format!(
            "date_part('day', {})",
            first_id_name(&record.schema(), ids)?
        ),
        PartitionTransform::Hour => format!(
            "date_part('hour', {})",
            first_id_name(&record.schema(), ids)?
        ),
        PartitionTransform::Bucket { num_buckets } => {
            if *num_buckets <= 0 {
                return Err(Error::InvalidInput {
                    source: format!("num_buckets must be positive, got {}", num_buckets).into(),
                    location: location!(),
                });
            }
            format!(
                "abs(murmur3_multi({})) % {}",
                first_id_name(&record.schema(), ids)?,
                num_buckets
            )
        }
        PartitionTransform::MultiBucket { num_buckets } => {
            if *num_buckets <= 0 {
                return Err(Error::InvalidInput {
                    source: format!("num_buckets must be positive, got {}", num_buckets).into(),
                    location: location!(),
                });
            }
            let cols: Vec<String> = ids
                .iter()
                .map(|id| record.schema().field(*id as usize).name().to_string())
                .collect();
            if cols.is_empty() {
                return Err(Error::InvalidInput {
                    source: "source_ids should have at least one element".into(),
                    location: location!(),
                });
            }
            format!("abs(murmur3_multi({})) % {}", cols.join(", "), num_buckets)
        }
        PartitionTransform::Truncate { width } => {
            if *width <= 0 {
                return Err(Error::InvalidInput {
                    source: format!("truncate width must be positive, got {}", width).into(),
                    location: location!(),
                });
            }

            let schema = record.schema();
            let id = *ids.first().ok_or_else(|| Error::InvalidInput {
                source: "source_ids should have at least one element".into(),
                location: location!(),
            })? as usize;
            let field = schema.fields().get(id).ok_or_else(|| Error::InvalidInput {
                source: format!(
                    "source_id {} is out of bounds for record schema with {} fields",
                    id,
                    schema.fields().len()
                )
                .into(),
                location: location!(),
            })?;

            match field.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 => {
                    format!("substring({}, 1, {})", field.name(), width)
                }
                _ => format!("{} - ({} % {})", field.name(), field.name(), width),
            }
        }
    };

    parse_partition_value_from_expr(record, &expr).await
}

pub(crate) fn scalar_to_bytes(array: &dyn arrow::array::Array, row: usize) -> Result<Vec<u8>> {
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

fn datatype_to_json_type(dt: &DataType) -> lance_namespace::models::JsonArrowDataType {
    use lance_namespace::models::JsonArrowDataType;

    let type_name = match dt {
        DataType::Boolean => "bool",
        DataType::Int8 => "int8",
        DataType::Int16 => "int16",
        DataType::Int32 => "int32",
        DataType::Int64 => "int64",
        DataType::UInt8 => "uint8",
        DataType::UInt16 => "uint16",
        DataType::UInt32 => "uint32",
        DataType::UInt64 => "uint64",
        DataType::Float16 => "float16",
        DataType::Float32 => "float32",
        DataType::Float64 => "float64",
        DataType::Date32 => "date32",
        DataType::Date64 => "date64",
        DataType::Utf8 => "utf8",
        DataType::Binary => "binary",
        other => {
            // Fallback to debug string for unsupported types. This keeps the
            // conversion infallible while still surfacing the type.
            return JsonArrowDataType::new(format!("{:?}", other));
        }
    };

    JsonArrowDataType::new(type_name.to_string())
}

fn json_type_to_datatype(
    json: &lance_namespace::models::JsonArrowDataType,
) -> lance_core::Result<DataType> {
    let type_name = json.r#type.to_lowercase();
    let dt = match type_name.as_str() {
        "bool" | "boolean" => DataType::Boolean,
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float16" => DataType::Float16,
        "float32" => DataType::Float32,
        "float64" => DataType::Float64,
        "date32" => DataType::Date32,
        "date64" => DataType::Date64,
        "utf8" => DataType::Utf8,
        "binary" => DataType::Binary,
        other => {
            return Err(lance_core::Error::Namespace {
                source: format!("Unsupported partition field result type: {}", other).into(),
                location: snafu::location!(),
            });
        }
    };

    Ok(dt)
}

/// Generate a random 16-character base36 identifier (a-z0-9).
fn random_partition_namespace_id() -> String {
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut buf = [0u8; 16];
    for b in &mut buf {
        let idx = (rand::random::<u64>() as usize) % CHARS.len();
        *b = CHARS[idx];
    }
    // Safety: all bytes are ASCII alphanumerics.
    String::from_utf8_lossy(&buf).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dir::manifest::TableInfo;
    use arrow::array::{BinaryArray, Date32Array, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{Field, Field as ArrowField, Schema, Schema as ArrowSchema};
    use lance_core::utils::tempfile::TempStdDir;
    use lance_namespace::models::JsonArrowDataType;
    use std::collections::{HashMap, HashSet};

    fn const_bool(expr: &Expr) -> Option<bool> {
        match expr {
            Expr::Literal(ScalarValue::Boolean(Some(b)), _) => Some(*b),
            Expr::BinaryExpr(b) if b.op == Operator::And => {
                Some(const_bool(&b.left)? && const_bool(&b.right)?)
            }
            Expr::BinaryExpr(b) if b.op == Operator::Or => {
                Some(const_bool(&b.left)? || const_bool(&b.right)?)
            }
            _ => None,
        }
    }

    fn collect_column_names(expr: &Expr, out: &mut Vec<String>) {
        match expr {
            Expr::Column(c) => out.push(c.name.clone()),
            Expr::BinaryExpr(b) => {
                collect_column_names(&b.left, out);
                collect_column_names(&b.right, out);
            }
            Expr::IsNull(e) | Expr::IsNotNull(e) => collect_column_names(e, out),
            Expr::Cast(c) => collect_column_names(&c.expr, out),
            Expr::TryCast(c) => collect_column_names(&c.expr, out),
            Expr::ScalarFunction(fun) => fun.args.iter().for_each(|a| collect_column_names(a, out)),
            _ => {}
        }
    }

    async fn setup_multi_version_namespace(
        temp_path: &str,
    ) -> (
        PartitionedNamespace,
        PartitionSpec,
        PartitionSpec,
        Vec<(PartitionTable, Vec<ScalarValue>)>,
        Vec<(PartitionTable, Vec<ScalarValue>)>,
    ) {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("ts", DataType::Date32, true),
            ArrowField::new("country", DataType::Utf8, true),
            ArrowField::new("business_unit", DataType::Int32, true),
        ]);
        let schema = lance_core::datatypes::Schema::try_from(&arrow_schema).unwrap();

        let spec_v1 = PartitionSpec {
            id: 1,
            fields: vec![
                PartitionField {
                    field_id: "event_year".to_string(),
                    source_ids: vec![0],
                    transform: Some(PartitionTransform::Year),
                    expression: None,
                    result_type: DataType::Int32,
                },
                PartitionField {
                    field_id: "country".to_string(),
                    source_ids: vec![1],
                    transform: Some(PartitionTransform::Identity),
                    expression: None,
                    result_type: DataType::Utf8,
                },
            ],
        };

        let ns = PartitionedNamespaceBuilder::new(temp_path)
            .schema(schema)
            .partition_spec(spec_v1.clone())
            .build()
            .await
            .unwrap();

        // v1 tables
        let v1_vals_1 = vec![
            ScalarValue::Int32(Some(2020)),
            ScalarValue::Utf8(Some("US".to_string())),
        ];
        let v1_t1 = ns
            .create_partition_table(&spec_v1, &v1_vals_1)
            .await
            .unwrap();

        let v1_vals_2 = vec![
            ScalarValue::Int32(Some(2021)),
            ScalarValue::Utf8(Some("CN".to_string())),
        ];
        let v1_t2 = ns
            .create_partition_table(&spec_v1, &v1_vals_2)
            .await
            .unwrap();

        // evolve to v2
        let spec_v2 = ns
            .update_partition_spec(vec![
                PartitionField {
                    field_id: "business_unit".to_string(),
                    source_ids: vec![2],
                    transform: Some(PartitionTransform::Identity),
                    expression: None,
                    result_type: DataType::Int32,
                },
                PartitionField {
                    field_id: "country".to_string(),
                    source_ids: vec![1],
                    transform: Some(PartitionTransform::Identity),
                    expression: None,
                    result_type: DataType::Utf8,
                },
            ])
            .await
            .unwrap();

        // v2 tables
        let v2_vals_1 = vec![
            ScalarValue::Int32(Some(1)),
            ScalarValue::Utf8(Some("US".to_string())),
        ];
        let v2_t1 = ns
            .create_partition_table(&spec_v2, &v2_vals_1)
            .await
            .unwrap();

        let v2_vals_2 = vec![
            ScalarValue::Int32(Some(2)),
            ScalarValue::Utf8(Some("FR".to_string())),
        ];
        let v2_t2 = ns
            .create_partition_table(&spec_v2, &v2_vals_2)
            .await
            .unwrap();

        (
            ns,
            spec_v1,
            spec_v2,
            vec![(v1_t1, v1_vals_1), (v1_t2, v1_vals_2)],
            vec![(v2_t1, v2_vals_1), (v2_t2, v2_vals_2)],
        )
    }

    #[test]
    fn partition_field_json_transform() {
        let field = PartitionField {
            field_id: "event_year".to_string(),
            source_ids: vec![1],
            transform: Some(PartitionTransform::Year),
            expression: None,
            result_type: DataType::Int32,
        };

        let json = field.to_json();
        assert_eq!(json.field_id, "event_year");
        assert!(json.expression.is_none());
        let transform = json.transform.as_ref().expect("transform should be set");
        assert_eq!(transform.r#type, "year");

        let other_field = PartitionField::from_json(&json).expect("from_json should succeed");
        assert_eq!(other_field.field_id, "event_year");
        assert_eq!(other_field.source_ids, vec![1]);
        assert_eq!(other_field.transform, Some(PartitionTransform::Year));
        assert_eq!(other_field.expression, None);
        assert_eq!(other_field.result_type, DataType::Int32);
    }

    #[tokio::test]
    async fn test_partition_prune_rewrites_to_manifest_column() {
        // Case 1: identity transform (direct rewrite)
        {
            let arrow_schema =
                ArrowSchema::new(vec![ArrowField::new("country", DataType::Utf8, true)]);
            let schema = lance_core::datatypes::Schema::try_from(&arrow_schema).unwrap();

            let field = PartitionField {
                field_id: "country".to_string(),
                source_ids: vec![0],
                transform: Some(PartitionTransform::Identity),
                expression: None,
                result_type: DataType::Utf8,
            };

            let filter = col("country").eq(lit("US"));
            let cdf = expr_to_cdf(&filter);
            let manifest_filter = field.partition_prune(schema, &cdf).await.unwrap();

            let mut cols = Vec::new();
            collect_column_names(&manifest_filter, &mut cols);
            assert!(cols.contains(&"partition_field_country".to_string()));
            assert!(!cols.contains(&"country".to_string()));
        }

        // Case 2: time transform (direct rewrite via SQL-parsed scalar functions)
        {
            let arrow_schema =
                ArrowSchema::new(vec![ArrowField::new("ts", DataType::Date32, true)]);
            let schema = lance_core::datatypes::Schema::try_from(&arrow_schema).unwrap();
            let field = PartitionField {
                field_id: "event_year".to_string(),
                source_ids: vec![0],
                transform: Some(PartitionTransform::Year),
                expression: None,
                result_type: DataType::Int32,
            };

            // date_part('year', ts) = 2020
            let filter =
                super::parse_filter_expr_from_sql("date_part('year', ts) = 2020", &arrow_schema)
                    .await
                    .unwrap();
            let cdf = expr_to_cdf(&filter);
            let manifest_filter = field.partition_prune(schema, &cdf).await.unwrap();

            let mut cols = Vec::new();
            collect_column_names(&manifest_filter, &mut cols);
            assert!(cols.contains(&"partition_field_event_year".to_string()));
            assert!(!cols.contains(&"ts".to_string()));
        }
    }

    #[tokio::test]
    async fn test_partition_prune_bucket_computes_partition_value() {
        // Case 1: single source_id bucket transform, can prune by computing partition value.
        {
            let arrow_schema =
                ArrowSchema::new(vec![ArrowField::new("country", DataType::Utf8, true)]);
            let schema = lance_core::datatypes::Schema::try_from(&arrow_schema).unwrap();

            let field = PartitionField {
                field_id: "country_bucket".to_string(),
                source_ids: vec![0],
                transform: Some(PartitionTransform::Bucket { num_buckets: 16 }),
                expression: None,
                result_type: DataType::Int64,
            };

            let filter = col("country").eq(lit("US"));
            let cdf = expr_to_cdf(&filter);
            let manifest_filter = field.partition_prune(schema, &cdf).await.unwrap();

            let mut cols = Vec::new();
            collect_column_names(&manifest_filter, &mut cols);
            assert!(cols.contains(&"partition_field_country_bucket".to_string()));
            assert!(!cols.contains(&"country".to_string()));
        }

        // Case 2: multi source_ids, can prune only when all sources are constrained by equality.
        {
            let arrow_schema = ArrowSchema::new(vec![
                ArrowField::new("country", DataType::Utf8, true),
                ArrowField::new("business_unit", DataType::Int32, true),
            ]);
            let schema = lance_core::datatypes::Schema::try_from(&arrow_schema).unwrap();

            let field = PartitionField {
                field_id: "mb".to_string(),
                source_ids: vec![0, 1],
                transform: Some(PartitionTransform::MultiBucket { num_buckets: 16 }),
                expression: None,
                result_type: DataType::Int64,
            };

            // 2.1 can prune
            let filter = col("country")
                .eq(lit("US"))
                .and(col("business_unit").eq(lit(1i32)));
            let cdf = expr_to_cdf(&filter);
            let manifest_filter = field.partition_prune(schema.clone(), &cdf).await.unwrap();

            assert_ne!(const_bool(&manifest_filter), Some(true));
            let mut cols = Vec::new();
            collect_column_names(&manifest_filter, &mut cols);
            assert!(cols.contains(&"partition_field_mb".to_string()));
            assert!(!cols.contains(&"country".to_string()));
            assert!(!cols.contains(&"business_unit".to_string()));

            // 2.2 cannot prune: missing equality on one source
            let filter = col("country").eq(lit("US"));
            let cdf = expr_to_cdf(&filter);
            let manifest_filter = field.partition_prune(schema.clone(), &cdf).await.unwrap();
            assert_eq!(const_bool(&manifest_filter), Some(true));

            // 2.3 cannot prune: non-equality predicate present
            let filter = col("country")
                .eq(lit("US"))
                .and(col("business_unit").gt(lit(1i32)));
            let cdf = expr_to_cdf(&filter);
            let manifest_filter = field.partition_prune(schema.clone(), &cdf).await.unwrap();
            assert_eq!(const_bool(&manifest_filter), Some(true));

            // 2.4 contradiction: no partitions can match
            let filter = col("country")
                .eq(lit("US"))
                .and(col("country").eq(lit("CN")))
                .and(col("business_unit").eq(lit(1i32)));
            let cdf = expr_to_cdf(&filter);
            let manifest_filter = field.partition_prune(schema, &cdf).await.unwrap();
            assert_eq!(const_bool(&manifest_filter), Some(false));
        }
    }

    #[test]
    fn partition_field_json_expression() {
        let json = JsonPartitionField {
            field_id: "country".to_string(),
            source_ids: vec![2],
            transform: None,
            expression: Some("col0".to_string()),
            result_type: Box::new(JsonArrowDataType::new("utf8".to_string())),
        };

        let field = PartitionField::from_json(&json).expect("from_json should succeed");
        assert_eq!(field.field_id, "country");
        assert_eq!(field.source_ids, vec![2]);
        assert!(field.transform.is_none());
        assert_eq!(field.expression.as_deref(), Some("col0"));
        assert_eq!(field.result_type, DataType::Utf8);

        let json2 = field.to_json();
        assert!(json2.transform.is_none());
        assert_eq!(json2.expression.as_deref(), Some("col0"));
        assert_eq!(json2.result_type.r#type.to_lowercase(), "utf8");
    }

    #[test]
    fn partition_field_json_requires_exactly_one_of_transform_or_expression() {
        let json = JsonPartitionField {
            field_id: "bad".to_string(),
            source_ids: vec![1],
            transform: Some(Box::new(JsonPartitionTransform {
                r#type: "identity".to_string(),
                num_buckets: None,
                width: None,
            })),
            expression: Some("col0".to_string()),
            result_type: Box::new(JsonArrowDataType::new("int32".to_string())),
        };

        let err = PartitionField::from_json(&json).expect_err("should fail");
        assert!(err
            .to_string()
            .contains("Exactly one of transform or expression"));
    }

    #[test]
    fn partition_spec_json() {
        let field1 = PartitionField {
            field_id: "event_date".to_string(),
            source_ids: vec![1],
            transform: Some(PartitionTransform::Identity),
            expression: None,
            result_type: DataType::Date32,
        };
        let field2 = PartitionField {
            field_id: "country".to_string(),
            source_ids: vec![2],
            transform: None,
            expression: Some("col0".to_string()),
            result_type: DataType::Utf8,
        };

        let spec = PartitionSpec {
            id: 1,
            fields: vec![field1, field2],
        };

        let json_spec = spec.to_json();
        assert_eq!(json_spec.id, 1);
        assert_eq!(json_spec.fields.len(), 2);

        let other_spec = PartitionSpec::from_json(&json_spec).expect("from_json should succeed");
        assert_eq!(other_spec.id, spec.id);
        assert_eq!(other_spec.fields.len(), spec.fields.len());
        for (a, b) in other_spec.fields.iter().zip(spec.fields.iter()) {
            assert_eq!(a.field_id, b.field_id);
            assert_eq!(a.source_ids, b.source_ids);
            assert_eq!(a.transform, b.transform);
            assert_eq!(a.expression, b.expression);
            assert_eq!(a.result_type, b.result_type);
        }
    }

    #[tokio::test]
    async fn test_update_partition_spec_reuse_and_manifest_update() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        // Minimal namespace schema (not used by update_partition_spec today, but required by constructor)
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("c0", DataType::Date32, true),
            ArrowField::new("c1", DataType::Utf8, true),
            ArrowField::new("c2", DataType::Int32, true),
        ]);
        let schema = lance_core::datatypes::Schema::try_from(&arrow_schema).unwrap();

        let initial_spec = PartitionSpec {
            id: 1,
            fields: vec![
                PartitionField {
                    field_id: "event_year".to_string(),
                    source_ids: vec![0],
                    transform: Some(PartitionTransform::Year),
                    expression: None,
                    result_type: DataType::Int32,
                },
                PartitionField {
                    field_id: "country".to_string(),
                    source_ids: vec![1],
                    transform: None,
                    expression: Some("col0".to_string()),
                    result_type: DataType::Utf8,
                },
            ],
        };
        let event_year_id_v1 = initial_spec
            .fields
            .iter()
            .find(|f| f.transform == Some(PartitionTransform::Year))
            .unwrap()
            .field_id
            .clone();

        let ns = PartitionedNamespaceBuilder::new(temp_path)
            .schema(schema)
            .partition_spec(initial_spec)
            .build()
            .await
            .unwrap();

        let spec_v2 = ns
            .update_partition_spec(vec![
                // Same signature as event_year but different incoming id should be overridden
                PartitionField {
                    field_id: "should_be_overridden".to_string(),
                    source_ids: vec![0],
                    transform: Some(PartitionTransform::Year),
                    expression: None,
                    result_type: DataType::Int32,
                },
                // New field keeps requested id
                PartitionField {
                    field_id: "business_unit".to_string(),
                    source_ids: vec![2],
                    transform: Some(PartitionTransform::Identity),
                    expression: None,
                    result_type: DataType::Int32,
                },
                // Existing signature should reuse field_id
                PartitionField {
                    field_id: "ignored".to_string(),
                    source_ids: vec![1],
                    transform: None,
                    expression: Some("col0".to_string()),
                    result_type: DataType::Utf8,
                },
            ])
            .await
            .unwrap();
        assert_eq!(spec_v2.id, 2);
        let event_year_id_v2 = spec_v2
            .fields
            .iter()
            .find(|f| f.transform == Some(PartitionTransform::Year))
            .unwrap()
            .field_id
            .clone();
        assert_eq!(event_year_id_v2, event_year_id_v1);

        // Verify __manifest table metadata has both partition_spec_v1 and partition_spec_v2
        let meta = ns.manifest.get_metadata().await.unwrap();
        assert!(meta.contains_key("partition_spec_v1"));
        assert!(meta.contains_key("partition_spec_v2"));

        // Verify __manifest schema has columns for partition fields
        let manifest_cols: HashSet<String> = ns
            .manifest
            .get_extended_properties_keys()
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert!(manifest_cols.contains(&format!("partition_field_{}", event_year_id_v1)));
        assert!(manifest_cols.contains("partition_field_country"));
        assert!(manifest_cols.contains("partition_field_business_unit"));
    }

    #[tokio::test]
    async fn test_parse_partition_value_transform_vs_expr_identity() {
        let array = Int32Array::from(vec![1]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "col0",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(array)],
        )
        .unwrap();

        let ids = vec![0];
        let transform = PartitionTransform::Identity;
        let expr = "col0";

        let v_expr = parse_partition_value_from_expr(&batch, expr).await.unwrap();
        let v_transform = parse_partition_value_from_transform(&ids, &batch, &transform)
            .await
            .unwrap();

        assert_eq!(v_expr.len(), v_transform.len());
        assert_eq!(v_expr.data_type(), v_transform.data_type());
        assert_eq!(v_expr.as_ref(), v_transform.as_ref());
    }

    #[tokio::test]
    async fn test_parse_partition_value_transform_vs_expr_year_date32() {
        let value: i32 = 19723;
        let array = Date32Array::from(vec![value]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "col0",
                DataType::Date32,
                false,
            )])),
            vec![Arc::new(array)],
        )
        .unwrap();

        let ids = vec![0];
        let transform = PartitionTransform::Year;
        let expr = "date_part('year', col0)";

        let v_expr = parse_partition_value_from_expr(&batch, expr).await.unwrap();
        let v_transform = parse_partition_value_from_transform(&ids, &batch, &transform)
            .await
            .unwrap();

        assert_eq!(v_expr.len(), v_transform.len());
        assert_eq!(v_expr.data_type(), v_transform.data_type());
        assert_eq!(v_expr.as_ref(), v_transform.as_ref());
    }

    #[tokio::test]
    async fn test_parse_partition_value_transform_vs_expr_truncate_utf8() {
        let array = StringArray::from(vec!["abcdef"]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("col0", DataType::Utf8, false)])),
            vec![Arc::new(array)],
        )
        .unwrap();

        let ids = vec![0];
        let width = 3;
        let transform = PartitionTransform::Truncate { width };
        let expr = "substring(col0, 1, 3)";

        let v_expr = parse_partition_value_from_expr(&batch, expr).await.unwrap();
        let v_transform = parse_partition_value_from_transform(&ids, &batch, &transform)
            .await
            .unwrap();

        assert_eq!(v_expr.len(), v_transform.len());
        assert_eq!(v_expr.data_type(), v_transform.data_type());
        assert_eq!(v_expr.as_ref(), v_transform.as_ref());
    }

    #[tokio::test]
    async fn test_parse_partition_value_transform_vs_expr_truncate_int32() {
        let array = Int32Array::from(vec![17]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "col0",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(array)],
        )
        .unwrap();

        let ids = vec![0];
        let width = 5;
        let transform = PartitionTransform::Truncate { width };
        let expr = "col0 - (col0 % 5)";

        let v_expr = parse_partition_value_from_expr(&batch, expr).await.unwrap();
        let v_transform = parse_partition_value_from_transform(&ids, &batch, &transform)
            .await
            .unwrap();

        assert_eq!(v_expr.len(), v_transform.len());
        assert_eq!(v_expr.data_type(), v_transform.data_type());
        assert_eq!(v_expr.as_ref(), v_transform.as_ref());
    }

    #[tokio::test]
    async fn test_parse_partition_value_transform_vs_expr_bucket_binary() {
        let data: Vec<&[u8]> = vec![b"abc".as_ref()];
        let array = BinaryArray::from(data);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "col0",
                DataType::Binary,
                false,
            )])),
            vec![Arc::new(array)],
        )
        .unwrap();

        let ids = vec![0];
        let num_buckets = 8;
        let transform = PartitionTransform::Bucket { num_buckets };
        let expr = format!("abs(murmur3_multi(col0)) % {}", num_buckets);

        let v_expr = parse_partition_value_from_expr(&batch, &expr)
            .await
            .unwrap();
        let v_transform = parse_partition_value_from_transform(&ids, &batch, &transform)
            .await
            .unwrap();

        assert_eq!(v_expr.len(), v_transform.len());
        assert_eq!(v_expr.data_type(), v_transform.data_type());
        assert_eq!(v_expr.as_ref(), v_transform.as_ref());
    }

    #[tokio::test]
    async fn test_parse_partition_value_transform_vs_expr_multi_bucket_utf8() {
        let col0 = StringArray::from(vec!["ab"]);
        let col1 = StringArray::from(vec!["12"]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("col0", DataType::Utf8, false),
                Field::new("col1", DataType::Utf8, false),
            ])),
            vec![Arc::new(col0), Arc::new(col1)],
        )
        .unwrap();

        let ids = vec![0, 1];
        let num_buckets = 16;
        let transform = PartitionTransform::MultiBucket { num_buckets };
        let expr = format!("abs(murmur3_multi(col0, col1)) % {}", num_buckets);

        let v_expr = parse_partition_value_from_expr(&batch, &expr)
            .await
            .unwrap();
        let v_transform = parse_partition_value_from_transform(&ids, &batch, &transform)
            .await
            .unwrap();

        assert_eq!(v_expr.len(), v_transform.len());
        assert_eq!(v_expr.data_type(), v_transform.data_type());
        assert_eq!(v_expr.as_ref(), v_transform.as_ref());
    }

    #[test]
    fn random_partition_namespace_id_is_base36() {
        let id = random_partition_namespace_id();
        assert_eq!(id.len(), 16);
        for ch in id.chars() {
            assert!(
                ch.is_ascii_lowercase() || ch.is_ascii_digit(),
                "unexpected character in id: {}",
                ch
            );
        }
    }

    #[tokio::test]
    async fn test_resolve_partition_table_multiple_partition_spec_versions() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let (ns, spec_v1, spec_v2, v1_tables, v2_tables) =
            setup_multi_version_namespace(temp_path).await;

        for (t, vals) in v1_tables.iter() {
            let resolved = ns
                .resolve_partition_table(&spec_v1, vals)
                .await
                .unwrap()
                .expect("should resolve v1 table");
            assert_eq!(&resolved, t);
            assert_eq!(resolved.id.first().map(|s| s.as_str()), Some("v1"));
        }

        for (t, vals) in v2_tables.iter() {
            let resolved = ns
                .resolve_partition_table(&spec_v2, vals)
                .await
                .unwrap()
                .expect("should resolve v2 table");
            assert_eq!(&resolved, t);
            assert_eq!(resolved.id.first().map(|s| s.as_str()), Some("v2"));
        }

        // missing partition should return None
        let missing = vec![
            ScalarValue::Int32(Some(1999)),
            ScalarValue::Utf8(Some("US".to_string())),
        ];
        let resolved = ns
            .resolve_partition_table(&spec_v1, &missing)
            .await
            .unwrap();
        assert!(resolved.is_none());
    }

    #[tokio::test]
    async fn test_create_partition_table_multiple_versions_manifest_properties() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        // create partition table
        let (ns, _spec_v1, _spec_v2, v1_tables, v2_tables) =
            setup_multi_version_namespace(temp_path).await;

        // verify created partition tables
        let objects = ns
            .manifest
            .query_manifest(col("object_type").eq(lit("table")))
            .await
            .unwrap();

        let mut tables_by_id: HashMap<String, TableInfo> = HashMap::new();
        let mut v1_count = 0usize;
        let mut v2_count = 0usize;
        for obj in objects {
            let ManifestObject::Table(t) = obj else {
                continue;
            };
            if t.name != "dataset" {
                continue;
            }
            if t.namespace.first().map(|s| s.as_str()) == Some("v1") {
                v1_count += 1;
            }
            if t.namespace.first().map(|s| s.as_str()) == Some("v2") {
                v2_count += 1;
            }

            let mut id = t.namespace.clone();
            id.push(t.name.clone());
            tables_by_id.insert(id.join("."), t);
        }

        assert_eq!(v1_count, v1_tables.len());
        assert_eq!(v2_count, v2_tables.len());

        // Check one v1 table has expected extended props and no v2-only field
        let (t_v1, vals_v1) = &v1_tables[0];
        let tbl = tables_by_id
            .get(&t_v1.id.join("."))
            .expect("v1 table should exist in manifest");
        let props = tbl.properties.as_ref().expect("properties should exist");
        let v1_year = crate::dir::manifest::scalar_to_str(&vals_v1[0])
            .unwrap()
            .unwrap();
        assert_eq!(
            props
                .get(&format!("{}partition_field_event_year", EXTENDED_PREFIX))
                .map(|s| s.as_str()),
            Some(v1_year.as_str())
        );
        assert_eq!(
            props
                .get(&format!("{}partition_field_country", EXTENDED_PREFIX))
                .map(|s| s.as_str()),
            Some("US")
        );
        assert!(!props.contains_key(&format!("{}partition_field_business_unit", EXTENDED_PREFIX)));

        // Check one v2 table has expected extended props including v2-only field
        let (t_v2, vals_v2) = &v2_tables[0];
        let tbl = tables_by_id
            .get(&t_v2.id.join("."))
            .expect("v2 table should exist in manifest");
        let props = tbl.properties.as_ref().expect("properties should exist");
        let v2_bu = crate::dir::manifest::scalar_to_str(&vals_v2[0])
            .unwrap()
            .unwrap();
        let v2_co = crate::dir::manifest::scalar_to_str(&vals_v2[1])
            .unwrap()
            .unwrap();
        assert_eq!(
            props
                .get(&format!("{}partition_field_event_year", EXTENDED_PREFIX))
                .map(|s| s.as_str()),
            None
        );
        assert_eq!(
            props
                .get(&format!("{}partition_field_business_unit", EXTENDED_PREFIX))
                .map(|s| s.as_str()),
            Some(v2_bu.as_str())
        );
        assert_eq!(
            props
                .get(&format!("{}partition_field_country", EXTENDED_PREFIX))
                .map(|s| s.as_str()),
            Some(v2_co.as_str())
        );
    }

    #[tokio::test]
    async fn test_plan_scan_multiple_partition_spec_versions() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let (ns, _spec_v1, _spec_v2, v1_tables, v2_tables) =
            setup_multi_version_namespace(temp_path).await;

        let v1_us_2020 = &v1_tables[0].0;
        let v2_us_2020_bu1 = &v2_tables[0].0;
        let v1_cn_2021 = &v1_tables[1].0;
        let v2_fr_2022_bu2 = &v2_tables[1].0;

        // (country = 'US') should match exactly the two US/2020 tables.
        let filter = col("country").eq(lit("US"));
        let planned = ns.plan_scan(&filter).await.unwrap();
        let got: HashSet<String> = planned.into_iter().map(|(t, _)| t.id.join(".")).collect();

        let expected: HashSet<String> = [v1_us_2020.id.join("."), v2_us_2020_bu1.id.join(".")]
            .into_iter()
            .collect();
        assert_eq!(got, expected);

        // business_unit = 1 should include v2 bu=1 table and all v1 tables (NULL => conservative keep), but not v2 bu=2.
        let filter = col("business_unit").eq(lit(1i32));
        let planned = ns.plan_scan(&filter).await.unwrap();
        let got: HashSet<String> = planned.into_iter().map(|(t, _)| t.id.join(".")).collect();
        let expected: HashSet<String> = [
            v2_us_2020_bu1.id.join("."),
            v1_us_2020.id.join("."),
            v1_cn_2021.id.join("."),
        ]
        .into_iter()
        .collect();
        assert_eq!(got, expected);

        // (business_unit = 1) AND (country = 'US') should prune away v1 CN table.
        let filter = col("business_unit")
            .eq(lit(1i32))
            .and(col("country").eq(lit("US")));
        let planned = ns.plan_scan(&filter).await.unwrap();
        let got: HashSet<String> = planned.into_iter().map(|(t, _)| t.id.join(".")).collect();
        let expected: HashSet<String> = [v1_us_2020.id.join("."), v2_us_2020_bu1.id.join(".")]
            .into_iter()
            .collect();
        assert_eq!(got, expected);

        // (year(ts) = 2020) should match v1 2020 and all v2 tables.
        let arrow_schema = ArrowSchema::from(&ns.schema());
        let filter = parse_filter_expr_from_sql("date_part('year', ts)=2020", &arrow_schema)
            .await
            .unwrap();
        let planned = ns.plan_scan(&filter).await.unwrap();
        let got: HashSet<String> = planned.into_iter().map(|(t, _)| t.id.join(".")).collect();

        let expected: HashSet<String> = [
            v1_us_2020.id.join("."),
            v2_fr_2022_bu2.id.join("."),
            v2_us_2020_bu1.id.join("."),
        ]
        .into_iter()
        .collect();
        assert_eq!(got, expected);
    }
}
