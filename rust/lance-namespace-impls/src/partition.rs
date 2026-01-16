// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
// NOTE: Keep this module warning-clean; avoid `#![allow(unused)]`.

#[cfg(test)]
use crate::dir::manifest::DELIMITER;
use crate::dir::manifest::{scalar_to_str, ManifestObject, EXTENDED_PREFIX};
use crate::udf::MURMUR3_MULTI_UDF;
use crate::util::{
    check_table_spec_consistency, ensure_all_schema_field_have_id, expr_to_cdf,
    is_cdf_always_false, is_comparison_op,
};
use crate::{context::DynamicContextProvider, DirectoryNamespace, ManifestNamespace};
use arrow::array::{new_null_array, Array, ArrayRef, RecordBatch};
use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, FieldRef, SchemaRef};
use async_trait::async_trait;
use bytes::Bytes;
use lance::deps::datafusion::logical_expr::{Expr, Operator};
use lance::deps::datafusion::prelude::{col, lit, SessionContext};
use lance::deps::datafusion::scalar::ScalarValue;
use lance::io::exec::Planner;
#[cfg(test)]
use lance::Dataset;
use lance_arrow::SchemaExt;
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
use lance_namespace::schema::{
    arrow_schema_to_json, arrow_type_to_json, convert_json_arrow_schema, convert_json_arrow_type,
};
use lance_namespace::LanceNamespace;
use lance_namespace_reqwest_client::models::PartitionField as JsonPartitionField;
use lance_namespace_reqwest_client::models::PartitionSpec as JsonPartitionSpec;
use lance_namespace_reqwest_client::models::PartitionTransform as JsonPartitionTransform;
use snafu::location;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

// Column name of table spec id.
const SPEC_ID_COL: &str = "spec_id";

// Keys of table spec schema and partition spec, stored as properties on table spec namespaces.
const NS_PROP_SCHEMA: &str = "schema";
const NS_PROP_PARTITION_SPEC: &str = "partition_spec";

/// A PartitionedNamespace is a directory namespace containing collections of tables that share
/// common schemas. These tables are physically separated and independent, but logically related
/// through partition fields definition.
pub struct PartitionedNamespace {
    /// Underlying directory namespace used for physical storage.
    directory: DirectoryNamespace,
    /// Underlying manifest namespace used for metadata and table discovery.
    manifest: Arc<ManifestNamespace>,
    /// Root location URI of this partitioned namespace.
    location: String,
    /// The definition of the table, each object represents one version of table in a sorted order.
    /// The last object is the current table spec.
    table_specs: Vec<TableSpec>,
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
    pub async fn plan_scan(&self, filter: &Expr) -> Result<Vec<(PartitionTable, Expr)>> {
        let cdf = expr_to_cdf(filter);

        let mut planned: Vec<(PartitionTable, Expr)> = Vec::new();
        for table in &self.table_specs {
            let schema = table.schema.clone();

            if is_cdf_always_false(&schema, &cdf) {
                continue;
            }

            // Prune for each partition.
            let mut manifest_pred = lit(true);
            for field in &table.partition_spec.fields {
                let expr = field.partition_prune(schema.clone(), &cdf).await?;
                manifest_pred = manifest_pred.and(expr);
            }

            // Query manifest to get candidate tables for this table spec.
            let table_filter = col("object_type")
                .eq(lit("table"))
                .and(col(partition_field_col(SPEC_ID_COL)).eq(lit(table.id)));
            let objects = self
                .manifest
                .query_manifest(table_filter.and(manifest_pred))
                .await?;

            // Currently, refine expr is always the original filter. We can return different refine
            // expr in future work.
            for t in extract_tables(objects)? {
                planned.push((t, filter.clone()));
            }
        }

        Ok(planned)
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
        let spec = self.partition_spec().await?;
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
        self.ensure_partition_fields_exists(&spec.fields).await?;
        if partition_values.len() != spec.fields.len() {
            return Err(Error::InvalidInput {
                source: format!(
                    "partition_values length {} must match partition fields length {}",
                    partition_values.len(),
                    spec.fields.len()
                )
                .into(),
                location: location!(),
            });
        }

        // Create missing namespaces.
        let mut table_id = self
            .create_missing_partition_namespaces(spec, partition_values)
            .await?;

        // Declare the leaf table.
        table_id.push("dataset".to_string());

        let table_record =
            build_partition_extended_record(spec.id, &spec.fields, partition_values)?;

        // Handle concurrent creation: if table already exists, resolve and return it.
        let create_table_req = CreateTableRequest {
            id: Some(table_id.clone()),
            mode: Some("Create".to_string()),
            ..Default::default()
        };
        let arrow_schema = Arc::new(self.schema()?);
        let mut data = Vec::new();
        let mut writer = StreamWriter::try_new(&mut data, &arrow_schema)?;
        writer.finish()?;
        if self
            .manifest
            .create_table_extended(create_table_req, Bytes::from(data), Some(table_record))
            .await
            .is_err()
        {
            if let Some(table) = self.resolve_partition_table(spec, partition_values).await? {
                return Ok(table);
            }
            return Err(Error::Internal {
                message: "Failed to create partition table".to_string(),
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
        let partition_expr = partition_expressions(spec.id, &spec.fields, partition_values)?;
        let table_expr = col("object_type").eq(lit("table")).and(partition_expr);

        let objects = self.manifest.query_manifest(table_expr).await?;
        let tables = extract_tables(objects)?;
        for table in tables.into_iter() {
            if table.id.first() == Some(&spec.spec_id_str()) {
                return Ok(Some(table));
            }
        }
        Ok(None)
    }

    /// Create all levels of partition namespaces, return the full partition namespace id.
    ///
    /// Create namespace with name generated from partition value if the level is missing.
    async fn create_missing_partition_namespaces(
        &self,
        spec: &PartitionSpec,
        partition_values: &[ScalarValue],
    ) -> Result<Vec<String>> {
        // Always use deterministic namespace ids derived from (spec_id, partition_values prefix).
        // For each level, we compute the deterministic id and ensure it exists.
        let ns_full_id = create_partition_namespace_id(spec, partition_values)?;
        for level in 0..partition_values.len() {
            // expected_id = v{n}.{level_0}.{level_1}...{level_i}
            let expected_id = ns_full_id[0..level + 2].to_vec();
            let request = DescribeNamespaceRequest {
                id: Some(expected_id.clone()),
                ..Default::default()
            };
            if self.describe_namespace(request).await.is_ok() {
                continue;
            }

            // Missing
            let create_req = CreateNamespaceRequest {
                id: Some(expected_id.clone()),
                ..Default::default()
            };
            let batch = build_partition_extended_record(
                spec.id,
                &spec.fields,
                &partition_values[0..level + 1],
            )?;

            if let Err(e) = self
                .manifest
                .create_namespace_extended(create_req, Some(batch))
                .await
            {
                // Concurrency: re-query and reuse if created by competitor.
                let request = DescribeNamespaceRequest {
                    id: Some(expected_id),
                    ..Default::default()
                };
                if self.describe_namespace(request).await.is_err() {
                    return Err(e);
                }
            }
        }

        Ok(ns_full_id)
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
    ) -> Result<Option<Vec<PartitionTable>>> {
        let _ = (read_version, new_version);
        Err(Error::Internal {
            message: "PartitionedNamespace.commit is not implemented".to_string(),
            location: location!(),
        })
    }

    /// Schema of the partitioned namespace.
    pub fn schema(&self) -> Result<ArrowSchema> {
        let tables = self.latest_table_spec()?;
        Ok(tables.schema.clone())
    }

    /// All partition tables of the partitioned namespace.
    pub async fn tables(&self) -> Result<Vec<PartitionTable>> {
        let objects = self
            .manifest
            .query_manifest(col("object_type").eq(lit("table")))
            .await?;
        extract_tables(objects)
    }

    /// Update the schema and partition spec. It will create a new version of table spec for the new
    /// definition.
    ///
    /// This method is used for both schema evolution and partition evolution.
    ///
    /// # Arguments
    ///
    /// * `schema`         - The new schema.
    /// * `partition_spec` - The new partition spec.
    ///
    /// Returns the new table spec.
    pub async fn update_table_spec(
        &mut self,
        schema: ArrowSchema,
        partition_spec: Vec<PartitionField>,
    ) -> Result<TableSpec> {
        let schema = ensure_all_schema_field_have_id(schema)?;
        check_table_spec_consistency(&schema, &partition_spec)?;

        // Build the new spec fields, reusing existing field_id where possible.
        let new_spec_id = self.latest_table_spec()?.id + 1;

        let mut existed_fields = HashMap::new();
        let mut existed_ids = HashMap::new();
        for field in self.all_partition_fields().await? {
            existed_fields.insert(field.signature(), field.clone());
            existed_ids.insert(field.field_id.clone(), field);
        }

        let mut new_fields: Vec<PartitionField> = Vec::with_capacity(partition_spec.len());
        for mut f in partition_spec.into_iter() {
            if let Some(field) = existed_fields.get(&f.signature()) {
                // Reuse existing field_id for the same signature.
                f.field_id = field.field_id.clone();
            } else if let Some(field) = existed_ids.get(&f.field_id) {
                // Field IDs must never be reused for a different definition.
                if field.signature() != f.signature() {
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
        let table = TableSpec::new(new_spec_id, schema, new_spec);
        self.force_sink_table_spec(&table).await?;

        // Keep in-memory specs consistent with persisted state.
        self.table_specs.push(table.clone());

        Ok(table)
    }

    /// Sink table spec to namespace.
    pub(crate) async fn force_sink_table_spec(&self, table: &TableSpec) -> Result<()> {
        let json_schema = arrow_schema_to_json(&table.schema)?;
        let schema_json = serde_json::to_string(&json_schema).map_err(|e| Error::Internal {
            message: format!("Failed to serialize schema: {}", e),
            location: location!(),
        })?;

        let spec_json = serde_json::to_string(&table.partition_spec.to_json()?).map_err(|e| {
            Error::Internal {
                message: format!("Failed to serialize partition spec: {}", e),
                location: location!(),
            }
        })?;

        let mut props = HashMap::new();
        props.insert(NS_PROP_SCHEMA.to_string(), schema_json);
        props.insert(NS_PROP_PARTITION_SPEC.to_string(), spec_json);

        // Sink
        let create_req = CreateNamespaceRequest {
            id: Some(vec![table.spec_id_str()]),
            properties: Some(props),
            ..Default::default()
        };

        if let Err(e) = self.create_namespace(create_req).await {
            // Handle concurrent sinking error.
            match load_table_spec(&self.directory, table.id).await {
                Ok(Some(existed_table)) => {
                    if existed_table.schema != table.schema
                        || existed_table.partition_spec != table.partition_spec
                    {
                        return Err(Error::InvalidInput {
                            source: format!("A table spec with id {} already exists and contains different schema or partition spec", existed_table.id).into(),
                            location: location!(),
                        });
                    }
                }
                _ => return Err(e),
            }
        }

        self.ensure_partition_fields_exists(&table.partition_spec.fields)
            .await?;
        Ok(())
    }

    /// Add a new column to the partitioned namespace.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to be added.
    ///
    /// Returns the new schema.
    pub async fn add_column(&mut self, column: &ArrowField) -> Result<ArrowSchema> {
        let schema = self.schema()?;
        if schema.fields().iter().any(|f| f.name() == column.name()) {
            return Err(Error::InvalidInput {
                source: format!("Column '{}' already exists", column.name()).into(),
                location: location!(),
            });
        }

        let mut fields: Vec<ArrowField> =
            schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        fields.push(column.clone());
        let new_schema = ArrowSchema::new_with_metadata(fields, schema.metadata().clone());

        let partition_spec = self.latest_table_spec()?.partition_spec.fields.clone();
        let new_table = self.update_table_spec(new_schema, partition_spec).await?;
        Ok(new_table.schema)
    }

    /// Drop the given column from the partitioned namespace.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to be dropped.
    ///
    /// Returns the new schema.
    pub async fn drop_column(&mut self, column: &str) -> Result<ArrowSchema> {
        let schema = self.schema()?;
        let mut fields: Vec<ArrowField> =
            schema.fields().iter().map(|f| f.as_ref().clone()).collect();

        // TODO: support remove nested column in the future.
        let Some(idx) = fields.iter().position(|f| f.name() == column) else {
            return Err(Error::InvalidInput {
                source: format!("Column '{}' not found", column).into(),
                location: location!(),
            });
        };

        fields.remove(idx);
        let new_schema = ArrowSchema::new_with_metadata(fields, schema.metadata().clone());

        // Drop column might remove column referenced by the current partition spec.
        // Since update_table_spec will check whether source_ids exist in schema, we don't check it
        // here.
        let partition_spec = self.latest_table_spec()?.partition_spec.fields.clone();

        let new_table = self.update_table_spec(new_schema, partition_spec).await?;
        Ok(new_table.schema)
    }

    /// Rename the given column in the partitioned namespace.
    ///
    /// # Arguments
    ///
    /// * `old_name` - The old name of the column.
    /// * `new_name` - The new name of the column.
    ///
    /// Returns the new schema.
    pub async fn rename_column(&mut self, old_name: &str, new_name: &str) -> Result<ArrowSchema> {
        if old_name == new_name {
            return self.schema();
        }

        let schema = self.schema()?;
        if schema.fields().iter().any(|f| f.name() == new_name) {
            return Err(Error::InvalidInput {
                source: format!("Column '{}' already exists", new_name).into(),
                location: location!(),
            });
        }

        let mut fields: Vec<ArrowField> =
            schema.fields().iter().map(|f| f.as_ref().clone()).collect();

        let Some(field) = fields.iter_mut().find(|f| f.name() == old_name) else {
            return Err(Error::InvalidInput {
                source: format!("Column '{}' not found", old_name).into(),
                location: location!(),
            });
        };

        field.set_name(new_name);

        let new_schema = ArrowSchema::new_with_metadata(fields, schema.metadata().clone());

        let partition_spec = self.latest_table_spec()?.partition_spec.fields.clone();
        let new_table = self.update_table_spec(new_schema, partition_spec).await?;
        Ok(new_table.schema)
    }

    /// Promote the type of the given column to the new type in the partitioned namespace.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to be promoted.
    /// * `new_type` - The new type of the column.
    ///
    /// Returns the new schema.
    pub fn type_promotion(&self, _column: &str, _new_type: &DataType) -> Result<ArrowSchema> {
        Err(Error::Internal {
            message: "PartitionedNamespace.type_promotion is not implemented".to_string(),
            location: location!(),
        })
    }

    /// Get the current partition spec
    pub async fn partition_spec(&self) -> Result<PartitionSpec> {
        Ok(self.latest_table_spec()?.partition_spec.clone())
    }

    /// Get the current/latest table spec
    fn latest_table_spec(&self) -> Result<&TableSpec> {
        let table = self.table_specs.last().ok_or_else(|| Error::Internal {
            message: "Could not find any table spec".to_string(),
            location: location!(),
        })?;
        Ok(table)
    }

    /// Get all unique partition fields
    async fn all_partition_fields(&self) -> Result<Vec<PartitionField>> {
        let mut id_set = HashSet::new();
        let mut partition_fields = vec![];

        for table in self.table_specs.iter() {
            for field in table.partition_spec.fields.iter() {
                if id_set.insert(field.field_id.clone()) {
                    partition_fields.push(field.clone());
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
        collect_non_existed_fields(&existing_fields, &mut to_add, SPEC_ID_COL, &DataType::Int32)?;
        for f in partition_fields.iter() {
            collect_non_existed_fields(&existing_fields, &mut to_add, &f.field_id, &f.result_type)?;
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
}

/// Load table specs from namespaces. Table specs are saved in children-namespaces with name v{i}
/// of the root namespace , where `i` represents the table spec id.
async fn load_table_specs(dir: &DirectoryNamespace) -> Result<Vec<TableSpec>> {
    let list_req = ListNamespacesRequest {
        id: Some(vec![]),
        page_token: None,
        limit: None,
        ..Default::default()
    };
    let list_resp = dir.list_namespaces(list_req).await?;

    let mut versions: Vec<i32> = list_resp
        .namespaces
        .iter()
        .filter_map(|name| parse_spec_namespace_version(name))
        .collect();
    versions.sort();

    let mut table_defs = vec![];
    for v in versions.iter().copied() {
        if let Some(table) = load_table_spec(dir, v).await? {
            table_defs.push(table);
        }
    }

    Ok(table_defs)
}

/// Load specified table specs from namespaces.
async fn load_table_spec(dir: &DirectoryNamespace, v: i32) -> Result<Option<TableSpec>> {
    let describe_req = DescribeNamespaceRequest {
        id: Some(vec![format!("v{}", v)]),
        ..Default::default()
    };
    let desc = dir.describe_namespace(describe_req).await?;
    let props = desc.properties.unwrap_or_default();

    let mut item_spec = None;
    if let Some(spec_json) = props.get(NS_PROP_PARTITION_SPEC) {
        let json_partition_spec: JsonPartitionSpec = serde_json::from_str(spec_json.as_str())
            .map_err(|e| Error::Internal {
                message: format!(
                    "Failed to parse partition spec from v{} namespace properties: {}",
                    v, e
                ),
                location: location!(),
            })?;
        let spec = PartitionSpec::from_json(&json_partition_spec)?;
        item_spec = Some(spec);
    }

    let mut item_schema: Option<ArrowSchema> = None;
    if let Some(schema_json) = props.get(NS_PROP_SCHEMA) {
        let json_schema: JsonArrowSchema =
            serde_json::from_str(schema_json.as_str()).map_err(|e| Error::Internal {
                message: format!(
                    "Failed to parse schema from v{} namespace properties: {}",
                    v, e
                ),
                location: location!(),
            })?;
        let arrow_schema = convert_json_arrow_schema(&json_schema)?;
        item_schema = Some(arrow_schema);
    }

    if let (Some(schema), Some(spec)) = (item_schema, item_spec) {
        Ok(Some(TableSpec::new(v, schema, spec)))
    } else {
        Ok(None)
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
fn partition_expressions(
    id: i32,
    fields: &[PartitionField],
    values: &[ScalarValue],
) -> Result<Expr> {
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

    let mut partition_expr = col(partition_field_col(SPEC_ID_COL)).eq(lit(id));
    for (field, value) in fields.iter().zip(values.iter()) {
        let col_name = partition_field_col(&field.field_id);
        let expr = col(col_name).eq(lit(value.clone()));
        partition_expr = partition_expr.and(expr);
    }
    Ok(partition_expr)
}

fn parse_spec_namespace_version(name: &str) -> Option<i32> {
    let rest = name.strip_prefix('v')?;
    if rest.is_empty() {
        None
    } else {
        rest.parse::<i32>().ok()
    }
}

/// Build extended record for partition fields.
fn build_partition_extended_record(
    spec_version: i32,
    fields: &[PartitionField],
    partition_values: &[ScalarValue],
) -> Result<RecordBatch> {
    let mut arrow_fields: Vec<ArrowField> = Vec::with_capacity(fields.len() + 1);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(fields.len() + 1);

    arrow_fields.push(ArrowField::new(
        partition_field_col(SPEC_ID_COL),
        DataType::Int32,
        false,
    ));
    arrays.push(
        ScalarValue::Int32(Some(spec_version))
            .to_array()
            .map_err(|e| Error::Internal {
                message: format!(
                    "Failed to convert spec_version {} to array: {}",
                    spec_version, e
                ),
                location: location!(),
            })?,
    );

    let max = partition_values.len() - 1;
    for (idx, f) in fields.iter().enumerate() {
        let col_name = partition_field_col(&f.field_id);
        arrow_fields.push(ArrowField::new(&col_name, f.result_type.clone(), true));

        let scalar = if idx > max {
            ScalarValue::try_from(&f.result_type).map_err(|e| Error::Internal {
                message: format!(
                    "Failed to create null scalar for partition field '{}': {}",
                    f.field_id, e
                ),
                location: location!(),
            })?
        } else {
            partition_values
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
                })?
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

#[async_trait]
impl LanceNamespace for PartitionedNamespace {
    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        self.directory.list_namespaces(request).await
    }

    async fn describe_namespace(
        &self,
        request: DescribeNamespaceRequest,
    ) -> Result<DescribeNamespaceResponse> {
        self.directory.describe_namespace(request).await
    }

    async fn create_namespace(
        &self,
        request: CreateNamespaceRequest,
    ) -> Result<CreateNamespaceResponse> {
        self.directory.create_namespace(request).await
    }

    async fn drop_namespace(&self, request: DropNamespaceRequest) -> Result<DropNamespaceResponse> {
        self.directory.drop_namespace(request).await
    }

    async fn namespace_exists(&self, request: NamespaceExistsRequest) -> Result<()> {
        self.directory.namespace_exists(request).await
    }

    async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
        self.directory.list_tables(request).await
    }

    async fn describe_table(&self, request: DescribeTableRequest) -> Result<DescribeTableResponse> {
        self.directory.describe_table(request).await
    }

    async fn register_table(&self, request: RegisterTableRequest) -> Result<RegisterTableResponse> {
        self.directory.register_table(request).await
    }

    async fn table_exists(&self, request: TableExistsRequest) -> Result<()> {
        self.directory.table_exists(request).await
    }

    async fn drop_table(&self, request: DropTableRequest) -> Result<DropTableResponse> {
        self.directory.drop_table(request).await
    }

    async fn deregister_table(
        &self,
        request: DeregisterTableRequest,
    ) -> Result<DeregisterTableResponse> {
        self.directory.deregister_table(request).await
    }

    async fn count_table_rows(&self, request: CountTableRowsRequest) -> Result<i64> {
        self.directory.count_table_rows(request).await
    }

    async fn create_table(
        &self,
        request: CreateTableRequest,
        request_data: Bytes,
    ) -> Result<CreateTableResponse> {
        self.directory.create_table(request, request_data).await
    }

    async fn declare_table(&self, request: DeclareTableRequest) -> Result<DeclareTableResponse> {
        self.directory.declare_table(request).await
    }

    async fn create_empty_table(
        &self,
        request: CreateEmptyTableRequest,
    ) -> Result<CreateEmptyTableResponse> {
        #[allow(deprecated)]
        self.directory.create_empty_table(request).await
    }

    async fn insert_into_table(
        &self,
        request: InsertIntoTableRequest,
        request_data: Bytes,
    ) -> Result<InsertIntoTableResponse> {
        self.directory
            .insert_into_table(request, request_data)
            .await
    }

    async fn merge_insert_into_table(
        &self,
        request: MergeInsertIntoTableRequest,
        request_data: Bytes,
    ) -> Result<MergeInsertIntoTableResponse> {
        self.directory
            .merge_insert_into_table(request, request_data)
            .await
    }

    async fn update_table(&self, request: UpdateTableRequest) -> Result<UpdateTableResponse> {
        self.directory.update_table(request).await
    }

    async fn delete_from_table(
        &self,
        request: DeleteFromTableRequest,
    ) -> Result<DeleteFromTableResponse> {
        self.directory.delete_from_table(request).await
    }

    async fn query_table(&self, request: QueryTableRequest) -> Result<Bytes> {
        self.directory.query_table(request).await
    }

    async fn create_table_index(
        &self,
        request: CreateTableIndexRequest,
    ) -> Result<CreateTableIndexResponse> {
        self.directory.create_table_index(request).await
    }

    async fn list_table_indices(
        &self,
        request: ListTableIndicesRequest,
    ) -> Result<ListTableIndicesResponse> {
        self.directory.list_table_indices(request).await
    }

    async fn describe_table_index_stats(
        &self,
        request: DescribeTableIndexStatsRequest,
    ) -> Result<DescribeTableIndexStatsResponse> {
        self.directory.describe_table_index_stats(request).await
    }

    async fn describe_transaction(
        &self,
        request: DescribeTransactionRequest,
    ) -> Result<DescribeTransactionResponse> {
        self.directory.describe_transaction(request).await
    }

    async fn alter_transaction(
        &self,
        request: AlterTransactionRequest,
    ) -> Result<AlterTransactionResponse> {
        self.directory.alter_transaction(request).await
    }

    async fn create_table_scalar_index(
        &self,
        request: CreateTableIndexRequest,
    ) -> Result<CreateTableScalarIndexResponse> {
        self.directory.create_table_scalar_index(request).await
    }

    async fn drop_table_index(
        &self,
        request: DropTableIndexRequest,
    ) -> Result<DropTableIndexResponse> {
        self.directory.drop_table_index(request).await
    }

    async fn list_all_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
        self.directory.list_all_tables(request).await
    }

    async fn restore_table(&self, request: RestoreTableRequest) -> Result<RestoreTableResponse> {
        self.directory.restore_table(request).await
    }

    async fn rename_table(&self, request: RenameTableRequest) -> Result<RenameTableResponse> {
        self.directory.rename_table(request).await
    }

    async fn list_table_versions(
        &self,
        request: ListTableVersionsRequest,
    ) -> Result<ListTableVersionsResponse> {
        self.directory.list_table_versions(request).await
    }

    async fn update_table_schema_metadata(
        &self,
        request: UpdateTableSchemaMetadataRequest,
    ) -> Result<UpdateTableSchemaMetadataResponse> {
        self.directory.update_table_schema_metadata(request).await
    }

    async fn get_table_stats(
        &self,
        request: GetTableStatsRequest,
    ) -> Result<GetTableStatsResponse> {
        self.directory.get_table_stats(request).await
    }

    async fn explain_table_query_plan(
        &self,
        request: ExplainTableQueryPlanRequest,
    ) -> Result<String> {
        self.directory.explain_table_query_plan(request).await
    }

    async fn analyze_table_query_plan(
        &self,
        request: AnalyzeTableQueryPlanRequest,
    ) -> Result<String> {
        self.directory.analyze_table_query_plan(request).await
    }

    async fn alter_table_add_columns(
        &self,
        request: AlterTableAddColumnsRequest,
    ) -> Result<AlterTableAddColumnsResponse> {
        self.directory.alter_table_add_columns(request).await
    }

    async fn alter_table_alter_columns(
        &self,
        request: AlterTableAlterColumnsRequest,
    ) -> Result<AlterTableAlterColumnsResponse> {
        self.directory.alter_table_alter_columns(request).await
    }

    async fn alter_table_drop_columns(
        &self,
        request: AlterTableDropColumnsRequest,
    ) -> Result<AlterTableDropColumnsResponse> {
        self.directory.alter_table_drop_columns(request).await
    }

    async fn list_table_tags(
        &self,
        request: ListTableTagsRequest,
    ) -> Result<ListTableTagsResponse> {
        self.directory.list_table_tags(request).await
    }

    async fn get_table_tag_version(
        &self,
        request: GetTableTagVersionRequest,
    ) -> Result<GetTableTagVersionResponse> {
        self.directory.get_table_tag_version(request).await
    }

    async fn create_table_tag(
        &self,
        request: CreateTableTagRequest,
    ) -> Result<CreateTableTagResponse> {
        self.directory.create_table_tag(request).await
    }

    async fn delete_table_tag(
        &self,
        request: DeleteTableTagRequest,
    ) -> Result<DeleteTableTagResponse> {
        self.directory.delete_table_tag(request).await
    }

    async fn update_table_tag(
        &self,
        request: UpdateTableTagRequest,
    ) -> Result<UpdateTableTagResponse> {
        self.directory.update_table_tag(request).await
    }

    fn namespace_id(&self) -> String {
        format!("partitioned(root={})", self.location)
    }
}

/// Builder for creating or loading a [`PartitionedNamespace`].
///
/// - If the `DirectoryNamespace` of specified location contains table specs,
///   then [`build`](PartitionedNamespaceBuilder::build) loads the existing partitioned namespace.
/// - Otherwise, it creates a new namespace using the provided schema and initial partition spec.
#[derive(Debug, Default)]
pub struct PartitionedNamespaceBuilder {
    location: String,
    schema: Option<ArrowSchema>,
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

    pub fn schema(mut self, schema: ArrowSchema) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn partition_spec(mut self, partition_spec: PartitionSpec) -> Self {
        self.partition_spec = Some(partition_spec);
        self
    }

    /// Load [`PartitionedNamespace`] if already initialized, otherwise create.
    pub async fn build(self) -> Result<PartitionedNamespace> {
        let (directory, manifest_ns) = Self::open_directory(
            &self.location,
            self.directory,
            &self.credential_vendor_properties,
            self.context_provider,
        )
        .await?;

        // Load initialized partitioned namespace.
        let tables = load_table_specs(&directory).await?;
        if !tables.is_empty() {
            for table in tables.iter() {
                check_table_spec_consistency(&table.schema, &table.partition_spec.fields)?;
            }
            return Ok(PartitionedNamespace {
                directory,
                manifest: manifest_ns,
                location: self.location.to_string(),
                table_specs: tables,
            });
        }

        // Create new.
        let schema = self.schema.ok_or_else(|| Error::InvalidInput {
            source: "schema is required when creating a new partitioned namespace".into(),
            location: location!(),
        })?;
        let partition = self.partition_spec.ok_or_else(|| Error::InvalidInput {
            source: "partition_spec is required when creating a new partitioned namespace".into(),
            location: location!(),
        })?;
        Self::create_new(&self.location, directory, manifest_ns, schema, partition).await
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

        let tables = load_table_specs(&directory).await?;
        if !tables.is_empty() {
            Ok(PartitionedNamespace {
                directory,
                manifest: manifest_ns,
                location: self.location.to_string(),
                table_specs: tables,
            })
        } else {
            Err(Error::InvalidInput {
                source: "PartitionedNamespace is not initialized".into(),
                location: location!(),
            })
        }
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

    async fn create_new(
        location: &str,
        directory: DirectoryNamespace,
        manifest_ns: Arc<ManifestNamespace>,
        schema: ArrowSchema,
        partition: PartitionSpec,
    ) -> Result<PartitionedNamespace> {
        if partition.id != 1 {
            return Err(Error::InvalidInput {
                source: "initial partition spec id must be 1".into(),
                location: location!(),
            });
        }

        let schema = ensure_all_schema_field_have_id(schema)?;
        check_table_spec_consistency(&schema, &partition.fields)?;

        let table = TableSpec {
            id: partition.id,
            schema,
            partition_spec: partition.clone(),
        };

        let ns = PartitionedNamespace {
            directory,
            manifest: manifest_ns,
            location: location.to_string(),
            table_specs: vec![table.clone()],
        };
        ns.force_sink_table_spec(&table).await?;

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

/// `TableSpec` represents a version of the table metadata state.
/// It contains an id, a schema and a partition spec.
#[derive(Debug, Clone)]
pub struct TableSpec {
    id: i32,
    schema: ArrowSchema,
    partition_spec: PartitionSpec,
}

impl TableSpec {
    pub fn new(id: i32, schema: ArrowSchema, partition_spec: PartitionSpec) -> Self {
        assert_eq!(id, partition_spec.id);
        Self {
            id,
            schema,
            partition_spec,
        }
    }

    pub fn spec_id_str(&self) -> String {
        format!("v{}", self.id)
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
    pub fn from_json(json: &JsonPartitionSpec) -> Result<Self> {
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
    pub fn to_json(&self) -> Result<JsonPartitionSpec> {
        let fields = self
            .fields
            .iter()
            .map(PartitionField::to_json)
            .collect::<Result<Vec<_>>>()?;

        Ok(JsonPartitionSpec {
            id: self.id,
            fields,
        })
    }

    pub fn spec_id_str(&self) -> String {
        format!("v{}", self.id)
    }
}

/// Supported well-known partition transforms.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

fn first_id_name(schema: &ArrowSchema, source_ids: &[i32]) -> Result<String> {
    Ok(first_id(schema, source_ids)?.1)
}

fn first_id(schema: &ArrowSchema, source_ids: &[i32]) -> Result<(FieldRef, String)> {
    let id = *source_ids.first().ok_or_else(|| Error::InvalidInput {
        source: "source_ids should have at least one element".into(),
        location: location!(),
    })?;

    let (col, field) = schema
        .path_and_field_by_id(id)?
        .ok_or_else(|| Error::InvalidInput {
            source: format!("Field id {} not found in schema", id).into(),
            location: location!(),
        })?;

    Ok((Arc::new(field), col))
}

/// Partition field definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionField {
    /// Unique identifier for this partition field
    pub field_id: String,
    /// Field ids of the source columns in the schema
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
    pub fn to_json(&self) -> Result<JsonPartitionField> {
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

        Ok(JsonPartitionField {
            field_id: self.field_id.clone(),
            source_ids: self.source_ids.clone(),
            transform,
            expression: self.expression.clone(),
            result_type: Box::new(arrow_type_to_json(&self.result_type)?),
        })
    }

    /// Construct a `PartitionField` from its JSON representation.
    pub fn from_json(json: &JsonPartitionField) -> Result<Self> {
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
            result_type: convert_json_arrow_type(&json.result_type)?,
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
                message: "partition value is empty, the expression might be invalid".to_string(),
                location: location!(),
            });
        }
        Ok(array)
    }

    /// Signature of this field
    pub fn signature(
        &self,
    ) -> (
        Vec<i32>,
        Option<PartitionTransform>,
        Option<String>,
        DataType,
    ) {
        (
            self.source_ids.clone(),
            self.transform.clone(),
            self.expression.clone(),
            self.result_type.clone(),
        )
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
    pub async fn partition_prune(&self, schema: ArrowSchema, cdf: &Vec<Vec<Expr>>) -> Result<Expr> {
        // Resolve source column names from schema and source_ids.
        let mut source_col_names: Vec<String> = Vec::with_capacity(self.source_ids.len());
        for source_id in &self.source_ids {
            let (path, _) =
                schema
                    .path_and_field_by_id(*source_id)?
                    .ok_or_else(|| Error::InvalidInput {
                        source: format!("Field id {} not found in schema", source_id).into(),
                        location: location!(),
                    })?;
            source_col_names.push(path.clone());
        }
        let source_col_set: HashSet<String> = source_col_names.iter().cloned().collect();

        let manifest_col = partition_field_col(&self.field_id);
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
                return Ok(lit(true));
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
            let mut all_relevant_are_eq = true;

            for atom in &relevant_atoms {
                if let Some((col_name, scalar)) = extract_eq_on_source_column(atom, &source_col_set)
                {
                    eq_map.insert(col_name, scalar);
                } else {
                    all_relevant_are_eq = false;
                    break;
                }
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
                        manifest_expr = manifest_expr.or(base);
                        continue;
                    }
                }
            }

            // 3) Can't prune.
            return Ok(lit(true));
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

// Build a single-row RecordBatch for partition computation.
//
// For each top-level field in `schema`:
// - If there is an exact match in `eq_map`, use the scalar value.
// - Otherwise, use a NULL of the field's type.
//
// `eq_map` may contain nested column paths (e.g. "s.city"). In that case, if a top-level
// field is a struct, we will populate its children recursively.
fn build_single_row_record_batch(
    schema: &ArrowSchema,
    eq_map: &HashMap<String, ScalarValue>,
) -> Option<RecordBatch> {
    let rb_schema: SchemaRef = Arc::new(schema.clone());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for f in schema.fields().iter() {
        if let Some(arr) = build_single_row_field_array(f, "", eq_map) {
            arrays.push(arr);
        } else {
            return None;
        }
    }

    RecordBatch::try_new(rb_schema, arrays).ok()
}

fn build_single_row_field_array(
    field: &FieldRef,
    prefix: &str,
    eq_map: &HashMap<String, ScalarValue>,
) -> Option<ArrayRef> {
    let name = field.name();
    let full_name = if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{}.{}", prefix, name)
    };

    if let Some(scalar) = eq_map.get(&full_name) {
        // Ensure the literal's array type matches the field type; otherwise, None.
        let arr = scalar.to_array_of_size(1).ok()?;
        if arr.data_type() != field.data_type() {
            return None;
        }
        return Some(arr);
    }

    match field.data_type() {
        DataType::Struct(fields) => {
            let mut cols: Vec<(FieldRef, ArrayRef)> = Vec::with_capacity(fields.len());
            for child in fields.iter() {
                let child_arr = build_single_row_field_array(child, &full_name, eq_map);
                if let Some(arr) = child_arr {
                    cols.push((child.clone(), arr));
                } else {
                    return None;
                }
            }
            Some(Arc::new(arrow::array::StructArray::from(cols)) as ArrayRef)
        }
        _ => Some(new_null_array(field.data_type(), 1)),
    }
}

/// Evaluate a partition expression using DataFusion and return the resulting
/// Arrow array (single column) for the given record batch.
async fn parse_partition_value_from_expr(
    record: &RecordBatch,
    expr: &str,
) -> Result<Arc<dyn Array>> {
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

/// Compute partition values using the transform description.
/// TODO: implement parse logic by code instead of datafusion + expr for better performance.
async fn parse_partition_value_from_transform(
    ids: &[i32],
    record: &RecordBatch,
    transform: &PartitionTransform,
) -> Result<Arc<dyn Array>> {
    let schema = record.schema();

    // Map transform to an equivalent SQL expression over the record batch.
    let expr = match transform {
        PartitionTransform::Identity => first_id_name(&schema, ids)?,
        PartitionTransform::Year => format!("date_part('year', {})", first_id_name(&schema, ids)?),
        PartitionTransform::Month => {
            format!("date_part('month', {})", first_id_name(&schema, ids)?)
        }
        PartitionTransform::Day => format!("date_part('day', {})", first_id_name(&schema, ids)?),
        PartitionTransform::Hour => format!("date_part('hour', {})", first_id_name(&schema, ids)?),
        PartitionTransform::Bucket { num_buckets } => {
            if *num_buckets <= 0 {
                return Err(Error::InvalidInput {
                    source: format!("num_buckets must be positive, got {}", num_buckets).into(),
                    location: location!(),
                });
            }
            format!(
                "abs(murmur3_multi({})) % {}",
                first_id_name(&schema, ids)?,
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

            // Collect all columns in multi-bucket.
            let cols: Vec<String> = ids
                .iter()
                .map(|id| {
                    schema
                        .path_and_field_by_id(*id)?
                        .map(|(p, _)| p)
                        .ok_or_else(|| Error::InvalidInput {
                            source: format!("Field id {} not found in schema", id).into(),
                            location: location!(),
                        })
                })
                .collect::<Result<Vec<_>>>()?;
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

            let (field, col) = first_id(&schema, ids)?;
            match field.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 => {
                    format!("substring({}, 1, {})", col, width)
                }
                _ => format!("{} - ({} % {})", col, col, width),
            }
        }
    };

    parse_partition_value_from_expr(record, &expr).await
}

/// Generate partition namespace id based on partition values.
///
/// This function transforms partition values into string and concat with delimiter.
///
/// The format is: v{n}${pv_0_str}${pv_1_str}$...${pv_n_str}
fn create_partition_namespace_id(
    spec: &PartitionSpec,
    partition_values: &[ScalarValue],
) -> Result<Vec<String>> {
    let mut buf: Vec<String> = Vec::with_capacity(partition_values.len() + 1);
    buf.push(spec.spec_id_str());

    for (i, v) in partition_values.iter().enumerate() {
        let v_str = scalar_to_str(v)?.ok_or_else(|| Error::InvalidInput {
            source: format!("partition value with index {} should not be null", i).into(),
            location: location!(),
        })?;
        buf.push(v_str);
    }

    Ok(buf)
}

fn collect_non_existed_fields(
    existing_fields: &HashMap<String, DataType>,
    to_add: &mut Vec<(String, DataType)>,
    col: &str,
    data_type: &DataType,
) -> Result<()> {
    if let Some(existing_ty) = existing_fields.get(col) {
        if existing_ty != data_type {
            return Err(Error::InvalidInput {
                source: format!(
                    "Manifest column '{}' already exists with type {:?}, expected {:?}",
                    col,
                    existing_ty,
                    DataType::Int32
                )
                .into(),
                location: location!(),
            });
        }
    } else {
        to_add.push((
            format!("{}{}", EXTENDED_PREFIX, partition_field_col(col)),
            data_type.clone(),
        ));
    }
    Ok(())
}

/// Partition field is saved as extended properties in __manifest table. We add a prefix
/// 'partition_field_' to it to distinguish from other extended properties.
fn partition_field_col(field_id: &str) -> String {
    format!("partition_field_{}", field_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dir::manifest::TableInfo;
    use crate::util::{ensure_all_schema_field_have_id, expr_to_cdf};
    use arrow::array::{
        BinaryArray, Date32Array, Int32Array, RecordBatch, StringArray, StructArray,
    };
    use arrow_schema::{Field, Field as ArrowField, Fields, Schema as ArrowSchema};
    use lance_core::utils::tempfile::TempStdDir;
    use lance_namespace::models::JsonArrowDataType;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use tokio::task::JoinSet;

    impl PartitionedNamespace {
        /// Resolve partition namespace through spec_id and partition values.
        ///
        /// The spec_id and partition values should uniquely point to a single namespace.
        async fn resolve_partition_namespace(
            &self,
            spec: &PartitionSpec,
            partition_values: &[ScalarValue],
        ) -> Result<Option<Vec<String>>> {
            let level = partition_values.len();

            let mut ns_expr = col("object_type").eq(lit("namespace"));
            ns_expr = ns_expr.and(col(partition_field_col(SPEC_ID_COL)).eq(lit(spec.id)));

            let partition_expr =
                partition_expressions(spec.id, &spec.fields[0..level], partition_values)?;
            ns_expr = ns_expr.and(partition_expr);

            for i in level..spec.fields.len() {
                let col_name = partition_field_col(&spec.fields.get(i).unwrap().field_id);
                ns_expr = ns_expr.and(col(&col_name).is_null());
            }

            let mut ns_ids = vec![];
            let objects = self.manifest.query_manifest(ns_expr).await?;
            for object in objects.into_iter() {
                if let ManifestObject::Namespace(ns) = object {
                    if ns.namespace.is_empty() {
                        continue;
                    }
                    if ns.namespace.first().unwrap() != &format!("v{}", spec.id) {
                        continue;
                    }
                    let mut ns_id: Vec<String> = vec![];
                    ns_id.extend(ns.namespace);
                    ns_id.push(ns.name);
                    ns_ids.push(ns_id);
                } else {
                    continue;
                }
            }

            if ns_ids.is_empty() {
                Ok(None)
            } else if ns_ids.len() == 1 {
                Ok(ns_ids.pop())
            } else {
                Err(Error::Internal {
                    message: format!(
                        "Found multiple partition namespace instance with \
                                      the same spec id and partition values {:?}",
                        ns_ids
                    ),
                    location: location!(),
                })
            }
        }
    }

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
        let schema = arrow_schema.clone();

        let spec_v1 = PartitionSpec {
            id: 1,
            fields: vec![make_pf_year("event_year", 0), make_pf_country("country", 1)],
        };

        let mut ns = PartitionedNamespaceBuilder::new(temp_path)
            .schema(schema.clone())
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
            .update_table_spec(
                schema,
                vec![
                    make_pf_business_unit("business_unit", 2),
                    make_pf_country("country", 1),
                ],
            )
            .await
            .unwrap();

        // v2 tables
        let v2_vals_1 = vec![
            ScalarValue::Int32(Some(1)),
            ScalarValue::Utf8(Some("US".to_string())),
        ];
        let v2_t1 = ns
            .create_partition_table(&spec_v2.partition_spec, &v2_vals_1)
            .await
            .unwrap();

        let v2_vals_2 = vec![
            ScalarValue::Int32(Some(2)),
            ScalarValue::Utf8(Some("FR".to_string())),
        ];
        let v2_t2 = ns
            .create_partition_table(&spec_v2.partition_spec, &v2_vals_2)
            .await
            .unwrap();

        (
            ns,
            spec_v1,
            spec_v2.partition_spec,
            vec![(v1_t1, v1_vals_1), (v1_t2, v1_vals_2)],
            vec![(v2_t1, v2_vals_1), (v2_t2, v2_vals_2)],
        )
    }

    #[test]
    fn test_partition_field_json_transform() {
        let field = PartitionField {
            field_id: "event_year".to_string(),
            source_ids: vec![1],
            transform: Some(PartitionTransform::Year),
            expression: None,
            result_type: DataType::Int32,
        };

        let json = field.to_json().unwrap();
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

    #[test]
    fn test_partition_spec_json_transform() {
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

        let json_spec = spec.to_json().unwrap();
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
    async fn test_partition_prune_rewrites_to_manifest_column() {
        // Case 1: identity transform (direct rewrite)
        {
            let arrow_schema =
                ArrowSchema::new(vec![ArrowField::new("country", DataType::Utf8, true)]);
            let schema = ensure_all_schema_field_have_id(arrow_schema).unwrap();

            let field = make_pf_country("country", 0);

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
            let schema = ensure_all_schema_field_have_id(arrow_schema).unwrap();

            let field = make_pf_year("event_year", 0);

            // date_part('year', ts) = 2020
            let filter = parse_filter_expr_from_sql("date_part('year', ts) = 2020", &schema)
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
            let schema = ensure_all_schema_field_have_id(arrow_schema).unwrap();

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
            let schema = ensure_all_schema_field_have_id(arrow_schema).unwrap();

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
        }
    }

    #[test]
    fn test_partition_field_json_expression() {
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

        let json2 = field.to_json().unwrap();
        assert!(json2.transform.is_none());
        assert_eq!(json2.expression.as_deref(), Some("col0"));
        assert_eq!(json2.result_type.r#type.to_lowercase(), "utf8");
    }

    #[test]
    fn test_partition_field_json_requires_exactly_one_of_transform_or_expression() {
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

    #[tokio::test]
    async fn test_update_partition_spec_successfully() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let schema_v1 = ArrowSchema::new(vec![
            ArrowField::new("c0", DataType::Date32, true),
            ArrowField::new("c1", DataType::Utf8, true),
        ]);
        let mut ns = create_ns(temp_path, schema_v1, vec![make_pf_year("event_year", 0)]).await;

        let schema_v2 = ArrowSchema::new(vec![
            ArrowField::new("c0", DataType::Date32, true),
            ArrowField::new("c1", DataType::Utf8, true),
            ArrowField::new("c2", DataType::Int32, true),
        ]);
        let table_v2 = ns
            .update_table_spec(
                schema_v2.clone(),
                vec![
                    make_pf_year("ignored_id", 0),
                    make_pf_business_unit("business_unit", 2),
                ],
            )
            .await
            .unwrap();
        assert_eq!(table_v2.id, 2);
        assert_eq!(
            table_v2.schema,
            ensure_all_schema_field_have_id(schema_v2.clone()).unwrap()
        );
        assert_eq!(table_v2.partition_spec.id, 2);
        assert_eq!(table_v2.partition_spec.fields.len(), 2);

        // Validate persisted state by reloading.
        let reloaded = PartitionedNamespaceBuilder::new(temp_path)
            .load()
            .await
            .unwrap();
        assert_eq!(
            reloaded.schema().unwrap(),
            ensure_all_schema_field_have_id(schema_v2).unwrap()
        );
        assert_eq!(
            reloaded.partition_spec().await.unwrap(),
            table_v2.partition_spec
        );
    }

    #[tokio::test]
    async fn test_schema_evolution_add_drop_rename_column() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let schema = ArrowSchema::new(vec![
            ArrowField::new("ts", DataType::Date32, true),
            ArrowField::new("country", DataType::Utf8, true),
        ]);
        let mut ns = create_ns(
            temp_path,
            schema,
            vec![make_pf_year("event_year", 0), make_pf_country("country", 1)],
        )
        .await;
        assert_eq!(ns.latest_table_spec().unwrap().id, 1);

        let new_col = ArrowField::new("business_unit", DataType::Int32, true);
        let schema_v2 = ns.add_column(&new_col).await.unwrap();
        assert!(schema_v2
            .fields()
            .iter()
            .any(|f| f.name() == "business_unit"));
        assert_eq!(ns.latest_table_spec().unwrap().id, 2);

        let schema_v3 = ns.rename_column("country", "region").await.unwrap();
        assert!(schema_v3.fields().iter().any(|f| f.name() == "region"));
        assert!(!schema_v3.fields().iter().any(|f| f.name() == "country"));
        assert_eq!(ns.latest_table_spec().unwrap().id, 3);

        let schema_v4 = ns.drop_column("business_unit").await.unwrap();
        assert!(!schema_v4
            .fields()
            .iter()
            .any(|f| f.name() == "business_unit"));
        assert_eq!(ns.latest_table_spec().unwrap().id, 4);

        // Cannot drop a column that is referenced by the current partition spec.
        assert!(ns.drop_column("region").await.is_err());
    }

    // Reuse field_id when signature matches.
    #[tokio::test]
    async fn test_update_partition_spec_reuse_field_id() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let schema = ArrowSchema::new(vec![
            ArrowField::new("c0", DataType::Date32, true),
            ArrowField::new("c1", DataType::Utf8, true),
            ArrowField::new("c2", DataType::Int32, true),
        ]);
        let mut ns = create_ns(
            temp_path,
            schema.clone(),
            vec![
                make_pf_year("event_year", 0),
                make_pf_expr_int32("country", 1, "col0"),
            ],
        )
        .await;

        // Update table spec then verify
        let table_v2 = ns
            .update_table_spec(
                schema,
                vec![
                    // Same signature as event_year but incoming id should be overridden.
                    make_pf_year("should_be_overridden", 0),
                    // Same signature as country but incoming id should be overridden.
                    make_pf_expr_int32("should_be_overridden_too", 1, "col0"),
                    // New field keeps requested id.
                    make_pf_business_unit("business_unit", 2),
                ],
            )
            .await
            .unwrap();

        assert_eq!(table_v2.id, 2);

        let year = table_v2
            .partition_spec
            .fields
            .iter()
            .find(|f| f.transform == Some(PartitionTransform::Year))
            .unwrap();
        assert_eq!(year.field_id, "event_year");

        let country = table_v2
            .partition_spec
            .fields
            .iter()
            .find(|f| f.expression.as_deref() == Some("col0"))
            .unwrap();
        assert_eq!(country.field_id, "country");
    }

    #[tokio::test]
    async fn test_update_partition_spec_reuse_field_id_error() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let schema = ArrowSchema::new(vec![
            ArrowField::new("c0", DataType::Date32, true),
            ArrowField::new("c1", DataType::Int32, true),
        ]);
        let mut ns = create_ns(
            temp_path,
            schema.clone(),
            vec![make_pf_year("event_year", 0)],
        )
        .await;

        let err = ns
            .update_table_spec(
                schema,
                vec![
                    // Reuse existing field_id but with a different signature.
                    make_pf_business_unit("event_year", 1),
                ],
            )
            .await
            .unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("already used by another field"));
    }

    fn make_pf_year(field_id: &str, source_id: i32) -> PartitionField {
        PartitionField {
            field_id: field_id.to_string(),
            source_ids: vec![source_id],
            transform: Some(PartitionTransform::Year),
            expression: None,
            result_type: DataType::Int32,
        }
    }

    fn make_pf_country(field_id: &str, source_id: i32) -> PartitionField {
        PartitionField {
            field_id: field_id.to_string(),
            source_ids: vec![source_id],
            transform: Some(PartitionTransform::Identity),
            expression: None,
            result_type: DataType::Utf8,
        }
    }

    fn make_pf_business_unit(field_id: &str, source_id: i32) -> PartitionField {
        PartitionField {
            field_id: field_id.to_string(),
            source_ids: vec![source_id],
            transform: Some(PartitionTransform::Identity),
            expression: None,
            result_type: DataType::Int32,
        }
    }

    fn make_pf_expr_int32(field_id: &str, source_id: i32, expression: &str) -> PartitionField {
        PartitionField {
            field_id: field_id.to_string(),
            source_ids: vec![source_id],
            transform: None,
            expression: Some(expression.to_string()),
            result_type: DataType::Int32,
        }
    }

    async fn create_ns(
        temp_path: &str,
        schema: ArrowSchema,
        fields: Vec<PartitionField>,
    ) -> PartitionedNamespace {
        let initial_spec = PartitionSpec { id: 1, fields };
        PartitionedNamespaceBuilder::new(temp_path)
            .schema(schema)
            .partition_spec(initial_spec)
            .build()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_parse_partition_value_transform_vs_expr_identity() {
        let array = Int32Array::from(vec![1]);
        let schema = Arc::new(
            ensure_all_schema_field_have_id(ArrowSchema::new(vec![Field::new(
                "col0",
                DataType::Int32,
                false,
            )]))
            .unwrap(),
        );
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();

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
    async fn test_parse_partition_value_nested_struct_field_id() {
        // Schema: s: struct<city: utf8>
        // Field ids after `ensure_schema_field_ids`:
        // - s => 0 (top-level)
        // - s.city => 1 (nested)
        let child_field: FieldRef = Arc::new(ArrowField::new("city", DataType::Utf8, false));
        let struct_field = ArrowField::new(
            "s",
            DataType::Struct(Fields::from(vec![child_field.clone()])),
            false,
        );

        let schema = Arc::new(
            ensure_all_schema_field_have_id(ArrowSchema::new(vec![struct_field])).unwrap(),
        );
        // Use child field from schema, it has id.
        let struct_field = schema.field(0);
        let child_field = match struct_field.data_type() {
            DataType::Struct(fields) => fields[0].clone(),
            _ => panic!("expected struct"),
        };
        let city_values = Arc::new(StringArray::from(vec![Some("US")]));
        let struct_array = StructArray::from(vec![(child_field, city_values as ArrayRef)]);

        let batch = RecordBatch::try_new(schema, vec![Arc::new(struct_array)]).unwrap();

        let ids = vec![1];
        let transform = PartitionTransform::Identity;

        let v = parse_partition_value_from_transform(&ids, &batch, &transform)
            .await
            .unwrap();
        let arr = v.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "US");
    }

    #[test]
    fn test_build_single_row_record_batch_with_nested_eq_map() {
        let child_field_city: FieldRef = Arc::new(ArrowField::new("city", DataType::Utf8, true));
        let child_field_zip: FieldRef = Arc::new(ArrowField::new("zip", DataType::Utf8, true));
        let struct_field = ArrowField::new(
            "s",
            DataType::Struct(Fields::from(vec![child_field_city, child_field_zip])),
            true,
        );
        let schema = ArrowSchema::new(vec![struct_field]);

        let mut eq_map = HashMap::new();
        eq_map.insert(
            "s.city".to_string(),
            ScalarValue::Utf8(Some("US".to_string())),
        );

        let batch = build_single_row_record_batch(&schema, &eq_map).unwrap();
        let s_arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let city_arr = s_arr
            .column_by_name("city")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(city_arr.value(0), "US");
        let zip_arr = s_arr
            .column_by_name("zip")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(zip_arr.is_null(0));
    }

    #[tokio::test]
    async fn test_parse_partition_value_transform_vs_expr_year_date32() {
        let value: i32 = 19723;
        let array = Date32Array::from(vec![value]);
        let schema = Arc::new(
            ensure_all_schema_field_have_id(ArrowSchema::new(vec![Field::new(
                "col0",
                DataType::Date32,
                false,
            )]))
            .unwrap(),
        );
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();

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
        let schema = Arc::new(
            ensure_all_schema_field_have_id(ArrowSchema::new(vec![Field::new(
                "col0",
                DataType::Utf8,
                false,
            )]))
            .unwrap(),
        );
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();

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
        let schema = Arc::new(
            ensure_all_schema_field_have_id(ArrowSchema::new(vec![Field::new(
                "col0",
                DataType::Int32,
                false,
            )]))
            .unwrap(),
        );
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();

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
        let schema = Arc::new(
            ensure_all_schema_field_have_id(ArrowSchema::new(vec![Field::new(
                "col0",
                DataType::Binary,
                false,
            )]))
            .unwrap(),
        );
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();

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
        let schema = Arc::new(
            ensure_all_schema_field_have_id(ArrowSchema::new(vec![
                Field::new("col0", DataType::Utf8, false),
                Field::new("col1", DataType::Utf8, false),
            ]))
            .unwrap(),
        );
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col0), Arc::new(col1)]).unwrap();

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
        let v1_year = scalar_to_str(&vals_v1[0]).unwrap().unwrap();
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
    async fn test_plan_scan_on_missing_columns() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        // v1 schema does NOT have column business_unit.
        let schema_v1 = ArrowSchema::new(vec![
            ArrowField::new("ts", DataType::Date32, true),
            ArrowField::new("country", DataType::Utf8, true),
        ]);
        let spec_v1 = PartitionSpec {
            id: 1,
            fields: vec![make_pf_year("event_year", 0), make_pf_country("country", 1)],
        };
        let mut ns = PartitionedNamespaceBuilder::new(temp_path)
            .schema(schema_v1.clone())
            .partition_spec(spec_v1.clone())
            .build()
            .await
            .unwrap();

        // Create v1 tables.
        let v1_vals_1 = vec![
            ScalarValue::Int32(Some(2020)),
            ScalarValue::Utf8(Some("US".to_string())),
        ];
        let v1_t1 = ns
            .create_partition_table(&spec_v1, &v1_vals_1)
            .await
            .unwrap();

        // Evolve schema + partition spec to v2: add business_unit partition field.
        let schema_v2 = ArrowSchema::new(vec![
            ArrowField::new("ts", DataType::Date32, true),
            ArrowField::new("country", DataType::Utf8, true),
            ArrowField::new("business_unit", DataType::Int32, true),
        ]);
        let spec_v2 = ns
            .update_table_spec(schema_v2, spec_v1.fields.clone())
            .await
            .unwrap();

        let v2_vals_1 = vec![
            ScalarValue::Int32(Some(1)),
            ScalarValue::Utf8(Some("US".to_string())),
        ];
        let v2_t1 = ns
            .create_partition_table(&spec_v2.partition_spec, &v2_vals_1)
            .await
            .unwrap();

        // business_unit IS NOT NULL should prune ALL v1 tables.
        let filter = col("business_unit").is_not_null();
        let planned = ns.plan_scan(&filter).await.unwrap();
        let got: HashSet<String> = planned.into_iter().map(|(t, _)| t.id.join(".")).collect();

        let expected: HashSet<String> = [v2_t1.id.join(".")].into_iter().collect();
        assert_eq!(got, expected);

        // Explicitly verify v1 tables are pruned.
        assert!(!got.contains(&v1_t1.id.join(".")));
    }

    #[tokio::test]
    async fn test_plan_scan_multiple_partition_spec_versions() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let (ns, _spec_v1, _spec_v2, v1_tables, v2_tables) =
            setup_multi_version_namespace(temp_path).await;

        let v1_us_2020 = &v1_tables[0].0;
        let v1_cn_2021 = &v1_tables[1].0;
        let v2_us_2020_bu1 = &v2_tables[0].0;
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
        let arrow_schema = ns.schema().unwrap();
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

    #[tokio::test]
    async fn test_create_partition_table() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let ns = create_partitioned_namespace_country_year(temp_path).await;
        let spec = ns.partition_spec().await.unwrap();

        // Pre-create namespaces for US (level0) and US/2020 (level1), but no table.
        let us_2020_values = vec![
            ScalarValue::Utf8(Some("US".to_string())),
            ScalarValue::Int32(Some(2020)),
        ];
        ns.create_missing_partition_namespaces(&spec, &us_2020_values)
            .await
            .unwrap();

        // Before: namespaces exist, but no table.
        let before_us = list_ns_ids_by_country_year(&ns, "US", None).await;
        assert_eq!(before_us.len(), 1);
        assert_eq!(before_us[0].0, vec![spec.spec_id_str()]);
        assert_eq!(before_us[0].1, "US");

        let before_us_2020 = list_ns_ids_by_country_year(&ns, "US", Some(2020)).await;
        assert_eq!(before_us_2020.len(), 1);
        assert_eq!(
            before_us_2020[0].0,
            vec![spec.spec_id_str(), "US".to_string()]
        );
        assert_eq!(before_us_2020[0].1, "2020");

        let tables_before = ns
            .manifest
            .query_manifest(col("object_type").eq(lit("table")))
            .await
            .unwrap();
        assert!(tables_before.is_empty());

        // Create US/2021 table.
        let us_2021_values = vec![
            ScalarValue::Utf8(Some("US".to_string())),
            ScalarValue::Int32(Some(2021)),
        ];
        let resp = ns
            .create_partition_table(&spec, &us_2021_values)
            .await
            .unwrap();
        let location = ns
            .describe_table(DescribeTableRequest {
                id: Some(resp.id),
                ..Default::default()
            })
            .await
            .unwrap()
            .location
            .unwrap();
        let ds = Dataset::open(&location).await.unwrap();
        assert_eq!(ArrowSchema::from(ds.schema()), ns.schema().unwrap());

        // US namespace should be reused: still exactly one, and same object_id.
        let after_us = list_ns_ids_by_country_year(&ns, "US", None).await;
        assert_eq!(after_us.len(), 1);
        assert_eq!(after_us, before_us);

        // US/2020 namespace should remain unchanged.
        let after_us_2020 = list_ns_ids_by_country_year(&ns, "US", Some(2020)).await;
        assert_eq!(after_us_2020.len(), 1);
        assert_eq!(after_us_2020, before_us_2020);

        // Table for US/2021 exists; US/2020 table still does not exist.
        let us_2021_tables = ns
            .manifest
            .query_manifest(
                col("object_type")
                    .eq(lit("table"))
                    .and(col("partition_field_country").eq(lit("US")))
                    .and(col("partition_field_year").eq(lit(2021i32))),
            )
            .await
            .unwrap();
        assert_eq!(us_2021_tables.len(), 1);

        let us_2020_tables = ns
            .manifest
            .query_manifest(
                col("object_type")
                    .eq(lit("table"))
                    .and(col("partition_field_country").eq(lit("US")))
                    .and(col("partition_field_year").eq(lit(2020i32))),
            )
            .await
            .unwrap();
        assert!(us_2020_tables.is_empty());
    }

    #[tokio::test]
    async fn test_create_missing_partition_namespaces() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let ns = create_partitioned_namespace_country_year(temp_path).await;
        let spec = ns.partition_spec().await.unwrap();

        // When creating missing namespaces, it should use deterministic ids derived from
        // (spec_id, partition_values).
        let values = vec![
            ScalarValue::Utf8(Some("US".to_string())),
            ScalarValue::Int32(Some(2020)),
        ];
        let got_1 = ns
            .create_missing_partition_namespaces(&spec, &values)
            .await
            .unwrap();
        let got_2 = ns
            .create_missing_partition_namespaces(&spec, &values)
            .await
            .unwrap();
        assert_eq!(got_1, got_2);

        let expected = vec![spec.spec_id_str(), "US".to_string(), "2020".to_string()];
        assert_eq!(got_1, expected);

        // Deterministic object_id should exist in manifest.
        let object_id = got_1.join(DELIMITER);
        let objs = ns
            .manifest
            .query_manifest(
                col("object_id")
                    .eq(lit(object_id))
                    .and(col("object_type").eq(lit("namespace"))),
            )
            .await
            .unwrap();
        assert_eq!(objs.len(), 1);
    }

    #[tokio::test]
    async fn test_create_missing_partition_namespaces_concurrent_same_id() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let ns = create_partitioned_namespace_country_year(temp_path).await;
        let spec = ns.partition_spec().await.unwrap();

        ns.ensure_partition_fields_exists(&spec.fields)
            .await
            .unwrap();

        let ns = Arc::new(ns);
        let spec = Arc::new(spec);
        let values = Arc::new(vec![
            ScalarValue::Utf8(Some("US".to_string())),
            ScalarValue::Int32(Some(2020)),
        ]);

        let mut set = JoinSet::new();
        for _ in 0..8 {
            let ns = Arc::clone(&ns);
            let spec = Arc::clone(&spec);
            let values = Arc::clone(&values);
            set.spawn(async move {
                ns.create_missing_partition_namespaces(&spec, &values)
                    .await
                    .unwrap()
            });
        }

        let mut ids: Vec<Vec<String>> = vec![];
        while let Some(res) = set.join_next().await {
            ids.push(res.unwrap());
        }

        assert!(!ids.is_empty());
        for id in ids.iter() {
            assert_eq!(id, &ids[0]);
        }

        // Resolve partition namespace from __manifest by partition values. There should be only one
        // namespace row.
        let leaf = ns
            .resolve_partition_namespace(&spec, &values)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(leaf, ids[0]);
    }

    async fn create_partitioned_namespace_country_year(temp_path: &str) -> PartitionedNamespace {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("country", DataType::Utf8, true),
            ArrowField::new("year", DataType::Int32, true),
        ]);
        let schema = arrow_schema.clone();

        let spec = PartitionSpec {
            id: 1,
            fields: vec![
                PartitionField {
                    field_id: "country".to_string(),
                    source_ids: vec![0],
                    transform: Some(PartitionTransform::Identity),
                    expression: None,
                    result_type: DataType::Utf8,
                },
                PartitionField {
                    field_id: "year".to_string(),
                    source_ids: vec![1],
                    transform: Some(PartitionTransform::Identity),
                    expression: None,
                    result_type: DataType::Int32,
                },
            ],
        };

        let ns = PartitionedNamespaceBuilder::new(temp_path)
            .schema(schema)
            .partition_spec(spec.clone())
            .build()
            .await
            .unwrap();
        ns.ensure_partition_fields_exists(&spec.fields)
            .await
            .unwrap();

        ns
    }

    async fn list_ns_ids_by_country_year(
        ns: &PartitionedNamespace,
        country: &str,
        year: Option<i32>,
    ) -> Vec<(Vec<String>, String)> {
        let mut pred = col("object_type")
            .eq(lit("namespace"))
            .and(col("partition_field_country").eq(lit(country)));
        pred = match year {
            Some(y) => pred.and(col("partition_field_year").eq(lit(y))),
            None => pred.and(col("partition_field_year").is_null()),
        };

        ns.manifest
            .query_manifest(pred)
            .await
            .unwrap()
            .into_iter()
            .filter_map(|obj| match obj {
                ManifestObject::Namespace(n) => Some((n.namespace, n.name)),
                _ => None,
            })
            .collect()
    }
}
