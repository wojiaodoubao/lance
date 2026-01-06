// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
#![allow(unused)]

use crate::dir::json::JsonPartitionField;
use crate::DirectoryNamespace;
use arrow::array::RecordBatch;
use arrow_schema::DataType;
use async_trait::async_trait;
use bytes::Bytes;
use lance::deps::datafusion::logical_expr::Expr;
use lance_core::datatypes::{Field, Schema};
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
    InsertIntoTableResponse, ListNamespacesRequest, ListNamespacesResponse,
    ListTableIndicesRequest, ListTableIndicesResponse, ListTableTagsRequest, ListTableTagsResponse,
    ListTableVersionsRequest, ListTableVersionsResponse, ListTablesRequest, ListTablesResponse,
    MergeInsertIntoTableRequest, MergeInsertIntoTableResponse, NamespaceExistsRequest,
    QueryTableRequest, RegisterTableRequest, RegisterTableResponse, RenameTableRequest,
    RenameTableResponse, RestoreTableRequest, RestoreTableResponse, TableExistsRequest,
    UpdateTableRequest, UpdateTableResponse, UpdateTableSchemaMetadataRequest,
    UpdateTableSchemaMetadataResponse, UpdateTableTagRequest, UpdateTableTagResponse,
};
use lance_namespace::LanceNamespace;
use std::fmt::{Debug, Formatter};

/// A PartitionedNamespace is a directory namespace containing a collection of tables that share a
/// common schema. These tables are physically separated and independent, but logically related
/// through partition fields definition.
pub struct PartitionedNamespace {
    dir: DirectoryNamespace,
}

impl Debug for PartitionedNamespace {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
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
    pub fn plan_scan(&self, filter: &Expr) -> lance_core::Result<Vec<(PartitionTable, Expr)>> {
        todo!()
    }

    /// Resolve the target partition table for the input row. Create it (empty table) if not exists.
    ///
    /// # Arguments
    ///
    /// * `record` - The record batch to be resolved, it should contain only one row.
    ///
    /// Returns the partition table that the input row belongs to.
    pub fn resolve_or_create_partition_table(
        &self,
        record: &RecordBatch,
    ) -> lance_core::Result<PartitionTable> {
        todo!()
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
        todo!()
    }

    /// Schema of the partitioned namespace.
    pub fn schema(&self) -> Schema {
        todo!()
    }

    /// All partition tables of the partitioned namespace.
    pub fn tables(&self) -> Vec<PartitionTable> {
        todo!()
    }

    /// Partitioning of the partitioned namespace.
    pub fn partitioning(&self) -> Partitioning {
        todo!()
    }

    // Partition Evolution.

    // TODO: Based on `update_partition_spec`, we can provides PartitionUpdater api, e.g.:
    // ```
    // partition_ns.partition_updater()
    //     .remove_field("old_partition_field")
    //     .add_field("new_partition_field", Expressions.day("ts"))
    //     .commit();
    // ```
    /// Update the partition spec.
    ///
    /// # Arguments
    ///
    /// * `partition_spec` - The new partition spec.
    ///
    /// Returns the new partition spec.
    fn update_partition_spec(
        &self,
        partition_spec: Vec<PartitionField>,
    ) -> lance_core::Result<PartitionSpec> {
        todo!()
    }

    // Schema Evolution.

    /// Add a new column to the partitioned namespace.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to be added.
    ///
    /// Returns the new schema.
    fn add_column(&self, column: &Field) -> lance_core::Result<Schema> {
        todo!()
    }

    /// Drop the given column from the partitioned namespace.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to be dropped.
    ///
    /// Returns the new schema.
    fn drop_column(&self, column: &str) -> lance_core::Result<Schema> {
        todo!()
    }

    /// Rename the given column in the partitioned namespace.
    ///
    /// # Arguments
    ///
    /// * `old_name` - The old name of the column.
    /// * `new_name` - The new name of the column.
    ///
    /// Returns the new schema.
    fn rename_column(&self, old_name: &str, new_name: &str) -> lance_core::Result<Schema> {
        todo!()
    }

    /// Promote the type of the given column to the new type in the partitioned namespace.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to be promoted.
    /// * `new_type` - The new type of the column.
    ///
    /// Returns the new schema.
    fn type_promotion(&self, column: &str, new_type: &DataType) -> lance_core::Result<Schema> {
        todo!()
    }
}

#[async_trait]
impl LanceNamespace for PartitionedNamespace {
    async fn list_namespaces(
        &self,
        _request: ListNamespacesRequest,
    ) -> lance_core::Result<ListNamespacesResponse> {
        todo!()
    }

    async fn describe_namespace(
        &self,
        _request: DescribeNamespaceRequest,
    ) -> lance_core::Result<DescribeNamespaceResponse> {
        todo!()
    }

    async fn create_namespace(
        &self,
        _request: CreateNamespaceRequest,
    ) -> lance_core::Result<CreateNamespaceResponse> {
        todo!()
    }

    async fn drop_namespace(
        &self,
        _request: DropNamespaceRequest,
    ) -> lance_core::Result<DropNamespaceResponse> {
        todo!()
    }

    async fn namespace_exists(&self, _request: NamespaceExistsRequest) -> lance_core::Result<()> {
        todo!()
    }

    async fn list_tables(
        &self,
        _request: ListTablesRequest,
    ) -> lance_core::Result<ListTablesResponse> {
        todo!()
    }

    async fn describe_table(
        &self,
        _request: DescribeTableRequest,
    ) -> lance_core::Result<DescribeTableResponse> {
        todo!()
    }

    async fn register_table(
        &self,
        _request: RegisterTableRequest,
    ) -> lance_core::Result<RegisterTableResponse> {
        todo!()
    }

    async fn table_exists(&self, _request: TableExistsRequest) -> lance_core::Result<()> {
        todo!()
    }

    async fn drop_table(
        &self,
        _request: DropTableRequest,
    ) -> lance_core::Result<DropTableResponse> {
        todo!()
    }

    async fn deregister_table(
        &self,
        _request: DeregisterTableRequest,
    ) -> lance_core::Result<DeregisterTableResponse> {
        todo!()
    }

    async fn count_table_rows(&self, _request: CountTableRowsRequest) -> lance_core::Result<i64> {
        todo!()
    }

    async fn create_table(
        &self,
        _request: CreateTableRequest,
        _request_data: Bytes,
    ) -> lance_core::Result<CreateTableResponse> {
        todo!()
    }

    async fn declare_table(
        &self,
        _request: DeclareTableRequest,
    ) -> lance_core::Result<DeclareTableResponse> {
        todo!()
    }

    async fn create_empty_table(
        &self,
        _request: CreateEmptyTableRequest,
    ) -> lance_core::Result<CreateEmptyTableResponse> {
        todo!()
    }

    async fn insert_into_table(
        &self,
        _request: InsertIntoTableRequest,
        _request_data: Bytes,
    ) -> lance_core::Result<InsertIntoTableResponse> {
        todo!()
    }

    async fn merge_insert_into_table(
        &self,
        _request: MergeInsertIntoTableRequest,
        _request_data: Bytes,
    ) -> lance_core::Result<MergeInsertIntoTableResponse> {
        todo!()
    }

    async fn update_table(
        &self,
        _request: UpdateTableRequest,
    ) -> lance_core::Result<UpdateTableResponse> {
        todo!()
    }

    async fn delete_from_table(
        &self,
        _request: DeleteFromTableRequest,
    ) -> lance_core::Result<DeleteFromTableResponse> {
        todo!()
    }

    async fn query_table(&self, _request: QueryTableRequest) -> lance_core::Result<Bytes> {
        todo!()
    }

    async fn create_table_index(
        &self,
        _request: CreateTableIndexRequest,
    ) -> lance_core::Result<CreateTableIndexResponse> {
        todo!()
    }

    async fn list_table_indices(
        &self,
        _request: ListTableIndicesRequest,
    ) -> lance_core::Result<ListTableIndicesResponse> {
        todo!()
    }

    async fn describe_table_index_stats(
        &self,
        _request: DescribeTableIndexStatsRequest,
    ) -> lance_core::Result<DescribeTableIndexStatsResponse> {
        todo!()
    }

    async fn describe_transaction(
        &self,
        _request: DescribeTransactionRequest,
    ) -> lance_core::Result<DescribeTransactionResponse> {
        todo!()
    }

    async fn alter_transaction(
        &self,
        _request: AlterTransactionRequest,
    ) -> lance_core::Result<AlterTransactionResponse> {
        todo!()
    }

    async fn create_table_scalar_index(
        &self,
        _request: CreateTableIndexRequest,
    ) -> lance_core::Result<CreateTableScalarIndexResponse> {
        todo!()
    }

    async fn drop_table_index(
        &self,
        _request: DropTableIndexRequest,
    ) -> lance_core::Result<DropTableIndexResponse> {
        todo!()
    }

    async fn list_all_tables(
        &self,
        _request: ListTablesRequest,
    ) -> lance_core::Result<ListTablesResponse> {
        todo!()
    }

    async fn restore_table(
        &self,
        _request: RestoreTableRequest,
    ) -> lance_core::Result<RestoreTableResponse> {
        todo!()
    }

    async fn rename_table(
        &self,
        _request: RenameTableRequest,
    ) -> lance_core::Result<RenameTableResponse> {
        todo!()
    }

    async fn list_table_versions(
        &self,
        _request: ListTableVersionsRequest,
    ) -> lance_core::Result<ListTableVersionsResponse> {
        todo!()
    }

    async fn update_table_schema_metadata(
        &self,
        _request: UpdateTableSchemaMetadataRequest,
    ) -> lance_core::Result<UpdateTableSchemaMetadataResponse> {
        todo!()
    }

    async fn get_table_stats(
        &self,
        _request: GetTableStatsRequest,
    ) -> lance_core::Result<GetTableStatsResponse> {
        todo!()
    }

    async fn explain_table_query_plan(
        &self,
        _request: ExplainTableQueryPlanRequest,
    ) -> lance_core::Result<String> {
        todo!()
    }

    async fn analyze_table_query_plan(
        &self,
        _request: AnalyzeTableQueryPlanRequest,
    ) -> lance_core::Result<String> {
        todo!()
    }

    async fn alter_table_add_columns(
        &self,
        _request: AlterTableAddColumnsRequest,
    ) -> lance_core::Result<AlterTableAddColumnsResponse> {
        todo!()
    }

    async fn alter_table_alter_columns(
        &self,
        _request: AlterTableAlterColumnsRequest,
    ) -> lance_core::Result<AlterTableAlterColumnsResponse> {
        todo!()
    }

    async fn alter_table_drop_columns(
        &self,
        _request: AlterTableDropColumnsRequest,
    ) -> lance_core::Result<AlterTableDropColumnsResponse> {
        todo!()
    }

    async fn list_table_tags(
        &self,
        _request: ListTableTagsRequest,
    ) -> lance_core::Result<ListTableTagsResponse> {
        todo!()
    }

    async fn get_table_tag_version(
        &self,
        _request: GetTableTagVersionRequest,
    ) -> lance_core::Result<GetTableTagVersionResponse> {
        todo!()
    }

    async fn create_table_tag(
        &self,
        _request: CreateTableTagRequest,
    ) -> lance_core::Result<CreateTableTagResponse> {
        todo!()
    }

    async fn delete_table_tag(
        &self,
        _request: DeleteTableTagRequest,
    ) -> lance_core::Result<DeleteTableTagResponse> {
        todo!()
    }

    async fn update_table_tag(
        &self,
        _request: UpdateTableTagRequest,
    ) -> lance_core::Result<UpdateTableTagResponse> {
        todo!()
    }

    fn namespace_id(&self) -> String {
        todo!()
    }
}

/// Create a new partitioned namespace with the given location, schema, and partition.
///
/// # Arguments
///
/// * `location` - The location of the partitioned namespace.
/// * `schema` - The schema of the partitioned namespace.
/// * `partition` - The initial partition of the partitioned namespace.
///
/// Returns the created partitioned namespace.
pub fn create_partitioned_namespace(
    location: &str,
    schema: Schema,
    partition: PartitionSpec,
) -> lance_core::Result<PartitionedNamespace> {
    todo!()
}

/// Partition table of the partitioned namespace.
pub struct PartitionTable {
    id: Vec<String>,           // namespace id
    read_version: Option<u64>, // read version
}

/// Partitioning contains all partition specs of the partitioned namespace.
pub struct Partitioning {}

impl Partitioning {
    pub fn current() -> PartitionSpec {
        todo!()
    }
    pub fn id(id: i32) -> PartitionSpec {
        todo!()
    }
    pub fn all() -> Vec<PartitionSpec> {
        todo!()
    }
}

/// Partition specification defines how to derive partition values from a record in a partitioned
/// namespace.
pub struct PartitionSpec {
    id: i32,
    fields: Vec<PartitionField>,
}

/// Partition field definition.
pub struct PartitionField {
    field_id: String,      // Unique identifier for this partition field
    source_ids: Vec<i32>,  // Field IDs of the source columns in the schema
    expression: Expr,      // DataFusion expression using `col0`, `col1`, ... as column references
    result_type: DataType, // Result type of the partition value
}

impl PartitionField {
    pub fn to_json(self) -> JsonPartitionField {
        todo!()
    }
    pub fn from_json(json: &str) -> Self {
        todo!()
    }
}
