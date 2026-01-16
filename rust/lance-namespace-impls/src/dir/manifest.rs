// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Manifest-based namespace implementation
//!
//! This module provides a namespace implementation that uses a manifest table
//! to track tables and nested namespaces.

use crate::dir::manifest_ext::ManifestNamespaceExt;
use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchIterator, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{FieldRef, Schema};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::StreamExt;
use lance::dataset::optimize::{compact_files, CompactionOptions};
use lance::dataset::transaction::UpdateMapEntry;
use lance::dataset::{builder::DatasetBuilder, NewColumnTransform, WriteParams};
use lance::deps::datafusion::logical_expr::Expr;
use lance::deps::datafusion::prelude::{col, lit};
use lance::deps::datafusion::scalar::ScalarValue;
use lance::session::Session;
use lance::{dataset::scanner::Scanner, Dataset};
use lance_arrow::RecordBatchExt;
use lance_core::{box_error, Error, Result};
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_index::traits::DatasetIndexExt;
use lance_index::IndexType;
use lance_io::object_store::{ObjectStore, ObjectStoreParams};
use lance_namespace::models::{
    CreateEmptyTableRequest, CreateEmptyTableResponse, CreateNamespaceRequest,
    CreateNamespaceResponse, CreateTableRequest, CreateTableResponse, DeclareTableRequest,
    DeclareTableResponse, DeregisterTableRequest, DeregisterTableResponse,
    DescribeNamespaceRequest, DescribeNamespaceResponse, DescribeTableRequest,
    DescribeTableResponse, DropNamespaceRequest, DropNamespaceResponse, DropTableRequest,
    DropTableResponse, ListNamespacesRequest, ListNamespacesResponse, ListTablesRequest,
    ListTablesResponse, NamespaceExistsRequest, RegisterTableRequest, RegisterTableResponse,
    TableExistsRequest,
};
use lance_namespace::schema::arrow_schema_to_json;
use lance_namespace::LanceNamespace;
use object_store::path::Path;
use snafu::location;
use std::collections::HashSet;
use std::io::Cursor;
use std::str::FromStr;
use std::{
    collections::HashMap,
    f32, f64,
    hash::{DefaultHasher, Hash, Hasher},
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

const MANIFEST_TABLE_NAME: &str = "__manifest";
pub(crate) const DELIMITER: &str = "$";

// Index names for the __manifest table
/// BTREE index on the object_id column for fast lookups
const OBJECT_ID_INDEX_NAME: &str = "object_id_btree";
/// Bitmap index on the object_type column for filtering by type
const OBJECT_TYPE_INDEX_NAME: &str = "object_type_bitmap";
/// LabelList index on the base_objects column for view dependencies
const BASE_OBJECTS_INDEX_NAME: &str = "base_objects_label_list";

/// Object types that can be stored in the manifest
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectType {
    Namespace,
    Table,
}

impl ObjectType {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Namespace => "namespace",
            Self::Table => "table",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "namespace" => Ok(Self::Namespace),
            "table" => Ok(Self::Table),
            _ => Err(Error::io(
                format!("Invalid object type: {}", s),
                location!(),
            )),
        }
    }
}

pub enum ManifestObject {
    Table(TableInfo),
    Namespace(NamespaceInfo),
}

/// Information about a table stored in the manifest
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub namespace: Vec<String>,
    pub name: String,
    pub location: String,
    pub properties: Option<HashMap<String, String>>,
}

/// Information about a namespace stored in the manifest
#[derive(Debug, Clone)]
pub struct NamespaceInfo {
    pub namespace: Vec<String>,
    pub name: String,
    pub metadata: Option<HashMap<String, String>>,
}

/// A wrapper around a Dataset that provides concurrent access.
///
/// This can be cloned cheaply. It supports concurrent reads or exclusive writes.
/// The manifest dataset is always kept strongly consistent by reloading on each read.
#[derive(Debug, Clone)]
pub struct DatasetConsistencyWrapper(Arc<RwLock<Dataset>>);

impl DatasetConsistencyWrapper {
    /// Create a new wrapper with the given dataset.
    pub fn new(dataset: Dataset) -> Self {
        Self(Arc::new(RwLock::new(dataset)))
    }

    /// Get an immutable reference to the dataset.
    /// Always reloads to ensure strong consistency.
    pub async fn get(&self) -> Result<DatasetReadGuard<'_>> {
        self.reload().await?;
        Ok(DatasetReadGuard {
            guard: self.0.read().await,
        })
    }

    /// Get a mutable reference to the dataset.
    /// Always reloads to ensure strong consistency.
    pub async fn get_mut(&self) -> Result<DatasetWriteGuard<'_>> {
        self.reload().await?;
        Ok(DatasetWriteGuard {
            guard: self.0.write().await,
        })
    }

    /// Provide a known latest version of the dataset.
    ///
    /// This is usually done after some write operation, which inherently will
    /// have the latest version.
    pub async fn set_latest(&self, dataset: Dataset) {
        let mut write_guard = self.0.write().await;
        if dataset.manifest().version > write_guard.manifest().version {
            *write_guard = dataset;
        }
    }

    /// Reload the dataset to the latest version.
    async fn reload(&self) -> Result<()> {
        // First check if we need to reload (with read lock)
        let read_guard = self.0.read().await;
        let dataset_uri = read_guard.uri().to_string();
        let current_version = read_guard.version().version;
        log::debug!(
            "Reload starting for uri={}, current_version={}",
            dataset_uri,
            current_version
        );
        let latest_version = read_guard
            .latest_version_id()
            .await
            .map_err(|e| Error::IO {
                source: box_error(std::io::Error::other(format!(
                    "Failed to get latest version: {}",
                    e
                ))),
                location: location!(),
            })?;
        log::debug!(
            "Reload got latest_version={} for uri={}, current_version={}",
            latest_version,
            dataset_uri,
            current_version
        );
        drop(read_guard);

        // If already up-to-date, return early
        if latest_version == current_version {
            log::debug!("Already up-to-date for uri={}", dataset_uri);
            return Ok(());
        }

        // Need to reload, acquire write lock
        let mut write_guard = self.0.write().await;

        // Double-check after acquiring write lock (someone else might have reloaded)
        let latest_version = write_guard
            .latest_version_id()
            .await
            .map_err(|e| Error::IO {
                source: box_error(std::io::Error::other(format!(
                    "Failed to get latest version: {}",
                    e
                ))),
                location: location!(),
            })?;

        if latest_version != write_guard.version().version {
            write_guard.checkout_latest().await.map_err(|e| Error::IO {
                source: box_error(std::io::Error::other(format!(
                    "Failed to checkout latest: {}",
                    e
                ))),
                location: location!(),
            })?;
        }

        Ok(())
    }
}

pub struct DatasetReadGuard<'a> {
    guard: RwLockReadGuard<'a, Dataset>,
}

impl Deref for DatasetReadGuard<'_> {
    type Target = Dataset;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

pub struct DatasetWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, Dataset>,
}

impl Deref for DatasetWriteGuard<'_> {
    type Target = Dataset;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for DatasetWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

/// Extended properties are special properties started with `lance.manifest.extended.` prefix, and
/// stored in the manifest table.
///
/// For example, a namespace object contains metadata like:
/// ```json
/// {
///     "user_name": "Alice",
///     "lance.manifest.extended.user_id": "123456"
/// }
/// ```
/// The first one is stored at column named "metadata", the second is stored at column named "user_id".
pub(crate) static EXTENDED_PREFIX: &str = "lance.manifest.extended.";

/// Manifest-based namespace implementation
///
/// Uses a special `__manifest` Lance table to track tables and nested namespaces.
#[derive(Debug)]
pub struct ManifestNamespace {
    pub(crate) root: String,
    pub(crate) storage_options: Option<HashMap<String, String>>,
    #[allow(dead_code)]
    session: Option<Arc<Session>>,
    #[allow(dead_code)]
    pub(crate) object_store: Arc<ObjectStore>,
    #[allow(dead_code)]
    pub(crate) base_path: Path,
    pub(crate) manifest_dataset: DatasetConsistencyWrapper,
    /// Whether directory listing is enabled in dual mode
    /// If true, root namespace tables use {table_name}.lance naming
    /// If false, they use namespace-prefixed names
    pub(crate) dir_listing_enabled: bool,
    /// Whether to perform inline optimization (compaction and indexing) on the __manifest table
    /// after every write. Defaults to true.
    inline_optimization_enabled: bool,
}

impl ManifestNamespace {
    /// Create a new ManifestNamespace from an existing DirectoryNamespace
    pub async fn from_directory(
        root: String,
        storage_options: Option<HashMap<String, String>>,
        session: Option<Arc<Session>>,
        object_store: Arc<ObjectStore>,
        base_path: Path,
        dir_listing_enabled: bool,
        inline_optimization_enabled: bool,
    ) -> Result<Self> {
        let manifest_dataset =
            Self::create_or_get_manifest(&root, &storage_options, session.clone()).await?;

        Ok(Self {
            root,
            storage_options,
            session,
            object_store,
            base_path,
            manifest_dataset,
            dir_listing_enabled,
            inline_optimization_enabled,
        })
    }

    /// Build object ID from namespace path and name
    pub fn build_object_id(namespace: &[String], name: &str) -> String {
        if namespace.is_empty() {
            name.to_string()
        } else {
            let mut id = namespace.join(DELIMITER);
            id.push_str(DELIMITER);
            id.push_str(name);
            id
        }
    }

    /// Parse object ID into namespace path and name
    pub fn parse_object_id(object_id: &str) -> (Vec<String>, String) {
        let parts: Vec<&str> = object_id.split(DELIMITER).collect();
        if parts.len() == 1 {
            (Vec::new(), parts[0].to_string())
        } else {
            let namespace = parts[..parts.len() - 1]
                .iter()
                .map(|s| s.to_string())
                .collect();
            let name = parts[parts.len() - 1].to_string();
            (namespace, name)
        }
    }

    /// Add extended properties to the manifest table.
    pub async fn add_extended_properties(&self, properties: &Vec<(&str, DataType)>) -> Result<()> {
        let full_schema = self.full_manifest_schema().await?;
        let fields: Vec<Field> = properties
            .iter()
            .map(|(name, data_type)| {
                if !name.starts_with(EXTENDED_PREFIX) {
                    return Err(Error::io(
                        format!(
                            "Extended properties key {} must start with prefix: {}",
                            name, EXTENDED_PREFIX
                        ),
                        location!(),
                    ));
                }
                Ok(Field::new(
                    name.strip_prefix(EXTENDED_PREFIX).unwrap().to_string(),
                    data_type.clone(),
                    true,
                ))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .filter(|f| full_schema.column_with_name(f.name()).is_none())
            .collect();

        let schema = Schema::new(fields);
        let transform = NewColumnTransform::AllNulls(Arc::new(schema));

        let mut ds = self.manifest_dataset.get_mut().await?;
        ds.add_columns(transform, None, None).await?;

        Ok(())
    }

    /// Get all extended properties keys
    pub async fn get_extended_properties_keys(&self) -> Result<Vec<String>> {
        let basic_cols: HashSet<String> = Self::basic_manifest_schema()
            .fields
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        let mut extended_props_keys = vec![];
        for f in self.full_manifest_schema().await?.fields.iter() {
            if !basic_cols.contains(f.name().as_str()) {
                extended_props_keys.push(f.name().to_string());
            }
        }
        Ok(extended_props_keys)
    }

    /// Remove extended properties from the manifest table.
    pub async fn remove_extended_properties(&mut self, properties: &Vec<&str>) -> Result<()> {
        let full_schema = self.full_manifest_schema().await?;
        let to_remove: Vec<String> = properties
            .iter()
            .map(|name| {
                if !name.starts_with(EXTENDED_PREFIX) {
                    return Err(Error::io(
                        format!(
                            "Extended properties key {} must start with prefix: {}",
                            name, EXTENDED_PREFIX
                        ),
                        location!(),
                    ));
                }
                Ok(name.strip_prefix(EXTENDED_PREFIX).unwrap().to_string())
            })
            .collect::<Result<Vec<String>>>()?
            .into_iter()
            .filter(|s| full_schema.column_with_name(s.as_str()).is_some())
            .collect();
        let remove: Vec<&str> = to_remove.iter().map(|s| s.as_str()).collect();

        let mut ds = self.manifest_dataset.get_mut().await?;
        ds.drop_columns(&remove).await
    }

    /// Split an object ID (table_id as vec of strings) into namespace and table name
    pub(crate) fn split_object_id(table_id: &[String]) -> (Vec<String>, String) {
        if table_id.len() == 1 {
            (vec![], table_id[0].clone())
        } else {
            (
                table_id[..table_id.len() - 1].to_vec(),
                table_id[table_id.len() - 1].clone(),
            )
        }
    }

    /// Convert a table ID (vec of strings) to an object_id string
    fn str_object_id(table_id: &[String]) -> String {
        table_id.join(DELIMITER)
    }

    /// Generate a new directory name in format: <hash>_<object_id>
    /// The hash is used to (1) optimize object store throughput,
    /// (2) have high enough entropy in a short period of time to prevent issues like
    /// failed table creation, delete and create new table of the same name, etc.
    /// The object_id is added after the hash to ensure
    /// dir name uniqueness and make debugging easier.
    pub(crate) fn generate_dir_name(object_id: &str) -> String {
        // Generate a random number for uniqueness
        let random_num: u64 = rand::random();

        // Create hash from random number + object_id
        let mut hasher = DefaultHasher::new();
        random_num.hash(&mut hasher);
        object_id.hash(&mut hasher);
        let hash = hasher.finish();

        // Format as lowercase hex (8 characters - sufficient entropy for uniqueness)
        format!("{:08x}_{}", (hash & 0xFFFFFFFF) as u32, object_id)
    }

    /// Construct a full URI from root and relative location
    pub(crate) fn construct_full_uri(root: &str, relative_location: &str) -> Result<String> {
        let mut base_url = lance_io::object_store::uri_to_url(root)?;

        // Ensure the base URL has a trailing slash so that URL.join() appends
        // rather than replaces the last path segment.
        // Without this fix, "s3://bucket/path/subdir".join("table.lance")
        // would incorrectly produce "s3://bucket/path/table.lance" (missing subdir).
        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }

        let full_url = base_url
            .join(relative_location)
            .map_err(|e| Error::InvalidInput {
                source: format!(
                    "Failed to join URI '{}' with '{}': {:?}",
                    root, relative_location, e
                )
                .into(),
                location: location!(),
            })?;

        Ok(full_url.to_string())
    }

    /// Perform inline optimization on the __manifest table.
    ///
    /// This method:
    /// 1. Creates three indexes on the manifest table:
    ///    - BTREE index on object_id for fast lookups
    ///    - Bitmap index on object_type for filtering by type
    ///    - LabelList index on base_objects for view dependencies
    /// 2. Runs file compaction to merge small files
    /// 3. Optimizes existing indices
    ///
    /// This is called automatically after writes when inline_optimization_enabled is true.
    pub(crate) async fn run_inline_optimization(&self) -> Result<()> {
        if !self.inline_optimization_enabled {
            return Ok(());
        }

        // Get a mutable reference to the dataset to perform optimization
        let mut dataset_guard = self.manifest_dataset.get_mut().await?;
        let dataset: &mut Dataset = &mut dataset_guard;

        // Step 1: Create indexes if they don't already exist
        let indices = dataset.load_indices().await?;

        // Check which indexes already exist
        let has_object_id_index = indices.iter().any(|idx| idx.name == OBJECT_ID_INDEX_NAME);
        let has_object_type_index = indices.iter().any(|idx| idx.name == OBJECT_TYPE_INDEX_NAME);
        let has_base_objects_index = indices
            .iter()
            .any(|idx| idx.name == BASE_OBJECTS_INDEX_NAME);

        // Create BTREE index on object_id
        if !has_object_id_index {
            log::debug!(
                "Creating BTREE index '{}' on object_id for __manifest table",
                OBJECT_ID_INDEX_NAME
            );
            let params = ScalarIndexParams::for_builtin(BuiltinIndexType::BTree);
            if let Err(e) = dataset
                .create_index(
                    &["object_id"],
                    IndexType::BTree,
                    Some(OBJECT_ID_INDEX_NAME.to_string()),
                    &params,
                    true,
                )
                .await
            {
                log::warn!("Failed to create BTREE index on object_id for __manifest table: {:?}. Query performance may be impacted.", e);
            } else {
                log::info!(
                    "Created BTREE index '{}' on object_id for __manifest table",
                    OBJECT_ID_INDEX_NAME
                );
            }
        }

        // Create Bitmap index on object_type
        if !has_object_type_index {
            log::debug!(
                "Creating Bitmap index '{}' on object_type for __manifest table",
                OBJECT_TYPE_INDEX_NAME
            );
            let params = ScalarIndexParams::default();
            if let Err(e) = dataset
                .create_index(
                    &["object_type"],
                    IndexType::Bitmap,
                    Some(OBJECT_TYPE_INDEX_NAME.to_string()),
                    &params,
                    true,
                )
                .await
            {
                log::warn!("Failed to create Bitmap index on object_type for __manifest table: {:?}. Query performance may be impacted.", e);
            } else {
                log::info!(
                    "Created Bitmap index '{}' on object_type for __manifest table",
                    OBJECT_TYPE_INDEX_NAME
                );
            }
        }

        // Create LabelList index on base_objects
        if !has_base_objects_index {
            log::debug!(
                "Creating LabelList index '{}' on base_objects for __manifest table",
                BASE_OBJECTS_INDEX_NAME
            );
            let params = ScalarIndexParams::default();
            if let Err(e) = dataset
                .create_index(
                    &["base_objects"],
                    IndexType::LabelList,
                    Some(BASE_OBJECTS_INDEX_NAME.to_string()),
                    &params,
                    true,
                )
                .await
            {
                log::warn!("Failed to create LabelList index on base_objects for __manifest table: {:?}. Query performance may be impacted.", e);
            } else {
                log::info!(
                    "Created LabelList index '{}' on base_objects for __manifest table",
                    BASE_OBJECTS_INDEX_NAME
                );
            }
        }

        // Step 2: Run file compaction
        log::debug!("Running file compaction on __manifest table");
        match compact_files(dataset, CompactionOptions::default(), None).await {
            Ok(compaction_metrics) => {
                if compaction_metrics.fragments_removed > 0 {
                    log::info!(
                        "Compacted __manifest table: removed {} fragments, added {} fragments",
                        compaction_metrics.fragments_removed,
                        compaction_metrics.fragments_added
                    );
                }
            }
            Err(e) => {
                log::warn!("Failed to compact files for __manifest table: {:?}. Continuing with optimization.", e);
            }
        }

        // Step 3: Optimize indices
        log::debug!("Optimizing indices on __manifest table");
        match dataset.optimize_indices(&OptimizeOptions::default()).await {
            Ok(_) => {
                log::info!("Successfully optimized indices on __manifest table");
            }
            Err(e) => {
                log::warn!(
                    "Failed to optimize indices on __manifest table: {:?}. Continuing anyway.",
                    e
                );
            }
        }

        Ok(())
    }

    /// Get the manifest schema of basic fields: object_id, object_type, location, metadata, base_objects
    fn basic_manifest_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("object_id", DataType::Utf8, false),
            Field::new("object_type", DataType::Utf8, false),
            Field::new("location", DataType::Utf8, true),
            Field::new("metadata", DataType::Utf8, true),
            Field::new(
                "base_objects",
                DataType::List(Arc::new(Field::new("object_id", DataType::Utf8, true))),
                true,
            ),
        ]))
    }

    /// Get the full manifest schema, including basic fields and extended fields.
    pub(crate) async fn full_manifest_schema(&self) -> Result<ArrowSchema> {
        let dataset_guard = self.manifest_dataset.get().await?;
        let schema = ArrowSchema::from(dataset_guard.schema());
        Ok(schema)
    }

    /// Get a scanner for the manifest dataset
    async fn manifest_scanner(&self) -> Result<Scanner> {
        let dataset_guard = self.manifest_dataset.get().await?;
        Ok(dataset_guard.scan())
    }

    /// Helper to execute a scanner and collect results into a Vec
    async fn execute_scanner(scanner: Scanner) -> Result<Vec<RecordBatch>> {
        let mut stream = scanner.try_into_stream().await.map_err(|e| Error::IO {
            source: box_error(std::io::Error::other(format!(
                "Failed to create stream: {}",
                e
            ))),
            location: location!(),
        })?;

        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.map_err(|e| Error::IO {
                source: box_error(std::io::Error::other(format!(
                    "Failed to read batch: {}",
                    e
                ))),
                location: location!(),
            })?);
        }

        Ok(batches)
    }

    /// Helper to get a string column from a record batch
    fn get_string_column<'a>(batch: &'a RecordBatch, column_name: &str) -> Result<&'a StringArray> {
        let column = batch
            .column_by_name(column_name)
            .ok_or_else(|| Error::io(format!("Column '{}' not found", column_name), location!()))?;
        column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                Error::io(
                    format!("Column '{}' is not a string array", column_name),
                    location!(),
                )
            })
    }

    /// Check if the manifest contains an object with the given ID
    async fn manifest_contains_object(&self, object_id: &str) -> Result<bool> {
        let dataset_guard = self.manifest_dataset.get().await?;
        let mut scanner = dataset_guard.scan();

        scanner.filter_expr(col("object_id").eq(lit(object_id.to_string())));

        // Project no columns and enable row IDs for count_rows to work
        scanner.project::<&str>(&[]).map_err(|e| Error::IO {
            source: box_error(std::io::Error::other(format!("Failed to project: {}", e))),
            location: location!(),
        })?;

        scanner.with_row_id();

        let count = scanner.count_rows().await.map_err(|e| Error::IO {
            source: box_error(std::io::Error::other(format!(
                "Failed to count rows: {}",
                e
            ))),
            location: location!(),
        })?;

        Ok(count > 0)
    }

    /// Query the manifest for a table with the given object ID
    pub(crate) async fn query_manifest_for_table(
        &self,
        object_id: &str,
    ) -> Result<Option<TableInfo>> {
        let objects = self
            .query_manifest(
                col("object_id")
                    .eq(lit(object_id.to_string()))
                    .and(col("object_type").eq(lit("table"))),
            )
            .await?;
        let mut found: Option<TableInfo> = None;
        for obj in objects {
            let ManifestObject::Table(t) = obj else {
                continue;
            };
            if found.is_some() {
                return Err(Error::io(
                    format!(
                        "Expected exactly 1 table with id '{}', found more than 1",
                        object_id
                    ),
                    location!(),
                ));
            }
            found = Some(t);
        }
        Ok(found)
    }

    /// List all table locations in the manifest (for root namespace only)
    /// Returns a set of table locations (e.g., "table_name.lance")
    pub async fn list_manifest_table_locations(&self) -> Result<std::collections::HashSet<String>> {
        let filter = "object_type = 'table' AND NOT contains(object_id, '$')";
        let mut scanner = self.manifest_scanner().await?;
        scanner.filter(filter).map_err(|e| Error::IO {
            source: box_error(std::io::Error::other(format!("Failed to filter: {}", e))),
            location: location!(),
        })?;
        scanner.project(&["location"]).map_err(|e| Error::IO {
            source: box_error(std::io::Error::other(format!("Failed to project: {}", e))),
            location: location!(),
        })?;

        let batches = Self::execute_scanner(scanner).await?;
        let mut locations = std::collections::HashSet::new();

        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let location_array = Self::get_string_column(&batch, "location")?;
            for i in 0..location_array.len() {
                locations.insert(location_array.value(i).to_string());
            }
        }

        Ok(locations)
    }

    /// Insert an entry into the manifest table
    async fn insert_into_manifest(
        &self,
        object_id: String,
        object_type: ObjectType,
        location: Option<String>,
    ) -> Result<()> {
        self.insert_into_manifest_with_metadata(object_id, object_type, location, None, None, None)
            .await
    }

    /// Insert an entry into the manifest table with metadata and base_objects
    pub(crate) async fn insert_into_manifest_with_metadata(
        &self,
        object_id: String,
        object_type: ObjectType,
        location: Option<String>,
        metadata: Option<String>,
        base_objects: Option<Vec<String>>,
        extended_batch: Option<RecordBatch>,
    ) -> Result<()> {
        use arrow::array::builder::{ListBuilder, StringBuilder};

        let basic_schema = Self::basic_manifest_schema();

        // Create base_objects array from the provided list
        let string_builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(string_builder).with_field(Arc::new(Field::new(
            "object_id",
            DataType::Utf8,
            true,
        )));

        match base_objects {
            Some(objects) => {
                for obj in objects {
                    list_builder.values().append_value(obj);
                }
                list_builder.append(true);
            }
            None => {
                list_builder.append_null();
            }
        }

        let base_objects_array = list_builder.finish();

        // Create arrays with optional values
        let location_array = match location {
            Some(loc) => Arc::new(StringArray::from(vec![Some(loc)])),
            None => Arc::new(StringArray::from(vec![None::<String>])),
        };

        let metadata_array = match metadata {
            Some(meta) => Arc::new(StringArray::from(vec![Some(meta)])),
            None => Arc::new(StringArray::from(vec![None::<String>])),
        };

        let batch = RecordBatch::try_new(
            basic_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![object_id.as_str()])),
                Arc::new(StringArray::from(vec![object_type.as_str()])),
                location_array,
                metadata_array,
                Arc::new(base_objects_array),
            ],
        )
        .map_err(|e| {
            Error::io(
                format!("Failed to create manifest entry: {}", e),
                location!(),
            )
        })?;

        // Merge extended_batch with basic batch if provided
        let batch = if let Some(extended_batch) = extended_batch {
            batch.merge(&extended_batch)?
        } else {
            batch
        };

        let schema = batch.schema();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);

        // Use MergeInsert to ensure uniqueness on object_id
        let dataset_guard = self.manifest_dataset.get().await?;
        let dataset_arc = Arc::new(dataset_guard.clone());
        drop(dataset_guard); // Drop read guard before merge insert

        let mut merge_builder =
            lance::dataset::MergeInsertBuilder::try_new(dataset_arc, vec!["object_id".to_string()])
                .map_err(|e| Error::IO {
                    source: box_error(std::io::Error::other(format!(
                        "Failed to create merge builder: {}",
                        e
                    ))),
                    location: location!(),
                })?;

        merge_builder.when_matched(lance::dataset::WhenMatched::Fail);
        merge_builder.when_not_matched(lance::dataset::WhenNotMatched::InsertAll);

        let (new_dataset_arc, _merge_stats) = merge_builder
            .try_build()
            .map_err(|e| Error::IO {
                source: box_error(std::io::Error::other(format!(
                    "Failed to build merge: {}",
                    e
                ))),
                location: location!(),
            })?
            .execute_reader(Box::new(reader))
            .await
            .map_err(|e| {
                // Check if this is a "matched row" error from WhenMatched::Fail
                let error_msg = e.to_string();
                if error_msg.contains("matched")
                    || error_msg.contains("duplicate")
                    || error_msg.contains("already exists")
                {
                    Error::io(
                        format!("Object with id '{}' already exists in manifest", object_id),
                        location!(),
                    )
                } else {
                    Error::IO {
                        source: box_error(std::io::Error::other(format!(
                            "Failed to execute merge: {}",
                            e
                        ))),
                        location: location!(),
                    }
                }
            })?;

        let new_dataset = Arc::try_unwrap(new_dataset_arc).unwrap_or_else(|arc| (*arc).clone());
        self.manifest_dataset.set_latest(new_dataset).await;

        // Run inline optimization after write
        if let Err(e) = self.run_inline_optimization().await {
            log::warn!(
                "Unexpected failure when running inline optimization: {:?}",
                e
            );
        }

        Ok(())
    }

    /// Delete an entry from the manifest table
    pub async fn delete_from_manifest(&self, object_id: &str) -> Result<()> {
        {
            let predicate = format!("object_id = '{}'", object_id);
            let mut dataset_guard = self.manifest_dataset.get_mut().await?;
            dataset_guard
                .delete(&predicate)
                .await
                .map_err(|e| Error::IO {
                    source: box_error(std::io::Error::other(format!("Failed to delete: {}", e))),
                    location: location!(),
                })?;
        } // Drop the guard here

        self.manifest_dataset.reload().await?;

        // Run inline optimization after delete
        if let Err(e) = self.run_inline_optimization().await {
            log::warn!(
                "Unexpected failure when running inline optimization: {:?}",
                e
            );
        }

        Ok(())
    }

    /// Register a table in the manifest without creating the physical table (internal helper for migration)
    pub async fn register_table(&self, name: &str, location: String) -> Result<()> {
        let object_id = Self::build_object_id(&[], name);
        if self.manifest_contains_object(&object_id).await? {
            return Err(Error::io(
                format!("Table '{}' already exists", name),
                location!(),
            ));
        }

        self.insert_into_manifest(object_id, ObjectType::Table, Some(location))
            .await
    }

    /// Get metadata of __manifest table
    pub async fn get_metadata(&self) -> Result<HashMap<String, String>> {
        let ds = self.manifest_dataset.get().await?;
        Ok(ds.metadata().clone())
    }

    /// Update metadata to __manifest table
    pub async fn update_metadata(
        &self,
        values: impl IntoIterator<Item = impl Into<UpdateMapEntry>>,
    ) -> Result<HashMap<String, String>> {
        let mut ds = self.manifest_dataset.get_mut().await?;
        ds.update_metadata(values).await
    }

    /// Validate that all levels of a namespace path exist
    pub(crate) async fn validate_namespace_levels_exist(
        &self,
        namespace_path: &[String],
    ) -> Result<()> {
        for i in 1..=namespace_path.len() {
            let partial_path = &namespace_path[..i];
            let object_id = partial_path.join(DELIMITER);
            if !self.manifest_contains_object(&object_id).await? {
                return Err(Error::Namespace {
                    source: format!("Parent namespace '{}' does not exist", object_id).into(),
                    location: location!(),
                });
            }
        }
        Ok(())
    }

    /// Query the manifest for a namespace with the given object ID
    async fn query_manifest_for_namespace(&self, object_id: &str) -> Result<Option<NamespaceInfo>> {
        let objects = self
            .query_manifest(
                col("object_id")
                    .eq(lit(object_id.to_string()))
                    .and(col("object_type").eq(lit("namespace"))),
            )
            .await?;
        let mut found: Option<NamespaceInfo> = None;
        for obj in objects {
            let ManifestObject::Namespace(ns) = obj else {
                continue;
            };
            if found.is_some() {
                return Err(Error::io(
                    format!(
                        "Expected exactly 1 namespace with id '{}', found more than 1",
                        object_id
                    ),
                    location!(),
                ));
            }
            found = Some(ns);
        }
        Ok(found)
    }

    pub(crate) async fn query_manifest(&self, filter: Expr) -> Result<Vec<ManifestObject>> {
        let mut scanner = self.manifest_scanner().await?;
        scanner.filter_expr(filter);
        let batches = Self::execute_scanner(scanner).await?;

        let mut objects: Vec<ManifestObject> = vec![];

        for batch in batches.iter() {
            for row_idx in 0..batch.num_rows() {
                let sliced_columns: Vec<Arc<dyn Array>> = batch
                    .columns()
                    .iter()
                    .map(|col| col.slice(row_idx, 1))
                    .collect();
                let row = RecordBatch::try_new(batch.schema(), sliced_columns)?;
                objects.push(parse_manifest_object(&row)?);
            }
        }
        Ok(objects)
    }

    /// Create or get the manifest dataset
    async fn create_or_get_manifest(
        root: &str,
        storage_options: &Option<HashMap<String, String>>,
        session: Option<Arc<Session>>,
    ) -> Result<DatasetConsistencyWrapper> {
        let manifest_path = format!("{}/{}", root, MANIFEST_TABLE_NAME);
        log::debug!("Attempting to load manifest from {}", manifest_path);
        let mut builder = DatasetBuilder::from_uri(&manifest_path);

        if let Some(sess) = session.clone() {
            builder = builder.with_session(sess);
        }

        if let Some(opts) = storage_options {
            builder = builder.with_storage_options(opts.clone());
        }

        let dataset_result = builder.load().await;
        if let Ok(dataset) = dataset_result {
            Ok(DatasetConsistencyWrapper::new(dataset))
        } else {
            log::info!("Creating new manifest table at {}", manifest_path);
            let schema = Self::basic_manifest_schema();
            let empty_batch = RecordBatch::new_empty(schema.clone());
            let reader = RecordBatchIterator::new(vec![Ok(empty_batch)], schema.clone());

            let write_params = WriteParams {
                session,
                store_params: storage_options.as_ref().map(|opts| ObjectStoreParams {
                    storage_options_accessor: Some(Arc::new(
                        lance_io::object_store::StorageOptionsAccessor::with_static_options(
                            opts.clone(),
                        ),
                    )),
                    ..Default::default()
                }),
                ..Default::default()
            };

            let dataset = Dataset::write(Box::new(reader), &manifest_path, Some(write_params))
                .await
                .map_err(|e| Error::IO {
                    source: box_error(std::io::Error::other(format!(
                        "Failed to create manifest dataset: {}",
                        e
                    ))),
                    location: location!(),
                })?;

            log::info!(
                "Successfully created manifest table at {}, version={}, uri={}",
                manifest_path,
                dataset.version().version,
                dataset.uri()
            );
            Ok(DatasetConsistencyWrapper::new(dataset))
        }
    }

    pub(crate) fn build_metadata_json(
        properties: &Option<HashMap<String, String>>,
    ) -> Option<String> {
        properties.as_ref().and_then(|props| {
            if props.is_empty() {
                None
            } else {
                let meta_props = props
                    .iter()
                    .filter(|(key, _)| !key.starts_with(EXTENDED_PREFIX))
                    .collect::<HashMap<_, _>>();
                Some(serde_json::to_string(&meta_props).ok()?)
            }
        })
    }
}

/// Parse one row of __manifest table into manifest object.
fn parse_manifest_object(batch: &RecordBatch) -> Result<ManifestObject> {
    if batch.num_rows() == 0 {
        return Err(Error::InvalidInput {
            source: "batch must have at least one row".into(),
            location: location!(),
        });
    }

    // Parse properties
    let mut merged = batch_to_extended_props(batch);
    let metadata_array = ManifestNamespace::get_string_column(batch, "metadata")?;

    if !metadata_array.is_null(0) {
        let metadata_str = metadata_array.value(0);
        match serde_json::from_str::<HashMap<String, String>>(metadata_str) {
            Ok(map) => merged.extend(map),
            Err(e) => {
                return Err(Error::io(
                    format!("Failed to deserialize metadata: {}", e),
                    location!(),
                ));
            }
        }
    }

    let properties = if merged.is_empty() {
        None
    } else {
        Some(merged)
    };

    // Parse manifest object
    let object_type = ManifestNamespace::get_string_column(batch, "object_type")?;
    let object_type = object_type.value(0).to_string();
    match object_type.as_str() {
        "namespace" => {
            let object_id_array = ManifestNamespace::get_string_column(batch, "object_id")?;
            let (namespace, name) = ManifestNamespace::parse_object_id(object_id_array.value(0));
            Ok(ManifestObject::Namespace(NamespaceInfo {
                namespace,
                name,
                metadata: properties,
            }))
        }
        "table" => {
            let object_id_array = ManifestNamespace::get_string_column(batch, "object_id")?;
            let location_array = ManifestNamespace::get_string_column(batch, "location")?;
            let location = location_array.value(0).to_string();
            let (namespace, name) = ManifestNamespace::parse_object_id(object_id_array.value(0));
            Ok(ManifestObject::Table(TableInfo {
                namespace,
                name,
                location,
                properties,
            }))
        }
        t => Err(Error::Internal {
            message: format!("Unknown object type {}", t),
            location: location!(),
        }),
    }
}

#[async_trait]
impl LanceNamespace for ManifestNamespace {
    fn namespace_id(&self) -> String {
        self.root.clone()
    }

    async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
        let namespace_id = request.id.as_ref().ok_or_else(|| Error::InvalidInput {
            source: "Namespace ID is required".into(),
            location: location!(),
        })?;

        // Build filter to find tables in this namespace
        let filter = if namespace_id.is_empty() {
            // Root namespace: find tables without a namespace prefix
            "object_type = 'table' AND NOT contains(object_id, '$')".to_string()
        } else {
            // Namespaced: find tables that start with namespace$ but have no additional $
            let prefix = namespace_id.join(DELIMITER);
            format!(
                "object_type = 'table' AND starts_with(object_id, '{}{}') AND NOT contains(substring(object_id, {}), '$')",
                prefix, DELIMITER, prefix.len() + 2
            )
        };

        let mut scanner = self.manifest_scanner().await?;
        scanner.filter(&filter).map_err(|e| Error::IO {
            source: box_error(std::io::Error::other(format!("Failed to filter: {}", e))),
            location: location!(),
        })?;
        scanner.project(&["object_id"]).map_err(|e| Error::IO {
            source: box_error(std::io::Error::other(format!("Failed to project: {}", e))),
            location: location!(),
        })?;

        let batches = Self::execute_scanner(scanner).await?;

        let mut tables = Vec::new();
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }

            let object_id_array = Self::get_string_column(&batch, "object_id")?;
            for i in 0..batch.num_rows() {
                let object_id = object_id_array.value(i);
                let (_namespace, name) = Self::parse_object_id(object_id);
                tables.push(name);
            }
        }

        Ok(ListTablesResponse::new(tables))
    }

    async fn describe_table(&self, request: DescribeTableRequest) -> Result<DescribeTableResponse> {
        let table_id = request.id.as_ref().ok_or_else(|| Error::InvalidInput {
            source: "Table ID is required".into(),
            location: location!(),
        })?;

        if table_id.is_empty() {
            return Err(Error::InvalidInput {
                source: "Table ID cannot be empty".into(),
                location: location!(),
            });
        }

        let object_id = Self::str_object_id(table_id);
        let table_info = self.query_manifest_for_table(&object_id).await?;

        // Extract table name and namespace from table_id
        let table_name = table_id.last().cloned().unwrap_or_default();
        let namespace_id: Vec<String> = if table_id.len() > 1 {
            table_id[..table_id.len() - 1].to_vec()
        } else {
            vec![]
        };

        let load_detailed_metadata = request.load_detailed_metadata.unwrap_or(false);
        // For backwards compatibility, only skip vending credentials when explicitly set to false
        let vend_credentials = request.vend_credentials.unwrap_or(true);

        match table_info {
            Some(info) => {
                // Construct full URI from relative location
                let table_uri = Self::construct_full_uri(&self.root, &info.location)?;

                let storage_options = if vend_credentials {
                    self.storage_options.clone()
                } else {
                    None
                };

                // If not loading detailed metadata, return minimal response with just location
                if !load_detailed_metadata {
                    return Ok(DescribeTableResponse {
                        table: Some(table_name),
                        namespace: Some(namespace_id),
                        location: Some(table_uri.clone()),
                        table_uri: Some(table_uri),
                        storage_options,
                        properties: info.properties,
                        ..Default::default()
                    });
                }

                // Try to open the dataset to get version and schema
                match Dataset::open(&table_uri).await {
                    Ok(mut dataset) => {
                        // If a specific version is requested, checkout that version
                        if let Some(requested_version) = request.version {
                            dataset = dataset.checkout_version(requested_version as u64).await?;
                        }

                        let version = dataset.version().version;
                        let lance_schema = dataset.schema();
                        let arrow_schema: arrow_schema::Schema = lance_schema.into();
                        let json_schema = arrow_schema_to_json(&arrow_schema)?;

                        Ok(DescribeTableResponse {
                            table: Some(table_name.clone()),
                            namespace: Some(namespace_id.clone()),
                            version: Some(version as i64),
                            location: Some(table_uri.clone()),
                            table_uri: Some(table_uri),
                            schema: Some(Box::new(json_schema)),
                            storage_options,
                            properties: info.properties,
                            ..Default::default()
                        })
                    }
                    Err(_) => {
                        // If dataset can't be opened (e.g., empty table), return minimal info
                        Ok(DescribeTableResponse {
                            table: Some(table_name),
                            namespace: Some(namespace_id),
                            location: Some(table_uri.clone()),
                            table_uri: Some(table_uri),
                            storage_options,
                            properties: info.properties,
                            ..Default::default()
                        })
                    }
                }
            }
            None => Err(Error::Namespace {
                source: format!("Table '{}' not found", object_id).into(),
                location: location!(),
            }),
        }
    }

    async fn table_exists(&self, request: TableExistsRequest) -> Result<()> {
        let table_id = request.id.as_ref().ok_or_else(|| Error::InvalidInput {
            source: "Table ID is required".into(),
            location: location!(),
        })?;

        if table_id.is_empty() {
            return Err(Error::InvalidInput {
                source: "Table ID cannot be empty".into(),
                location: location!(),
            });
        }

        let (namespace, table_name) = Self::split_object_id(table_id);
        let object_id = Self::build_object_id(&namespace, &table_name);
        let exists = self.manifest_contains_object(&object_id).await?;
        if exists {
            Ok(())
        } else {
            Err(Error::Namespace {
                source: format!("Table '{}' not found", table_name).into(),
                location: location!(),
            })
        }
    }

    async fn create_table(
        &self,
        request: CreateTableRequest,
        data: Bytes,
    ) -> Result<CreateTableResponse> {
        let table_id = request.id.as_ref().ok_or_else(|| Error::InvalidInput {
            source: "Table ID is required".into(),
            location: location!(),
        })?;

        if table_id.is_empty() {
            return Err(Error::InvalidInput {
                source: "Table ID cannot be empty".into(),
                location: location!(),
            });
        }

        let (namespace, table_name) = Self::split_object_id(table_id);
        let object_id = Self::build_object_id(&namespace, &table_name);

        // Serialize properties and compute extended batch if provided
        let metadata = Self::build_metadata_json(&request.properties);

        let extended_batch = if let Some(props) = &request.properties {
            batch_from_extended_props(props, &self.full_manifest_schema().await?)?
        } else {
            None
        };

        // Check if table already exists in manifest
        if self.manifest_contains_object(&object_id).await? {
            return Err(Error::io(
                format!("Table '{}' already exists", table_name),
                location!(),
            ));
        }

        // Create the physical table location with hash-based naming
        // When dir_listing_enabled is true and it's a root table, use directory-style naming: {table_name}.lance
        // Otherwise, use hash-based naming: {hash}_{object_id}
        let dir_name = if namespace.is_empty() && self.dir_listing_enabled {
            // Root table with directory listing enabled: use {table_name}.lance
            format!("{}.lance", table_name)
        } else {
            // Child namespace table or dir listing disabled: use hash-based naming
            Self::generate_dir_name(&object_id)
        };
        let table_uri = Self::construct_full_uri(&self.root, &dir_name)?;

        // Validate that request_data is provided
        if data.is_empty() {
            return Err(Error::Namespace {
                source: "Request data (Arrow IPC stream) is required for create_table".into(),
                location: location!(),
            });
        }

        // Write the data using Lance Dataset
        let cursor = Cursor::new(data.to_vec());
        let stream_reader = StreamReader::try_new(cursor, None)
            .map_err(|e| Error::io(format!("Failed to read IPC stream: {}", e), location!()))?;

        let batches: Vec<RecordBatch> =
            stream_reader
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| Error::io(format!("Failed to collect batches: {}", e), location!()))?;

        if batches.is_empty() {
            return Err(Error::io(
                "No data provided for table creation",
                location!(),
            ));
        }

        let schema = batches[0].schema();
        let batch_results: Vec<std::result::Result<RecordBatch, arrow_schema::ArrowError>> =
            batches.into_iter().map(Ok).collect();
        let reader = RecordBatchIterator::new(batch_results, schema);

        let write_params = WriteParams::default();
        let _dataset = Dataset::write(Box::new(reader), &table_uri, Some(write_params))
            .await
            .map_err(|e| Error::IO {
                source: box_error(std::io::Error::other(format!(
                    "Failed to write dataset: {}",
                    e
                ))),
                location: location!(),
            })?;

        // Register in manifest (store dir_name, not full URI)
        self.insert_into_manifest_with_metadata(
            object_id,
            ObjectType::Table,
            Some(dir_name),
            metadata,
            None,
            extended_batch,
        )
        .await?;

        Ok(CreateTableResponse {
            version: Some(1),
            location: Some(table_uri),
            storage_options: self.storage_options.clone(),
            properties: request.properties.clone(),
            ..Default::default()
        })
    }

    async fn drop_table(&self, request: DropTableRequest) -> Result<DropTableResponse> {
        let table_id = request.id.as_ref().ok_or_else(|| Error::InvalidInput {
            source: "Table ID is required".into(),
            location: location!(),
        })?;

        if table_id.is_empty() {
            return Err(Error::InvalidInput {
                source: "Table ID cannot be empty".into(),
                location: location!(),
            });
        }

        let (namespace, table_name) = Self::split_object_id(table_id);
        let object_id = Self::build_object_id(&namespace, &table_name);

        // Query manifest for table location
        let table_info = self.query_manifest_for_table(&object_id).await?;

        match table_info {
            Some(info) => {
                // Delete from manifest first
                self.delete_from_manifest(&object_id).await?;

                // Delete physical data directory using the dir_name from manifest
                let table_path = self.base_path.child(info.location.as_str());
                let table_uri = Self::construct_full_uri(&self.root, &info.location)?;

                // Remove the table directory
                self.object_store
                    .remove_dir_all(table_path)
                    .await
                    .map_err(|e| Error::Namespace {
                        source: format!("Failed to delete table directory: {}", e).into(),
                        location: location!(),
                    })?;

                Ok(DropTableResponse {
                    id: request.id.clone(),
                    location: Some(table_uri),
                    ..Default::default()
                })
            }
            None => Err(Error::Namespace {
                source: format!("Table '{}' not found", table_name).into(),
                location: location!(),
            }),
        }
    }

    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        let parent_namespace = request.id.as_ref().ok_or_else(|| Error::InvalidInput {
            source: "Namespace ID is required".into(),
            location: location!(),
        })?;

        // Build filter to find direct child namespaces
        let filter = if parent_namespace.is_empty() {
            // Root namespace: find all namespaces without a parent
            "object_type = 'namespace' AND NOT contains(object_id, '$')".to_string()
        } else {
            // Non-root: find namespaces that start with parent$ but have no additional $
            let prefix = parent_namespace.join(DELIMITER);
            format!(
                "object_type = 'namespace' AND starts_with(object_id, '{}{}') AND NOT contains(substring(object_id, {}), '$')",
                prefix, DELIMITER, prefix.len() + 2
            )
        };

        let mut scanner = self.manifest_scanner().await?;
        scanner.filter(&filter).map_err(|e| Error::IO {
            source: box_error(std::io::Error::other(format!("Failed to filter: {}", e))),
            location: location!(),
        })?;
        scanner.project(&["object_id"]).map_err(|e| Error::IO {
            source: box_error(std::io::Error::other(format!("Failed to project: {}", e))),
            location: location!(),
        })?;

        let batches = Self::execute_scanner(scanner).await?;
        let mut namespaces = Vec::new();

        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }

            let object_id_array = Self::get_string_column(&batch, "object_id")?;
            for i in 0..batch.num_rows() {
                let object_id = object_id_array.value(i);
                let (_namespace, name) = Self::parse_object_id(object_id);
                namespaces.push(name);
            }
        }

        Ok(ListNamespacesResponse::new(namespaces))
    }

    async fn describe_namespace(
        &self,
        request: DescribeNamespaceRequest,
    ) -> Result<DescribeNamespaceResponse> {
        let namespace_id = request.id.as_ref().ok_or_else(|| Error::InvalidInput {
            source: "Namespace ID is required".into(),
            location: location!(),
        })?;

        // Root namespace always exists
        if namespace_id.is_empty() {
            #[allow(clippy::needless_update)]
            return Ok(DescribeNamespaceResponse {
                properties: Some(HashMap::new()),
                ..Default::default()
            });
        }

        // Check if namespace exists in manifest
        let object_id = namespace_id.join(DELIMITER);
        let namespace_info = self.query_manifest_for_namespace(&object_id).await?;

        match namespace_info {
            #[allow(clippy::needless_update)]
            Some(info) => Ok(DescribeNamespaceResponse {
                properties: info.metadata,
                ..Default::default()
            }),
            None => Err(Error::Namespace {
                source: format!("Namespace '{}' not found", object_id).into(),
                location: location!(),
            }),
        }
    }

    async fn create_namespace(
        &self,
        request: CreateNamespaceRequest,
    ) -> Result<CreateNamespaceResponse> {
        let namespace_id = request.id.as_ref().ok_or_else(|| Error::InvalidInput {
            source: "Namespace ID is required".into(),
            location: location!(),
        })?;

        // Root namespace always exists and cannot be created
        if namespace_id.is_empty() {
            return Err(Error::Namespace {
                source: "Root namespace already exists and cannot be created".into(),
                location: location!(),
            });
        }

        // Validate parent namespaces exist (but not the namespace being created)
        if namespace_id.len() > 1 {
            self.validate_namespace_levels_exist(&namespace_id[..namespace_id.len() - 1])
                .await?;
        }

        let object_id = namespace_id.join(DELIMITER);
        if self.manifest_contains_object(&object_id).await? {
            return Err(Error::Namespace {
                source: format!("Namespace '{}' already exists", object_id).into(),
                location: location!(),
            });
        }

        // Serialize properties and compute extended batch if provided
        let metadata = Self::build_metadata_json(&request.properties);

        let extended_batch = if let Some(props) = &request.properties {
            batch_from_extended_props(props, &self.full_manifest_schema().await?)?
        } else {
            None
        };

        self.insert_into_manifest_with_metadata(
            object_id,
            ObjectType::Namespace,
            None,
            metadata,
            None,
            extended_batch,
        )
        .await?;

        Ok(CreateNamespaceResponse {
            properties: request.properties,
            ..Default::default()
        })
    }

    async fn drop_namespace(&self, request: DropNamespaceRequest) -> Result<DropNamespaceResponse> {
        let namespace_id = request.id.as_ref().ok_or_else(|| Error::InvalidInput {
            source: "Namespace ID is required".into(),
            location: location!(),
        })?;

        // Root namespace always exists and cannot be dropped
        if namespace_id.is_empty() {
            return Err(Error::Namespace {
                source: "Root namespace cannot be dropped".into(),
                location: location!(),
            });
        }

        let object_id = namespace_id.join(DELIMITER);

        // Check if namespace exists
        if !self.manifest_contains_object(&object_id).await? {
            return Err(Error::Namespace {
                source: format!("Namespace '{}' not found", object_id).into(),
                location: location!(),
            });
        }

        // Check for child namespaces
        let prefix = format!("{}{}", object_id, DELIMITER);
        let filter = format!("starts_with(object_id, '{}')", prefix);
        let mut scanner = self.manifest_scanner().await?;
        scanner.filter(&filter).map_err(|e| Error::IO {
            source: box_error(std::io::Error::other(format!("Failed to filter: {}", e))),
            location: location!(),
        })?;
        scanner.project::<&str>(&[]).map_err(|e| Error::IO {
            source: box_error(std::io::Error::other(format!("Failed to project: {}", e))),
            location: location!(),
        })?;
        scanner.with_row_id();
        let count = scanner.count_rows().await.map_err(|e| Error::IO {
            source: box_error(std::io::Error::other(format!(
                "Failed to count rows: {}",
                e
            ))),
            location: location!(),
        })?;

        if count > 0 {
            return Err(Error::Namespace {
                source: format!(
                    "Namespace '{}' is not empty (contains {} child objects)",
                    object_id, count
                )
                .into(),
                location: location!(),
            });
        }

        self.delete_from_manifest(&object_id).await?;

        Ok(DropNamespaceResponse::default())
    }

    async fn namespace_exists(&self, request: NamespaceExistsRequest) -> Result<()> {
        let namespace_id = request.id.as_ref().ok_or_else(|| Error::InvalidInput {
            source: "Namespace ID is required".into(),
            location: location!(),
        })?;

        // Root namespace always exists
        if namespace_id.is_empty() {
            return Ok(());
        }

        let object_id = namespace_id.join(DELIMITER);
        if self.manifest_contains_object(&object_id).await? {
            Ok(())
        } else {
            Err(Error::Namespace {
                source: format!("Namespace '{}' not found", object_id).into(),
                location: location!(),
            })
        }
    }

    async fn create_empty_table(
        &self,
        request: CreateEmptyTableRequest,
    ) -> Result<CreateEmptyTableResponse> {
        let table_id = request.id.as_ref().ok_or_else(|| Error::InvalidInput {
            source: "Table ID is required".into(),
            location: location!(),
        })?;

        if table_id.is_empty() {
            return Err(Error::InvalidInput {
                source: "Table ID cannot be empty".into(),
                location: location!(),
            });
        }

        let (namespace, table_name) = Self::split_object_id(table_id);
        let object_id = Self::build_object_id(&namespace, &table_name);

        // Serialize properties and compute extended batch if provided
        let metadata = Self::build_metadata_json(&request.properties);

        let extended_batch = if let Some(props) = &request.properties {
            batch_from_extended_props(props, &self.full_manifest_schema().await?)?
        } else {
            None
        };

        // Check if table already exists in manifest
        let existing = self.query_manifest_for_table(&object_id).await?;
        if existing.is_some() {
            return Err(Error::Namespace {
                source: format!("Table '{}' already exists", table_name).into(),
                location: location!(),
            });
        }

        // Create table location path with hash-based naming
        // When dir_listing_enabled is true and it's a root table, use directory-style naming: {table_name}.lance
        // Otherwise, use hash-based naming: {hash}_{object_id}
        let dir_name = if namespace.is_empty() && self.dir_listing_enabled {
            // Root table with directory listing enabled: use {table_name}.lance
            format!("{}.lance", table_name)
        } else {
            // Child namespace table or dir listing disabled: use hash-based naming
            Self::generate_dir_name(&object_id)
        };
        let table_path = self.base_path.child(dir_name.as_str());
        let table_uri = Self::construct_full_uri(&self.root, &dir_name)?;

        // Validate location if provided
        if let Some(req_location) = &request.location {
            let req_location = req_location.trim_end_matches('/');
            if req_location != table_uri {
                return Err(Error::Namespace {
                    source: format!(
                        "Cannot create table {} at location {}, must be at location {}",
                        table_name, req_location, table_uri
                    )
                    .into(),
                    location: location!(),
                });
            }
        }

        // Create the .lance-reserved file to mark the table as existing
        let reserved_file_path = table_path.child(".lance-reserved");

        self.object_store
            .create(&reserved_file_path)
            .await
            .map_err(|e| Error::Namespace {
                source: format!(
                    "Failed to create .lance-reserved file for table {}: {}",
                    table_name, e
                )
                .into(),
                location: location!(),
            })?
            .shutdown()
            .await
            .map_err(|e| Error::Namespace {
                source: format!(
                    "Failed to finalize .lance-reserved file for table {}: {}",
                    table_name, e
                )
                .into(),
                location: location!(),
            })?;

        // Add entry to manifest marking this as an empty table (store dir_name, not full path)
        self.insert_into_manifest_with_metadata(
            object_id,
            ObjectType::Table,
            Some(dir_name),
            metadata,
            None,
            extended_batch,
        )
        .await?;

        log::info!(
            "Created empty table '{}' in manifest at {}",
            table_name,
            table_uri
        );

        // For backwards compatibility, only skip vending credentials when explicitly set to false
        let vend_credentials = request.vend_credentials.unwrap_or(true);
        let storage_options = if vend_credentials {
            self.storage_options.clone()
        } else {
            None
        };

        Ok(CreateEmptyTableResponse {
            location: Some(table_uri),
            storage_options,
            properties: request.properties,
            ..Default::default()
        })
    }

    async fn declare_table(&self, request: DeclareTableRequest) -> Result<DeclareTableResponse> {
        let extended_batch = if let Some(props) = &request.properties {
            batch_from_extended_props(props, &self.full_manifest_schema().await?)?
        } else {
            None
        };
        self.declare_table_extended(request, extended_batch).await
    }

    async fn register_table(&self, request: RegisterTableRequest) -> Result<RegisterTableResponse> {
        let table_id = request.id.as_ref().ok_or_else(|| Error::InvalidInput {
            source: "Table ID is required".into(),
            location: location!(),
        })?;

        if table_id.is_empty() {
            return Err(Error::InvalidInput {
                source: "Table ID cannot be empty".into(),
                location: location!(),
            });
        }

        let location = request.location.clone();

        // Validate that location is a relative path within the root directory
        // We don't allow absolute URIs or paths that escape the root
        if location.contains("://") {
            return Err(Error::InvalidInput {
                source: format!(
                    "Absolute URIs are not allowed for register_table. Location must be a relative path within the root directory: {}",
                    location
                ).into(),
                location: location!(),
            });
        }

        if location.starts_with('/') {
            return Err(Error::InvalidInput {
                source: format!(
                    "Absolute paths are not allowed for register_table. Location must be a relative path within the root directory: {}",
                    location
                ).into(),
                location: location!(),
            });
        }

        // Check for path traversal attempts
        if location.contains("..") {
            return Err(Error::InvalidInput {
                source: format!(
                    "Path traversal is not allowed. Location must be a relative path within the root directory: {}",
                    location
                ).into(),
                location: location!(),
            });
        }

        let (namespace, table_name) = Self::split_object_id(table_id);
        let object_id = Self::build_object_id(&namespace, &table_name);

        // Validate that parent namespaces exist (if not root)
        if !namespace.is_empty() {
            self.validate_namespace_levels_exist(&namespace).await?;
        }

        // Check if table already exists
        if self.manifest_contains_object(&object_id).await? {
            return Err(Error::Namespace {
                source: format!("Table '{}' already exists", object_id).into(),
                location: location!(),
            });
        }

        // Serialize properties and compute extended batch if provided
        let metadata = Self::build_metadata_json(&request.properties);

        let extended_batch = if let Some(props) = &request.properties {
            batch_from_extended_props(props, &self.full_manifest_schema().await?)?
        } else {
            None
        };

        // Register the table with its location in the manifest
        self.insert_into_manifest_with_metadata(
            object_id,
            ObjectType::Table,
            Some(location.clone()),
            metadata,
            None,
            extended_batch,
        )
        .await?;

        Ok(RegisterTableResponse {
            location: Some(location),
            properties: request.properties.clone(),
            ..Default::default()
        })
    }

    async fn deregister_table(
        &self,
        request: DeregisterTableRequest,
    ) -> Result<DeregisterTableResponse> {
        let table_id = request.id.as_ref().ok_or_else(|| Error::InvalidInput {
            source: "Table ID is required".into(),
            location: location!(),
        })?;

        if table_id.is_empty() {
            return Err(Error::InvalidInput {
                source: "Table ID cannot be empty".into(),
                location: location!(),
            });
        }

        let (namespace, table_name) = Self::split_object_id(table_id);
        let object_id = Self::build_object_id(&namespace, &table_name);

        // Get table info before deleting
        let table_info = self.query_manifest_for_table(&object_id).await?;

        let table_uri = match table_info {
            Some(info) => {
                // Delete from manifest only (leave physical data intact)
                self.delete_from_manifest(&object_id).await?;
                Self::construct_full_uri(&self.root, &info.location)?
            }
            None => {
                return Err(Error::Namespace {
                    source: format!("Table '{}' not found", object_id).into(),
                    location: location!(),
                });
            }
        };

        Ok(DeregisterTableResponse {
            id: request.id.clone(),
            location: Some(table_uri),
            ..Default::default()
        })
    }
}

/// Parse the first row of a RecordBatch into a HashMap, excluding specified columns.
fn batch_to_extended_props(batch: &RecordBatch) -> HashMap<String, String> {
    // Collect basic columns to excluded
    let basic_schema = ManifestNamespace::basic_manifest_schema();
    let mut excluded: Vec<&str> = vec![];
    for field in basic_schema.fields.iter() {
        excluded.push(field.name());
    }

    // Transform batch to properties
    let mut result = HashMap::new();

    if batch.num_rows() == 0 {
        return result;
    }

    for (i, field) in batch.schema().fields().iter().enumerate() {
        let col_name = field.name().to_string();
        if excluded.contains(&col_name.as_str()) {
            continue;
        }

        let array = batch.column(i);

        if array.is_null(0) {
            // skip null properties.
            continue;
        }

        let Ok(scalar) = ScalarValue::try_from_array(array.as_ref(), 0) else {
            continue;
        };

        let Ok(value_str) = scalar_to_str(&scalar) else {
            continue;
        };

        if let Some(value) = value_str {
            if !value.is_empty() {
                result.insert(format!("{}{}", EXTENDED_PREFIX, col_name), value);
            }
        }
    }

    result
}

/// Convert a HashMap into a RecordBatch, excluding specified columns.
fn batch_from_extended_props(
    map: &HashMap<String, String>,
    schema: &Schema,
) -> Result<Option<RecordBatch>> {
    // Collect basic columns to excluded
    let basic_schema = ManifestNamespace::basic_manifest_schema();
    let mut excluded: Vec<&str> = vec![];
    for field in basic_schema.fields.iter() {
        excluded.push(field.name());
    }

    fn is_nullish_extended_value(v: &str) -> bool {
        v.is_empty() || v.eq_ignore_ascii_case("null")
    }

    // All non-null extended properties must be covered in schema.
    for (k, v) in map.iter() {
        if is_nullish_extended_value(v) {
            continue;
        }
        if let Some(col_name) = k.strip_prefix(EXTENDED_PREFIX) {
            if !excluded.contains(&col_name) && schema.column_with_name(col_name).is_none() {
                return Err(Error::InvalidInput {
                    source: format!("Column {} does not exist in extended properties", col_name)
                        .into(),
                    location: location!(),
                });
            }
        }
    }

    // Construct record batch
    let mut array: Vec<ArrayRef> = vec![];
    let mut fields: Vec<FieldRef> = vec![];
    for field in schema
        .fields()
        .iter()
        .filter(|field| !excluded.contains(&field.name().as_str()))
    {
        let field_name = field.name().as_str();

        match map.get(&format!("{}{}", EXTENDED_PREFIX, field_name)) {
            Some(value) if !is_nullish_extended_value(value) => {
                let scalar = scalar_from_str(field.data_type(), value)?;
                let v = scalar.to_array().map_err(|e| Error::IO {
                    source: box_error(std::io::Error::other(format!(
                        "Failed to convert scalar for column '{}' to array: {}",
                        field_name, e
                    ))),
                    location: location!(),
                })?;
                array.push(v);
                fields.push(field.clone());
            }
            _ => {}
        }
    }

    if fields.is_empty() {
        return Ok(None);
    }

    let schema = Schema::new(fields);
    Ok(Some(RecordBatch::try_new(Arc::new(schema), array)?))
}

pub(crate) fn scalar_to_str(scalar: &ScalarValue) -> Result<Option<String>> {
    if scalar.is_null() {
        return Ok(None);
    }

    match scalar {
        ScalarValue::Utf8(Some(v))
        | ScalarValue::Utf8View(Some(v))
        | ScalarValue::LargeUtf8(Some(v)) => Ok(Some(v.clone())),
        ScalarValue::Boolean(Some(v)) => Ok(Some(v.to_string())),
        ScalarValue::Int32(Some(v)) => Ok(Some(v.to_string())),
        ScalarValue::Int64(Some(v)) => Ok(Some(v.to_string())),
        ScalarValue::UInt32(Some(v)) => Ok(Some(v.to_string())),
        ScalarValue::UInt64(Some(v)) => Ok(Some(v.to_string())),
        ScalarValue::Float32(Some(v)) => Ok(Some(v.to_string())),
        ScalarValue::Float64(Some(v)) => Ok(Some(v.to_string())),
        ScalarValue::Date32(Some(v)) => Ok(Some(v.to_string())),
        ScalarValue::Date64(Some(v)) => Ok(Some(v.to_string())),
        ScalarValue::Binary(Some(v))
        | ScalarValue::LargeBinary(Some(v))
        | ScalarValue::BinaryView(Some(v))
        | ScalarValue::FixedSizeBinary(_, Some(v)) => Ok(Some(bytes_to_hex(v))),
        _ => Err(Error::InvalidInput {
            source: format!("Unsupported extended scalar: {:?}", scalar).into(),
            location: location!(),
        }),
    }
}

pub(crate) fn scalar_from_str(dt: &DataType, value: &str) -> Result<ScalarValue> {
    match dt {
        DataType::Utf8 => Ok(ScalarValue::Utf8(Some(value.to_string()))),
        DataType::LargeUtf8 => Ok(ScalarValue::LargeUtf8(Some(value.to_string()))),
        DataType::Boolean => Ok(ScalarValue::Boolean(Some(bool::from_str(value).map_err(
            |e| Error::InvalidInput {
                source: format!("Invalid boolean '{}': {}", value, e).into(),
                location: location!(),
            },
        )?))),
        DataType::Int32 => Ok(ScalarValue::Int32(Some(i32::from_str(value).map_err(
            |e| Error::InvalidInput {
                source: format!("Invalid int32 '{}': {}", value, e).into(),
                location: location!(),
            },
        )?))),
        DataType::Int64 => Ok(ScalarValue::Int64(Some(i64::from_str(value).map_err(
            |e| Error::InvalidInput {
                source: format!("Invalid int64 '{}': {}", value, e).into(),
                location: location!(),
            },
        )?))),
        DataType::UInt32 => Ok(ScalarValue::UInt32(Some(u32::from_str(value).map_err(
            |e| Error::InvalidInput {
                source: format!("Invalid uint32 '{}': {}", value, e).into(),
                location: location!(),
            },
        )?))),
        DataType::UInt64 => Ok(ScalarValue::UInt64(Some(u64::from_str(value).map_err(
            |e| Error::InvalidInput {
                source: format!("Invalid uint64 '{}': {}", value, e).into(),
                location: location!(),
            },
        )?))),
        DataType::Float32 => Ok(ScalarValue::Float32(Some(f32::from_str(value).map_err(
            |e| Error::InvalidInput {
                source: format!("Invalid float32 '{}': {}", value, e).into(),
                location: location!(),
            },
        )?))),
        DataType::Float64 => Ok(ScalarValue::Float64(Some(f64::from_str(value).map_err(
            |e| Error::InvalidInput {
                source: format!("Invalid float64 '{}': {}", value, e).into(),
                location: location!(),
            },
        )?))),
        DataType::Date32 => Ok(ScalarValue::Date32(Some(i32::from_str(value).map_err(
            |e| Error::InvalidInput {
                source: format!("Invalid date32 '{}': {}", value, e).into(),
                location: location!(),
            },
        )?))),
        DataType::Date64 => Ok(ScalarValue::Date64(Some(i64::from_str(value).map_err(
            |e| Error::InvalidInput {
                source: format!("Invalid date64 '{}': {}", value, e).into(),
                location: location!(),
            },
        )?))),
        DataType::Binary => Ok(ScalarValue::Binary(Some(hex_to_bytes(value)?))),
        DataType::LargeBinary => Ok(ScalarValue::LargeBinary(Some(hex_to_bytes(value)?))),
        _ => Err(Error::InvalidInput {
            source: format!("Unsupported extended column type: {:?}", dt).into(),
            location: location!(),
        }),
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(&mut out, "{:02x}", b);
    }
    out
}

fn hex_to_bytes(s: &str) -> Result<Vec<u8>> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if !s.len().is_multiple_of(2) {
        return Err(Error::InvalidInput {
            source: format!("Invalid hex string length {}", s.len()).into(),
            location: location!(),
        });
    }

    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    for i in (0..bytes.len()).step_by(2) {
        let hex = std::str::from_utf8(&bytes[i..i + 2]).map_err(|e| Error::InvalidInput {
            source: format!("Invalid hex string encoding: {}", e).into(),
            location: location!(),
        })?;
        let v = u8::from_str_radix(hex, 16).map_err(|e| Error::InvalidInput {
            source: format!("Invalid hex byte '{}': {}", hex, e).into(),
            location: location!(),
        })?;
        out.push(v);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use crate::dir::manifest::batch_to_extended_props;
    use crate::dir::manifest_ext::{CreateMultiNamespacesRequestBuilder, ManifestNamespaceExt};
    use crate::{DirectoryNamespaceBuilder, ManifestNamespace};
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::DataType;
    use bytes::Bytes;
    use lance_core::utils::tempfile::TempStdDir;
    use lance_io::object_store::ObjectStore;
    use lance_namespace::models::{
        CreateEmptyTableRequest, CreateNamespaceRequest, CreateTableRequest,
        DescribeNamespaceRequest, DescribeTableRequest, DropNamespaceRequest, DropTableRequest,
        ListNamespacesRequest, ListTablesRequest, NamespaceExistsRequest, RegisterTableRequest,
        TableExistsRequest,
    };
    use lance_namespace::LanceNamespace;
    use lance_namespace_reqwest_client::models::CreateNamespaceRequest as ClientCreateNamespaceRequest;
    use lance_namespace_reqwest_client::models::DeclareTableRequest;
    use rstest::rstest;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_ipc_data() -> Vec<u8> {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::ipc::writer::StreamWriter;
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        buffer
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_manifest_namespace_basic_create_and_list(#[case] inline_optimization: bool) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        // Create a DirectoryNamespace with manifest enabled (default)
        let dir_namespace = DirectoryNamespaceBuilder::new(temp_path)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();

        // Verify we can list tables (should be empty)
        let mut request = ListTablesRequest::new();
        request.id = Some(vec![]);
        let response = dir_namespace.list_tables(request).await.unwrap();
        assert_eq!(response.tables.len(), 0);

        // Create a test table
        let buffer = create_test_ipc_data();
        let mut create_request = CreateTableRequest::new();
        create_request.id = Some(vec!["test_table".to_string()]);

        let _response = dir_namespace
            .create_table(create_request, Bytes::from(buffer))
            .await
            .unwrap();

        // List tables again - should see our new table
        let mut request = ListTablesRequest::new();
        request.id = Some(vec![]);
        let response = dir_namespace.list_tables(request).await.unwrap();
        assert_eq!(response.tables.len(), 1);
        assert_eq!(response.tables[0], "test_table");
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_manifest_namespace_table_exists(#[case] inline_optimization: bool) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let dir_namespace = DirectoryNamespaceBuilder::new(temp_path)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();

        // Check non-existent table
        let mut request = TableExistsRequest::new();
        request.id = Some(vec!["nonexistent".to_string()]);
        let result = dir_namespace.table_exists(request).await;
        assert!(result.is_err());

        // Create table
        let buffer = create_test_ipc_data();
        let mut create_request = CreateTableRequest::new();
        create_request.id = Some(vec!["test_table".to_string()]);
        dir_namespace
            .create_table(create_request, Bytes::from(buffer))
            .await
            .unwrap();

        // Check existing table
        let mut request = TableExistsRequest::new();
        request.id = Some(vec!["test_table".to_string()]);
        let result = dir_namespace.table_exists(request).await;
        assert!(result.is_ok());
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_manifest_namespace_describe_table(#[case] inline_optimization: bool) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let dir_namespace = DirectoryNamespaceBuilder::new(temp_path)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();

        // Describe non-existent table
        let mut request = DescribeTableRequest::new();
        request.id = Some(vec!["nonexistent".to_string()]);
        let result = dir_namespace.describe_table(request).await;
        assert!(result.is_err());

        // Create table
        let buffer = create_test_ipc_data();
        let mut create_request = CreateTableRequest::new();
        create_request.id = Some(vec!["test_table".to_string()]);
        dir_namespace
            .create_table(create_request, Bytes::from(buffer))
            .await
            .unwrap();

        // Describe existing table
        let mut request = DescribeTableRequest::new();
        request.id = Some(vec!["test_table".to_string()]);
        let response = dir_namespace.describe_table(request).await.unwrap();
        assert!(response.location.is_some());
        assert!(response.location.unwrap().contains("test_table"));
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_manifest_namespace_drop_table(#[case] inline_optimization: bool) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let dir_namespace = DirectoryNamespaceBuilder::new(temp_path)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();

        // Create table
        let buffer = create_test_ipc_data();
        let mut create_request = CreateTableRequest::new();
        create_request.id = Some(vec!["test_table".to_string()]);
        dir_namespace
            .create_table(create_request, Bytes::from(buffer))
            .await
            .unwrap();

        // Verify table exists
        let mut request = ListTablesRequest::new();
        request.id = Some(vec![]);
        let response = dir_namespace.list_tables(request).await.unwrap();
        assert_eq!(response.tables.len(), 1);

        // Drop table
        let mut drop_request = DropTableRequest::new();
        drop_request.id = Some(vec!["test_table".to_string()]);
        let _response = dir_namespace.drop_table(drop_request).await.unwrap();

        // Verify table is gone
        let mut request = ListTablesRequest::new();
        request.id = Some(vec![]);
        let response = dir_namespace.list_tables(request).await.unwrap();
        assert_eq!(response.tables.len(), 0);
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_manifest_namespace_multiple_tables(#[case] inline_optimization: bool) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let dir_namespace = DirectoryNamespaceBuilder::new(temp_path)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();

        // Create multiple tables
        let buffer = create_test_ipc_data();
        for i in 1..=3 {
            let mut create_request = CreateTableRequest::new();
            create_request.id = Some(vec![format!("table{}", i)]);
            dir_namespace
                .create_table(create_request, Bytes::from(buffer.clone()))
                .await
                .unwrap();
        }

        // List all tables
        let mut request = ListTablesRequest::new();
        request.id = Some(vec![]);
        let response = dir_namespace.list_tables(request).await.unwrap();
        assert_eq!(response.tables.len(), 3);
        assert!(response.tables.contains(&"table1".to_string()));
        assert!(response.tables.contains(&"table2".to_string()));
        assert!(response.tables.contains(&"table3".to_string()));
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_directory_only_mode(#[case] inline_optimization: bool) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        // Create a DirectoryNamespace with manifest disabled
        let dir_namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(false)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();

        // Verify we can list tables (should be empty)
        let mut request = ListTablesRequest::new();
        request.id = Some(vec![]);
        let response = dir_namespace.list_tables(request).await.unwrap();
        assert_eq!(response.tables.len(), 0);

        // Create a test table
        let buffer = create_test_ipc_data();
        let mut create_request = CreateTableRequest::new();
        create_request.id = Some(vec!["test_table".to_string()]);

        // Create table - this should use directory-only mode
        let _response = dir_namespace
            .create_table(create_request, Bytes::from(buffer))
            .await
            .unwrap();

        // List tables - should see our new table
        let mut request = ListTablesRequest::new();
        request.id = Some(vec![]);
        let response = dir_namespace.list_tables(request).await.unwrap();
        assert_eq!(response.tables.len(), 1);
        assert_eq!(response.tables[0], "test_table");
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_dual_mode_merge(#[case] inline_optimization: bool) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        // Create a DirectoryNamespace with both manifest and directory enabled
        let dir_namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(true)
            .dir_listing_enabled(true)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();

        // Create tables through manifest
        let buffer = create_test_ipc_data();
        let mut create_request = CreateTableRequest::new();
        create_request.id = Some(vec!["table1".to_string()]);
        dir_namespace
            .create_table(create_request, Bytes::from(buffer))
            .await
            .unwrap();

        // List tables - should see table from both manifest and directory
        let mut request = ListTablesRequest::new();
        request.id = Some(vec![]);
        let response = dir_namespace.list_tables(request).await.unwrap();
        assert_eq!(response.tables.len(), 1);
        assert_eq!(response.tables[0], "table1");
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_manifest_only_mode(#[case] inline_optimization: bool) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        // Create a DirectoryNamespace with only manifest enabled
        let dir_namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(true)
            .dir_listing_enabled(false)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();

        // Create table
        let buffer = create_test_ipc_data();
        let mut create_request = CreateTableRequest::new();
        create_request.id = Some(vec!["test_table".to_string()]);
        dir_namespace
            .create_table(create_request, Bytes::from(buffer))
            .await
            .unwrap();

        // List tables - should only use manifest
        let mut request = ListTablesRequest::new();
        request.id = Some(vec![]);
        let response = dir_namespace.list_tables(request).await.unwrap();
        assert_eq!(response.tables.len(), 1);
        assert_eq!(response.tables[0], "test_table");
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_drop_nonexistent_table(#[case] inline_optimization: bool) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let dir_namespace = DirectoryNamespaceBuilder::new(temp_path)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();

        // Try to drop non-existent table
        let mut drop_request = DropTableRequest::new();
        drop_request.id = Some(vec!["nonexistent".to_string()]);
        let result = dir_namespace.drop_table(drop_request).await;
        assert!(result.is_err());
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_create_duplicate_table_fails(#[case] inline_optimization: bool) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let dir_namespace = DirectoryNamespaceBuilder::new(temp_path)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();

        // Create table
        let buffer = create_test_ipc_data();
        let mut create_request = CreateTableRequest::new();
        create_request.id = Some(vec!["test_table".to_string()]);
        dir_namespace
            .create_table(create_request, Bytes::from(buffer.clone()))
            .await
            .unwrap();

        // Try to create table with same name - should fail
        let mut create_request = CreateTableRequest::new();
        create_request.id = Some(vec!["test_table".to_string()]);
        let result = dir_namespace
            .create_table(create_request, Bytes::from(buffer))
            .await;
        assert!(result.is_err());
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_create_child_namespace(#[case] inline_optimization: bool) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let dir_namespace = DirectoryNamespaceBuilder::new(temp_path)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();

        // Create a child namespace
        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec!["ns1".to_string()]);
        let result = dir_namespace.create_namespace(create_req).await;
        assert!(
            result.is_ok(),
            "Failed to create child namespace: {:?}",
            result.err()
        );

        // Verify namespace exists
        let exists_req = NamespaceExistsRequest {
            id: Some(vec!["ns1".to_string()]),
            ..Default::default()
        };
        let result = dir_namespace.namespace_exists(exists_req).await;
        assert!(result.is_ok(), "Namespace should exist");

        // List child namespaces of root
        let list_req = ListNamespacesRequest {
            id: Some(vec![]),
            page_token: None,
            limit: None,
            ..Default::default()
        };
        let result = dir_namespace.list_namespaces(list_req).await;
        assert!(result.is_ok());
        let namespaces = result.unwrap();
        assert_eq!(namespaces.namespaces.len(), 1);
        assert_eq!(namespaces.namespaces[0], "ns1");
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_create_nested_namespace(#[case] inline_optimization: bool) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let dir_namespace = DirectoryNamespaceBuilder::new(temp_path)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();

        // Create parent namespace
        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec!["parent".to_string()]);
        dir_namespace.create_namespace(create_req).await.unwrap();

        // Create nested child namespace
        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec!["parent".to_string(), "child".to_string()]);
        let result = dir_namespace.create_namespace(create_req).await;
        assert!(
            result.is_ok(),
            "Failed to create nested namespace: {:?}",
            result.err()
        );

        // Verify nested namespace exists
        let exists_req = NamespaceExistsRequest {
            id: Some(vec!["parent".to_string(), "child".to_string()]),
            ..Default::default()
        };
        let result = dir_namespace.namespace_exists(exists_req).await;
        assert!(result.is_ok(), "Nested namespace should exist");

        // List child namespaces of parent
        let list_req = ListNamespacesRequest {
            id: Some(vec!["parent".to_string()]),
            page_token: None,
            limit: None,
            ..Default::default()
        };
        let result = dir_namespace.list_namespaces(list_req).await;
        assert!(result.is_ok());
        let namespaces = result.unwrap();
        assert_eq!(namespaces.namespaces.len(), 1);
        assert_eq!(namespaces.namespaces[0], "child");
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_create_namespace_without_parent_fails(#[case] inline_optimization: bool) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let dir_namespace = DirectoryNamespaceBuilder::new(temp_path)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();

        // Try to create nested namespace without parent
        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec!["nonexistent_parent".to_string(), "child".to_string()]);
        let result = dir_namespace.create_namespace(create_req).await;
        assert!(result.is_err(), "Should fail when parent doesn't exist");
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_drop_child_namespace(#[case] inline_optimization: bool) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let dir_namespace = DirectoryNamespaceBuilder::new(temp_path)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();

        // Create a child namespace
        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec!["ns1".to_string()]);
        dir_namespace.create_namespace(create_req).await.unwrap();

        // Drop the namespace
        let mut drop_req = DropNamespaceRequest::new();
        drop_req.id = Some(vec!["ns1".to_string()]);
        let result = dir_namespace.drop_namespace(drop_req).await;
        assert!(
            result.is_ok(),
            "Failed to drop namespace: {:?}",
            result.err()
        );

        // Verify namespace no longer exists
        let exists_req = NamespaceExistsRequest {
            id: Some(vec!["ns1".to_string()]),
            ..Default::default()
        };
        let result = dir_namespace.namespace_exists(exists_req).await;
        assert!(result.is_err(), "Namespace should not exist after drop");
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_drop_namespace_with_children_fails(#[case] inline_optimization: bool) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let dir_namespace = DirectoryNamespaceBuilder::new(temp_path)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();

        // Create parent and child namespaces
        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec!["parent".to_string()]);
        dir_namespace.create_namespace(create_req).await.unwrap();

        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec!["parent".to_string(), "child".to_string()]);
        dir_namespace.create_namespace(create_req).await.unwrap();

        // Try to drop parent namespace - should fail because it has children
        let mut drop_req = DropNamespaceRequest::new();
        drop_req.id = Some(vec!["parent".to_string()]);
        let result = dir_namespace.drop_namespace(drop_req).await;
        assert!(result.is_err(), "Should fail when namespace has children");
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_create_table_in_child_namespace(#[case] inline_optimization: bool) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let dir_namespace = DirectoryNamespaceBuilder::new(temp_path)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();

        // Create a child namespace
        let mut create_ns_req = CreateNamespaceRequest::new();
        create_ns_req.id = Some(vec!["ns1".to_string()]);
        dir_namespace.create_namespace(create_ns_req).await.unwrap();

        // Create a table in the child namespace
        let buffer = create_test_ipc_data();
        let mut create_table_req = CreateTableRequest::new();
        create_table_req.id = Some(vec!["ns1".to_string(), "table1".to_string()]);
        let result = dir_namespace
            .create_table(create_table_req, Bytes::from(buffer))
            .await;
        assert!(
            result.is_ok(),
            "Failed to create table in child namespace: {:?}",
            result.err()
        );

        // List tables in the namespace
        let list_req = ListTablesRequest {
            id: Some(vec!["ns1".to_string()]),
            page_token: None,
            limit: None,
            ..Default::default()
        };
        let result = dir_namespace.list_tables(list_req).await;
        assert!(result.is_ok());
        let tables = result.unwrap();
        assert_eq!(tables.tables.len(), 1);
        assert_eq!(tables.tables[0], "table1");
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_describe_child_namespace(#[case] inline_optimization: bool) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let dir_namespace = DirectoryNamespaceBuilder::new(temp_path)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();

        // Create a child namespace with properties
        let mut properties = std::collections::HashMap::new();
        properties.insert("key1".to_string(), "value1".to_string());

        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec!["ns1".to_string()]);
        create_req.properties = Some(properties.clone());
        dir_namespace.create_namespace(create_req).await.unwrap();

        // Describe the namespace
        let describe_req = DescribeNamespaceRequest {
            id: Some(vec!["ns1".to_string()]),
            ..Default::default()
        };
        let result = dir_namespace.describe_namespace(describe_req).await;
        assert!(
            result.is_ok(),
            "Failed to describe namespace: {:?}",
            result.err()
        );
        let response = result.unwrap();
        assert!(response.properties.is_some());
        assert_eq!(
            response.properties.unwrap().get("key1"),
            Some(&"value1".to_string())
        );
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_add_extended_properties_creates_columns_and_idempotent(
        #[case] inline_optimization: bool,
    ) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let manifest_ns = create_manifest_namespace_for_test(temp_path, inline_optimization).await;
        let schema = manifest_ns.full_manifest_schema().await.unwrap();
        assert_eq!(
            ManifestNamespace::basic_manifest_schema().fields().len(),
            schema.fields().len()
        );

        // Adding extended properties should create new columns
        manifest_ns
            .add_extended_properties(&vec![
                ("lance.manifest.extended.user_id", DataType::Utf8),
                ("lance.manifest.extended.score", DataType::Int32),
            ])
            .await
            .unwrap();

        let schema = manifest_ns.full_manifest_schema().await.unwrap();
        let user_field = schema.field_with_name("user_id").unwrap();
        assert_eq!(user_field.data_type(), &DataType::Utf8);
        let score_field = schema.field_with_name("score").unwrap();
        assert_eq!(score_field.data_type(), &DataType::Int32);
        let initial_field_count = schema.fields().len();

        // Adding the same properties again should be a no-op
        manifest_ns
            .add_extended_properties(&vec![
                ("lance.manifest.extended.user_id", DataType::Utf8),
                ("lance.manifest.extended.score", DataType::Int32),
            ])
            .await
            .unwrap();
        let schema_after = manifest_ns.full_manifest_schema().await.unwrap();
        assert_eq!(schema_after.fields().len(), initial_field_count);
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_add_extended_properties_rejects_missing_prefix(
        #[case] inline_optimization: bool,
    ) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let manifest_ns = create_manifest_namespace_for_test(temp_path, inline_optimization).await;
        let result = manifest_ns
            .add_extended_properties(&vec![("invalid_key", DataType::Utf8)])
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("must start with prefix"));
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_remove_extended_properties_drops_specified_columns(
        #[case] inline_optimization: bool,
    ) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let mut manifest_ns =
            create_manifest_namespace_for_test(temp_path, inline_optimization).await;
        manifest_ns
            .add_extended_properties(&vec![
                ("lance.manifest.extended.user_id", DataType::Utf8),
                ("lance.manifest.extended.group", DataType::Utf8),
            ])
            .await
            .unwrap();
        let schema = manifest_ns.full_manifest_schema().await.unwrap();
        assert!(schema.field_with_name("user_id").is_ok());
        assert!(schema.field_with_name("group").is_ok());

        manifest_ns
            .remove_extended_properties(&vec!["lance.manifest.extended.user_id"])
            .await
            .unwrap();
        let schema_after = manifest_ns.full_manifest_schema().await.unwrap();
        assert!(schema_after.field_with_name("user_id").is_err());
        assert!(schema_after.field_with_name("group").is_ok());

        // Remove non-existent property should be a no-op
        manifest_ns
            .remove_extended_properties(&vec!["lance.manifest.extended.user_id"])
            .await
            .unwrap();
        let schema_after = manifest_ns.full_manifest_schema().await.unwrap();
        assert!(schema_after.field_with_name("user_id").is_err());
        assert!(schema_after.field_with_name("group").is_ok());
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_remove_extended_properties_rejects_missing_prefix(
        #[case] inline_optimization: bool,
    ) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let mut manifest_ns =
            create_manifest_namespace_for_test(temp_path, inline_optimization).await;
        let result = manifest_ns
            .remove_extended_properties(&vec!["user_id"])
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("must start with prefix"));
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_create_namespace_with_extended_properties_without_columns_fails(
        #[case] inline_optimization: bool,
    ) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();

        let mut properties = std::collections::HashMap::new();
        properties.insert(
            "lance.manifest.extended.user_id".to_string(),
            "123".to_string(),
        );

        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec!["ns1".to_string()]);
        create_req.properties = Some(properties);

        let result = namespace.create_namespace(create_req).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Column user_id does not exist in extended properties"));
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_create_namespace_with_extended_properties_succeeds_and_describe_unified(
        #[case] inline_optimization: bool,
    ) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let manifest_ns = create_manifest_namespace_for_test(temp_path, inline_optimization).await;
        manifest_ns
            .add_extended_properties(&vec![("lance.manifest.extended.user_id", DataType::Utf8)])
            .await
            .unwrap();

        let mut properties = std::collections::HashMap::new();
        properties.insert("owner".to_string(), "alice".to_string());
        properties.insert(
            "lance.manifest.extended.user_id".to_string(),
            "123".to_string(),
        );
        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec!["ns1".to_string()]);
        create_req.properties = Some(properties);
        manifest_ns.create_namespace(create_req).await.unwrap();

        let describe_req = DescribeNamespaceRequest {
            id: Some(vec!["ns1".to_string()]),
            ..Default::default()
        };
        let response = manifest_ns.describe_namespace(describe_req).await.unwrap();
        let props = response.properties.expect("properties should be present");
        assert_eq!(props.get("owner"), Some(&"alice".to_string()));
        assert_eq!(
            props.get("lance.manifest.extended.user_id"),
            Some(&"123".to_string())
        );
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_extended_properties_null_and_empty_values_omitted(
        #[case] inline_optimization: bool,
    ) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let manifest_ns = create_manifest_namespace_for_test(temp_path, inline_optimization).await;
        manifest_ns
            .add_extended_properties(&vec![
                ("lance.manifest.extended.null_prop", DataType::Utf8),
                ("lance.manifest.extended.empty_prop", DataType::Utf8),
                ("lance.manifest.extended.non_existed", DataType::Utf8),
                ("lance.manifest.extended.valid_prop", DataType::Utf8),
            ])
            .await
            .unwrap();

        let mut properties = std::collections::HashMap::new();
        properties.insert("owner".to_string(), "alice".to_string());
        properties.insert(
            "lance.manifest.extended.null_prop".to_string(),
            "null".to_string(),
        );
        properties.insert(
            "lance.manifest.extended.empty_prop".to_string(),
            "".to_string(),
        );
        properties.insert(
            "lance.manifest.extended.valid_prop".to_string(),
            "42".to_string(),
        );
        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec!["ns1".to_string()]);
        create_req.properties = Some(properties);
        manifest_ns.create_namespace(create_req).await.unwrap();

        let describe_req = DescribeNamespaceRequest {
            id: Some(vec!["ns1".to_string()]),
            ..Default::default()
        };
        let response = manifest_ns.describe_namespace(describe_req).await.unwrap();
        let props = response.properties.expect("properties should be present");

        assert_eq!(props.get("owner"), Some(&"alice".to_string()));
        assert_eq!(
            props.get("lance.manifest.extended.valid_prop"),
            Some(&"42".to_string())
        );
        assert!(!props.contains_key("lance.manifest.extended.null_prop"));
        assert!(!props.contains_key("lance.manifest.extended.empty_prop"));
        assert!(!props.contains_key("lance.manifest.extended.non_existed"));
    }

    async fn create_manifest_namespace_for_test(
        root: &str,
        inline_optimization: bool,
    ) -> ManifestNamespace {
        let (object_store, base_path) = ObjectStore::from_uri(root).await.unwrap();
        ManifestNamespace::from_directory(
            root.to_string(),
            None,
            None,
            object_store,
            base_path,
            true,
            inline_optimization,
        )
        .await
        .unwrap()
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_create_multi_namespaces_extended_creates_nested_namespaces(
        #[case] inline_optimization: bool,
    ) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let manifest_ns = create_manifest_namespace_for_test(temp_path, inline_optimization).await;

        let mut props_a = HashMap::new();
        props_a.insert("owner".to_string(), "alice".to_string());
        let mut req_a = ClientCreateNamespaceRequest::new();
        req_a.id = Some(vec!["a".to_string()]);
        req_a.properties = Some(props_a);

        let mut props_ab = HashMap::new();
        props_ab.insert("owner".to_string(), "bob".to_string());
        let mut req_ab = ClientCreateNamespaceRequest::new();
        req_ab.id = Some(vec!["a".to_string(), "b".to_string()]);
        req_ab.properties = Some(props_ab);

        let request = CreateMultiNamespacesRequestBuilder::new()
            .namespaces(vec![req_a, req_ab])
            .build();
        manifest_ns
            .create_multi_namespaces_extended(request, vec![])
            .await
            .unwrap();

        // Parent namespace "a" should exist even though it was created in the same batch.
        for (object_id, expected_owner) in [("a".to_string(), "alice"), ("a$b".to_string(), "bob")]
        {
            let mut scanner = manifest_ns.manifest_scanner().await.unwrap();
            scanner
                .filter(&format!(
                    "object_type = 'namespace' AND object_id = '{}'",
                    object_id
                ))
                .unwrap();
            scanner.project(&["metadata"]).unwrap();
            let batches = ManifestNamespace::execute_scanner(scanner).await.unwrap();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 1);
            let batch = batches
                .into_iter()
                .find(|b| b.num_rows() > 0)
                .expect("expected a non-empty batch");
            let metadata_array = ManifestNamespace::get_string_column(&batch, "metadata").unwrap();
            let metadata_str = metadata_array.value(0);
            let metadata_map: HashMap<String, String> = serde_json::from_str(metadata_str).unwrap();
            assert_eq!(metadata_map.get("owner"), Some(&expected_owner.to_string()));
        }
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_create_multi_namespaces_extended_extended_record_overrides_properties(
        #[case] inline_optimization: bool,
    ) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let manifest_ns = create_manifest_namespace_for_test(temp_path, inline_optimization).await;
        manifest_ns
            .add_extended_properties(&vec![("lance.manifest.extended.user_id", DataType::Utf8)])
            .await
            .unwrap();

        let mut props_1 = HashMap::new();
        props_1.insert("owner".to_string(), "alice".to_string());
        props_1.insert(
            "lance.manifest.extended.user_id".to_string(),
            "111".to_string(),
        );
        let mut req_1 = ClientCreateNamespaceRequest::new();
        req_1.id = Some(vec!["ns1".to_string()]);
        req_1.properties = Some(props_1);

        let mut props_2 = HashMap::new();
        props_2.insert("owner".to_string(), "bob".to_string());
        props_2.insert(
            "lance.manifest.extended.user_id".to_string(),
            "222".to_string(),
        );
        let mut req_2 = ClientCreateNamespaceRequest::new();
        req_2.id = Some(vec!["ns2".to_string()]);
        req_2.properties = Some(props_2);

        let ext_schema = Arc::new(Schema::new(vec![Field::new(
            "user_id",
            DataType::Utf8,
            true,
        )]));
        let ext_batch_1 = RecordBatch::try_new(
            ext_schema,
            vec![Arc::new(StringArray::from(vec![Some("999")]))],
        )
        .unwrap();

        let request = CreateMultiNamespacesRequestBuilder::new()
            .namespaces(vec![req_1, req_2])
            .build();
        manifest_ns
            .create_multi_namespaces_extended(request, vec![Some(ext_batch_1), None])
            .await
            .unwrap();

        for (object_id, expected_user_id) in
            [("ns1".to_string(), "999"), ("ns2".to_string(), "222")]
        {
            let mut scanner = manifest_ns.manifest_scanner().await.unwrap();
            scanner
                .filter(&format!(
                    "object_type = 'namespace' AND object_id = '{}'",
                    object_id
                ))
                .unwrap();
            scanner.project(&["user_id"]).unwrap();
            let batches = ManifestNamespace::execute_scanner(scanner).await.unwrap();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 1);
            let batch = batches
                .into_iter()
                .find(|b| b.num_rows() > 0)
                .expect("expected a non-empty batch");
            let user_id_array = ManifestNamespace::get_string_column(&batch, "user_id").unwrap();
            assert_eq!(user_id_array.value(0), expected_user_id);
        }
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_declare_table_extended_writes_extended_record(#[case] inline_optimization: bool) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let manifest_ns = create_manifest_namespace_for_test(temp_path, inline_optimization).await;
        manifest_ns
            .add_extended_properties(&vec![
                ("lance.manifest.extended.user_id", DataType::Utf8),
                ("lance.manifest.extended.score", DataType::Int32),
            ])
            .await
            .unwrap();

        let ext_schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, true),
            Field::new("score", DataType::Int32, true),
        ]));
        let ext_batch = RecordBatch::try_new(
            ext_schema,
            vec![
                Arc::new(StringArray::from(vec![Some("u1")])),
                Arc::new(Int32Array::from(vec![Some(7)])),
            ],
        )
        .unwrap();

        let mut props = HashMap::new();
        props.insert("owner".to_string(), "alice".to_string());

        let mut declare_req = DeclareTableRequest::new();
        declare_req.id = Some(vec!["test_table".to_string()]);
        declare_req.properties = Some(props);

        let resp = manifest_ns
            .declare_table_extended(declare_req, Some(ext_batch))
            .await
            .unwrap();
        let resp_loc = resp.location.expect("response location should be present");
        assert!(resp_loc.ends_with("test_table.lance"));

        let mut scanner = manifest_ns.manifest_scanner().await.unwrap();
        scanner
            .filter("object_type = 'table' AND object_id = 'test_table'")
            .unwrap();
        scanner
            .project(&["metadata", "user_id", "score", "location"])
            .unwrap();
        let batches = ManifestNamespace::execute_scanner(scanner).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
        let batch = batches
            .into_iter()
            .find(|b| b.num_rows() > 0)
            .expect("expected a non-empty batch");

        let metadata_array = ManifestNamespace::get_string_column(&batch, "metadata").unwrap();
        let metadata_str = metadata_array.value(0);
        let metadata_map: HashMap<String, String> = serde_json::from_str(metadata_str).unwrap();
        assert_eq!(metadata_map.get("owner"), Some(&"alice".to_string()));

        let user_id_array = ManifestNamespace::get_string_column(&batch, "user_id").unwrap();
        assert_eq!(user_id_array.value(0), "u1");
        let score_array = batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(score_array.value(0), 7);

        let location_array = ManifestNamespace::get_string_column(&batch, "location").unwrap();
        assert!(location_array.value(0).ends_with("test_table.lance"));
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_create_table_with_properties_persisted(#[case] inline_optimization: bool) {
        let (_temp_dir, manifest_ns, properties) =
            create_manifest_and_persist_properties(inline_optimization).await;

        let buffer = create_test_ipc_data();
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        create_req.properties = Some(properties);

        manifest_ns
            .create_table(create_req, Bytes::from(buffer))
            .await
            .unwrap();
        verify_persist_properties(&manifest_ns, "test_table").await;
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_declare_table_with_properties_persisted(#[case] inline_optimization: bool) {
        let (_temp_dir, manifest_ns, properties) =
            create_manifest_and_persist_properties(inline_optimization).await;

        let mut declare_req = DeclareTableRequest::new();
        declare_req.id = Some(vec!["test_table".to_string()]);
        declare_req.properties = Some(properties);

        manifest_ns.declare_table(declare_req).await.unwrap();
        verify_persist_properties(&manifest_ns, "test_table").await;
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_register_table_with_properties_persisted(#[case] inline_optimization: bool) {
        let (_temp_dir, manifest_ns, properties) =
            create_manifest_and_persist_properties(inline_optimization).await;

        let mut register_req = RegisterTableRequest::new("registered_table.lance".to_string());
        register_req.id = Some(vec!["registered_table".to_string()]);
        register_req.properties = Some(properties);

        LanceNamespace::register_table(&manifest_ns, register_req)
            .await
            .unwrap();
        verify_persist_properties(&manifest_ns, "registered_table").await;
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_create_empty_table_persist_properties(#[case] inline_optimization: bool) {
        let (_temp_dir, manifest_ns, properties) =
            create_manifest_and_persist_properties(inline_optimization).await;

        let mut request = CreateEmptyTableRequest::new();
        request.id = Some(vec!["empty_table".to_string()]);
        request.properties = Some(properties);

        #[allow(deprecated)]
        manifest_ns.create_empty_table(request).await.unwrap();
        verify_persist_properties(&manifest_ns, "empty_table").await;
    }

    async fn create_manifest_and_persist_properties(
        inline_optimization: bool,
    ) -> (TempStdDir, ManifestNamespace, HashMap<String, String>) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let manifest_ns = create_manifest_namespace_for_test(temp_path, inline_optimization).await;

        manifest_ns
            .add_extended_properties(&vec![
                ("lance.manifest.extended.user_id", DataType::Utf8),
                ("lance.manifest.extended.score", DataType::Int32),
            ])
            .await
            .unwrap();

        let properties = std::collections::HashMap::from([
            ("owner".to_string(), "alice".to_string()),
            (
                "lance.manifest.extended.user_id".to_string(),
                "123".to_string(),
            ),
            (
                "lance.manifest.extended.score".to_string(),
                "42".to_string(),
            ),
        ]);

        (temp_dir, manifest_ns, properties)
    }

    async fn verify_persist_properties(manifest_ns: &ManifestNamespace, table: &str) {
        let object_id = ManifestNamespace::build_object_id(&[], table);
        let mut scanner = manifest_ns.manifest_scanner().await.unwrap();
        let filter = format!("object_id = '{}'", object_id);
        scanner.filter(&filter).unwrap();
        scanner.project(&["metadata", "user_id", "score"]).unwrap();
        let batches = ManifestNamespace::execute_scanner(scanner).await.unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);

        let metadata_array = ManifestNamespace::get_string_column(batch, "metadata").unwrap();
        let metadata_str = metadata_array.value(0);
        let metadata_map: std::collections::HashMap<String, String> =
            serde_json::from_str(metadata_str).unwrap();
        assert_eq!(metadata_map.get("owner"), Some(&"alice".to_string()));

        let user_id_array = ManifestNamespace::get_string_column(batch, "user_id").unwrap();
        assert_eq!(user_id_array.value(0), "123");

        let score_array = batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(score_array.value(0), 42);
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_create_table_with_extended_properties_without_columns_fails(
        #[case] inline_optimization: bool,
    ) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let manifest_ns = create_manifest_namespace_for_test(temp_path, inline_optimization).await;

        let mut properties = std::collections::HashMap::new();
        properties.insert(
            "lance.manifest.extended.user_id".to_string(),
            "123".to_string(),
        );

        let buffer = create_test_ipc_data();
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        create_req.properties = Some(properties);

        let result = manifest_ns
            .create_table(create_req, Bytes::from(buffer))
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Column user_id does not exist in extended properties"));
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_declare_table_with_extended_properties_without_columns_fails(
        #[case] inline_optimization: bool,
    ) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let manifest_ns = create_manifest_namespace_for_test(temp_path, inline_optimization).await;

        let mut properties = std::collections::HashMap::new();
        properties.insert(
            "lance.manifest.extended.user_id".to_string(),
            "123".to_string(),
        );

        let mut declare_req = DeclareTableRequest::new();
        declare_req.id = Some(vec!["test_table".to_string()]);
        declare_req.properties = Some(properties);

        let result = manifest_ns.declare_table(declare_req).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Column user_id does not exist in extended properties"));
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_register_table_with_extended_properties_without_columns_fails(
        #[case] inline_optimization: bool,
    ) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let manifest_ns = create_manifest_namespace_for_test(temp_path, inline_optimization).await;

        let mut properties = std::collections::HashMap::new();
        properties.insert(
            "lance.manifest.extended.user_id".to_string(),
            "123".to_string(),
        );

        let mut register_req = RegisterTableRequest::new("registered_table.lance".to_string());
        register_req.id = Some(vec!["registered_table".to_string()]);
        register_req.properties = Some(properties);

        let result = LanceNamespace::register_table(&manifest_ns, register_req).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Column user_id does not exist in extended properties"));
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_create_table_extended_properties_null_and_empty_values_omitted(
        #[case] inline_optimization: bool,
    ) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let manifest_ns = create_manifest_namespace_for_test(temp_path, inline_optimization).await;
        manifest_ns
            .add_extended_properties(&vec![
                ("lance.manifest.extended.null_prop", DataType::Utf8),
                ("lance.manifest.extended.empty_prop", DataType::Utf8),
                ("lance.manifest.extended.valid_prop", DataType::Utf8),
                ("lance.manifest.extended.non_existed_prop", DataType::Utf8),
            ])
            .await
            .unwrap();

        let mut properties = std::collections::HashMap::new();
        properties.insert("owner".to_string(), "alice".to_string());
        properties.insert(
            "lance.manifest.extended.null_prop".to_string(),
            "null".to_string(),
        );
        properties.insert(
            "lance.manifest.extended.empty_prop".to_string(),
            "".to_string(),
        );
        properties.insert(
            "lance.manifest.extended.valid_prop".to_string(),
            "42".to_string(),
        );

        let buffer = create_test_ipc_data();
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        create_req.properties = Some(properties);
        manifest_ns
            .create_table(create_req, Bytes::from(buffer))
            .await
            .unwrap();

        let object_id = ManifestNamespace::build_object_id(&[], "test_table");

        let mut scanner = manifest_ns.manifest_scanner().await.unwrap();
        let filter = format!("object_id = '{}'", object_id);
        scanner.filter(&filter).unwrap();
        let batches = ManifestNamespace::execute_scanner(scanner).await.unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);

        let extended_props = batch_to_extended_props(batch);

        assert_eq!(
            extended_props.get("lance.manifest.extended.valid_prop"),
            Some(&"42".to_string())
        );
        assert!(!extended_props.contains_key("lance.manifest.extended.null_prop"));
        assert!(!extended_props.contains_key("lance.manifest.extended.empty_prop"));
        assert!(!extended_props.contains_key("lance.manifest.extended.non_existed_prop"));
    }

    #[tokio::test]
    async fn test_describe_table_unifies_properties() {
        let (_temp_dir, manifest_ns, base_properties) = prepare_properties_env().await;

        // create_table scenario
        let buffer = create_test_ipc_data();
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["created_table".to_string()]);
        create_req.properties = Some(base_properties.clone());
        manifest_ns
            .create_table(create_req, Bytes::from(buffer))
            .await
            .unwrap();

        verify_describe_table_props(&manifest_ns, "created_table", Some(true), &base_properties)
            .await;
        verify_describe_table_props(&manifest_ns, "created_table", Some(false), &base_properties)
            .await;

        // declare_table scenario
        let mut declare_req = DeclareTableRequest::new();
        declare_req.id = Some(vec!["declared_table".to_string()]);
        declare_req.properties = Some(base_properties.clone());
        manifest_ns.declare_table(declare_req).await.unwrap();

        verify_describe_table_props(&manifest_ns, "declared_table", Some(true), &base_properties)
            .await;
        verify_describe_table_props(
            &manifest_ns,
            "declared_table",
            Some(false),
            &base_properties,
        )
        .await;

        // register_table scenario
        let mut register_req = RegisterTableRequest::new("registered_table.lance".to_string());
        register_req.id = Some(vec!["registered_table".to_string()]);
        register_req.properties = Some(base_properties.clone());
        LanceNamespace::register_table(&manifest_ns, register_req)
            .await
            .unwrap();

        verify_describe_table_props(
            &manifest_ns,
            "registered_table",
            Some(true),
            &base_properties,
        )
        .await;
        verify_describe_table_props(
            &manifest_ns,
            "registered_table",
            Some(false),
            &base_properties,
        )
        .await;
    }

    async fn prepare_properties_env() -> (
        TempStdDir,
        ManifestNamespace,
        std::collections::HashMap<String, String>,
    ) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let manifest_ns = create_manifest_namespace_for_test(temp_path, true).await;
        manifest_ns
            .add_extended_properties(&vec![
                ("lance.manifest.extended.user_id", DataType::Utf8),
                ("lance.manifest.extended.score", DataType::Int32),
            ])
            .await
            .unwrap();

        // prepare base properties
        let mut base_properties = std::collections::HashMap::new();
        base_properties.insert("owner".to_string(), "alice".to_string());
        base_properties.insert(
            "lance.manifest.extended.user_id".to_string(),
            "123".to_string(),
        );
        base_properties.insert(
            "lance.manifest.extended.score".to_string(),
            "42".to_string(),
        );

        (temp_dir, manifest_ns, base_properties)
    }

    async fn verify_describe_table_props(
        manifest_ns: &ManifestNamespace,
        table_name: &str,
        load_detailed_metadata: Option<bool>,
        base_properties: &std::collections::HashMap<String, String>,
    ) {
        let req = DescribeTableRequest {
            id: Some(vec![table_name.to_string()]),
            load_detailed_metadata,
            ..Default::default()
        };
        let response = manifest_ns.describe_table(req).await.unwrap();
        let props = response.properties.expect("properties should be present");
        for (k, v) in base_properties.iter() {
            assert_eq!(props.get(k), Some(v));
        }
    }

    #[test]
    fn test_construct_full_uri_with_cloud_urls() {
        // Test S3-style URL with nested path (no trailing slash)
        let s3_result =
            ManifestNamespace::construct_full_uri("s3://bucket/path/subdir", "table.lance")
                .unwrap();
        assert_eq!(
            s3_result, "s3://bucket/path/subdir/table.lance",
            "S3 URL should correctly append table name to nested path"
        );

        // Test Azure-style URL with nested path (no trailing slash)
        let az_result =
            ManifestNamespace::construct_full_uri("az://container/path/subdir", "table.lance")
                .unwrap();
        assert_eq!(
            az_result, "az://container/path/subdir/table.lance",
            "Azure URL should correctly append table name to nested path"
        );

        // Test GCS-style URL with nested path (no trailing slash)
        let gs_result =
            ManifestNamespace::construct_full_uri("gs://bucket/path/subdir", "table.lance")
                .unwrap();
        assert_eq!(
            gs_result, "gs://bucket/path/subdir/table.lance",
            "GCS URL should correctly append table name to nested path"
        );

        // Test with deeper nesting
        let deep_result =
            ManifestNamespace::construct_full_uri("s3://bucket/a/b/c/d", "my_table.lance").unwrap();
        assert_eq!(
            deep_result, "s3://bucket/a/b/c/d/my_table.lance",
            "Deeply nested path should work correctly"
        );

        // Test with root-level path (single segment after bucket)
        let shallow_result =
            ManifestNamespace::construct_full_uri("s3://bucket", "table.lance").unwrap();
        assert_eq!(
            shallow_result, "s3://bucket/table.lance",
            "Single-level nested path should work correctly"
        );

        // Test that URLs with trailing slash already work (no regression)
        let trailing_slash_result =
            ManifestNamespace::construct_full_uri("s3://bucket/path/subdir/", "table.lance")
                .unwrap();
        assert_eq!(
            trailing_slash_result, "s3://bucket/path/subdir/table.lance",
            "URL with existing trailing slash should still work"
        );
    }
}
