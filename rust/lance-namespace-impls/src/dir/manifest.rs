// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Manifest-based namespace implementation
//!
//! This module provides a namespace implementation that uses a manifest table
//! to track tables and nested namespaces.

use arrow::array::builder::{ListBuilder, StringBuilder};
use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchIterator, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{FieldRef, Schema};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{FutureExt, stream::StreamExt};
use lance::dataset::NewColumnTransform;
use lance::dataset::optimize::{CompactionOptions, compact_files};
use lance::dataset::{
    DeleteBuilder, MergeInsertBuilder, ReadParams, WhenMatched, WhenNotMatched, WriteParams,
    builder::DatasetBuilder,
};
use lance::deps::arrow_array::RecordBatchOptions;
use lance::deps::datafusion::scalar::ScalarValue;
use lance::index::DatasetIndexExt;
use lance::session::Session;
use lance::{Dataset, dataset::scanner::Scanner};
use lance_arrow::RecordBatchExt;
use lance_core::Error as LanceError;
use lance_core::datatypes::LANCE_UNENFORCED_PRIMARY_KEY_POSITION;
use lance_core::{Error, Result};
use lance_index::IndexType;
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_io::object_store::{ObjectStore, ObjectStoreParams};
use lance_namespace::LanceNamespace;
use lance_namespace::error::NamespaceError;
use lance_namespace::models::{
    CreateNamespaceRequest, CreateNamespaceResponse, CreateTableRequest, CreateTableResponse,
    DeclareTableRequest, DeclareTableResponse, DeregisterTableRequest, DeregisterTableResponse,
    DescribeNamespaceRequest, DescribeNamespaceResponse, DescribeTableRequest,
    DescribeTableResponse, DescribeTableVersionResponse, DropNamespaceRequest,
    DropNamespaceResponse, DropTableRequest, DropTableResponse, ListNamespacesRequest,
    ListNamespacesResponse, ListTableVersionsResponse, ListTablesRequest, ListTablesResponse,
    NamespaceExistsRequest, RegisterTableRequest, RegisterTableResponse, TableExistsRequest,
    TableVersion,
};
use lance_namespace::schema::arrow_schema_to_json;
use object_store::path::Path;
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
const DELIMITER: &str = "$";

/// Extended properties are special properties starting with `lance.manifest.extended.`.
/// They are stored as dedicated columns in the `__manifest` table.
///
/// Example:
/// ```json
/// {
///   "user_name": "Alice",
///   "lance.manifest.extended.user_id": "123456"
/// }
/// ```
/// `user_name` is stored in the `metadata` JSON column, while `user_id` is stored in a column
/// named `user_id`.
static EXTENDED_PREFIX: &str = "lance.manifest.extended.";

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
    TableVersion,
}

impl ObjectType {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Namespace => "namespace",
            Self::Table => "table",
            Self::TableVersion => "table_version",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "namespace" => Ok(Self::Namespace),
            "table" => Ok(Self::Table),
            "table_version" => Ok(Self::TableVersion),
            _ => Err(NamespaceError::Internal {
                message: format!("Invalid object type: {}", s),
            }
            .into()),
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

/// An entry to be inserted into the manifest table.
///
/// This struct makes the meaning of each field explicit, replacing the
/// previous tuple-based API `(String, ObjectType, Option<String>, Option<String>)`.
#[derive(Debug, Clone)]
pub struct ManifestEntry {
    /// The unique object identifier (e.g., table name or version object_id)
    pub object_id: String,
    /// The type of the object (Namespace, Table, or TableVersion)
    pub object_type: ObjectType,
    /// The storage location (e.g., directory name for tables)
    pub location: Option<String>,
    /// Additional metadata serialized as JSON
    pub metadata: Option<String>,
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
        let latest_version = read_guard.latest_version_id().await.map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to get latest version: {}", e),
            })
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
        let latest_version = write_guard.latest_version_id().await.map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to get latest version: {}", e),
            })
        })?;

        if latest_version != write_guard.version().version {
            write_guard.checkout_latest().await.map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!("Failed to checkout latest: {}", e),
                })
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

/// Manifest-based namespace implementation
///
/// Uses a special `__manifest` Lance table to track tables and nested namespaces.
pub struct ManifestNamespace {
    root: String,
    storage_options: Option<HashMap<String, String>>,
    #[allow(dead_code)]
    session: Option<Arc<Session>>,
    #[allow(dead_code)]
    object_store: Arc<ObjectStore>,
    #[allow(dead_code)]
    base_path: Path,
    manifest_dataset: DatasetConsistencyWrapper,
    /// Whether directory listing is enabled in dual mode
    /// If true, root namespace tables use {table_name}.lance naming
    /// If false, they use namespace-prefixed names
    dir_listing_enabled: bool,
    /// Whether to perform inline optimization (compaction and indexing) on the __manifest table
    /// after every write. Defaults to true.
    inline_optimization_enabled: bool,
    /// Number of retries for commit operations on the manifest table.
    /// If None, defaults to [`lance_table::io::commit::CommitConfig`] default (20).
    commit_retries: Option<u32>,
}

impl std::fmt::Debug for ManifestNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestNamespace")
            .field("root", &self.root)
            .field("storage_options", &self.storage_options)
            .field("dir_listing_enabled", &self.dir_listing_enabled)
            .field(
                "inline_optimization_enabled",
                &self.inline_optimization_enabled,
            )
            .finish()
    }
}

/// Convert a Lance commit error to an appropriate namespace error.
///
/// Maps lance commit errors to namespace errors:
/// - `CommitConflict`: version collision retries exhausted -> Throttled (safe to retry)
/// - `TooMuchWriteContention`: RetryableCommitConflict (semantic conflict) retries exhausted -> ConcurrentModification
/// - `IncompatibleTransaction`: incompatible concurrent change -> ConcurrentModification
/// - Errors containing "matched/duplicate/already exists": ConcurrentModification (from WhenMatched::Fail)
/// - Other errors: IO error with the operation description
fn convert_lance_commit_error(e: &LanceError, operation: &str, object_id: Option<&str>) -> Error {
    match e {
        // CommitConflict: version collision retries exhausted -> Throttled (safe to retry)
        LanceError::CommitConflict { .. } => NamespaceError::Throttled {
            message: format!("Too many concurrent writes, please retry later: {:?}", e),
        }
        .into(),
        // TooMuchWriteContention: RetryableCommitConflict (semantic conflict) retries exhausted -> ConcurrentModification
        // IncompatibleTransaction: incompatible concurrent change -> ConcurrentModification
        LanceError::TooMuchWriteContention { .. } | LanceError::IncompatibleTransaction { .. } => {
            let message = if let Some(id) = object_id {
                format!(
                    "Object '{}' was concurrently modified by another operation: {:?}",
                    id, e
                )
            } else {
                format!(
                    "Object was concurrently modified by another operation: {:?}",
                    e
                )
            };
            NamespaceError::ConcurrentModification { message }.into()
        }
        // Other errors: check message for semantic conflicts (matched/duplicate from WhenMatched::Fail)
        _ => {
            let error_msg = e.to_string();
            if error_msg.contains("matched")
                || error_msg.contains("duplicate")
                || error_msg.contains("already exists")
            {
                let message = if let Some(id) = object_id {
                    format!(
                        "Object '{}' was concurrently created by another operation: {:?}",
                        id, e
                    )
                } else {
                    format!(
                        "Object was concurrently created by another operation: {:?}",
                        e
                    )
                };
                return NamespaceError::ConcurrentModification { message }.into();
            }
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("{}: {:?}", operation, e),
            })
        }
    }
}

impl ManifestNamespace {
    /// Create a new ManifestNamespace from an existing DirectoryNamespace
    #[allow(clippy::too_many_arguments)]
    pub async fn from_directory(
        root: String,
        storage_options: Option<HashMap<String, String>>,
        session: Option<Arc<Session>>,
        object_store: Arc<ObjectStore>,
        base_path: Path,
        dir_listing_enabled: bool,
        inline_optimization_enabled: bool,
        commit_retries: Option<u32>,
        table_version_storage_enabled: bool,
    ) -> Result<Self> {
        let manifest_dataset = Self::ensure_manifest_table_up_to_date(
            &root,
            &storage_options,
            session.clone(),
            table_version_storage_enabled,
        )
        .await?;

        Ok(Self {
            root,
            storage_options,
            session,
            object_store,
            base_path,
            manifest_dataset,
            dir_listing_enabled,
            inline_optimization_enabled,
            commit_retries,
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

    /// Add extended properties to the __manifest table.
    ///
    /// Each property key must start with the prefix `lance.manifest.extended.`. The stored column name is the key
    /// with the prefix stripped.
    pub async fn add_extended_properties(&self, properties: &Vec<(&str, DataType)>) -> Result<()> {
        let full_schema = self.full_manifest_schema().await?;
        let fields: Vec<Field> = properties
            .iter()
            .map(|(name, data_type)| {
                if !name.starts_with(EXTENDED_PREFIX) {
                    return Err(Error::from(NamespaceError::InvalidInput {
                        message: format!(
                            "Extended properties key {} must start with prefix: {}",
                            name, EXTENDED_PREFIX
                        ),
                    }));
                }

                let field = Field::new(
                    name.strip_prefix(EXTENDED_PREFIX).unwrap().to_string(),
                    data_type.clone(),
                    true,
                );
                if full_schema.column_with_name(field.name()).is_some() {
                    return Err(Error::from(NamespaceError::InvalidInput {
                        message: format!("Field {} already exists in schema", field.name()),
                    }));
                }
                Ok(field)
            })
            .collect::<Result<Vec<_>>>()?;

        if fields.is_empty() {
            return Ok(());
        }

        let schema = Schema::new(fields);
        let transform = NewColumnTransform::AllNulls(Arc::new(schema));
        let mut ds = self.manifest_dataset.get_mut().await?;
        ds.add_columns(transform, None, None).await?;
        Ok(())
    }

    /// Get all extended properties keys currently present in the manifest schema.
    pub async fn get_extended_properties_keys(&self) -> Result<Vec<String>> {
        let basic_cols: HashSet<String> = Self::manifest_schema()
            .fields
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        let mut keys = vec![];
        for f in self.full_manifest_schema().await?.fields.iter() {
            if !basic_cols.contains(f.name().as_str()) {
                keys.push(format!("{}{}", EXTENDED_PREFIX, f.name()));
            }
        }
        Ok(keys)
    }

    /// Remove extended properties from the manifest table.
    ///
    /// Each property key must start with the prefix `lance.manifest.extended.`.
    pub async fn remove_extended_properties(&mut self, properties: &Vec<&str>) -> Result<()> {
        let full_schema = self.full_manifest_schema().await?;
        let to_remove: Vec<String> = properties
            .iter()
            .map(|name| {
                if !name.starts_with(EXTENDED_PREFIX) {
                    return Err(Error::from(NamespaceError::InvalidInput {
                        message: format!(
                            "Extended properties key {} must start with prefix: {}",
                            name, EXTENDED_PREFIX
                        ),
                    }));
                }

                let name = name.strip_prefix(EXTENDED_PREFIX).unwrap().to_string();
                if full_schema.column_with_name(name.as_str()).is_none() {
                    return Err(Error::from(NamespaceError::InvalidInput {
                        message: format!("Field {} doesn't exist in schema", name),
                    }));
                }
                Ok(name)
            })
            .collect::<Result<Vec<String>>>()?;

        if to_remove.is_empty() {
            return Ok(());
        }

        let remove: Vec<&str> = to_remove.iter().map(|s| s.as_str()).collect();
        let mut ds = self.manifest_dataset.get_mut().await?;
        ds.drop_columns(&remove).await
    }

    /// Split an object ID (vec of strings) into namespace and table name
    pub fn split_object_id(object_id: &[String]) -> (Vec<String>, String) {
        if object_id.len() == 1 {
            (vec![], object_id[0].clone())
        } else {
            (
                object_id[..object_id.len() - 1].to_vec(),
                object_id[object_id.len() - 1].clone(),
            )
        }
    }

    /// Convert an ID (vec of strings) to an object_id string
    pub fn str_object_id(object_id: &[String]) -> String {
        object_id.join(DELIMITER)
    }

    /// Format a version number as a zero-padded lexicographically sortable string.
    ///
    /// Versions are stored as 20-digit zero-padded integers (e.g., `00000000000000000001`
    /// for version 1) so that string-based range queries and sorting work correctly.
    pub fn format_table_version(version: i64) -> String {
        format!("{:020}", version)
    }

    /// Build the object_id for a table version entry.
    ///
    /// Format: `{table_object_id}${zero_padded_version}`
    pub fn build_version_object_id(table_object_id: &str, version: i64) -> String {
        format!(
            "{}{}{}",
            table_object_id,
            DELIMITER,
            Self::format_table_version(version)
        )
    }

    /// Parse a version number from the version suffix of a table version object_id.
    ///
    /// The object_id is formatted as `{table_id}${zero_padded_version}`.
    pub fn parse_version_from_object_id(object_id: &str) -> Option<i64> {
        let (_namespace, name) = Self::parse_object_id(object_id);
        name.parse::<i64>().ok()
    }

    /// Generate a new directory name in format: `<hash>_<object_id>`
    /// The hash is used to (1) optimize object store throughput,
    /// (2) have high enough entropy in a short period of time to prevent issues like
    /// failed table creation, delete and create new table of the same name, etc.
    /// The object_id is added after the hash to ensure
    /// dir name uniqueness and make debugging easier.
    pub fn generate_dir_name(object_id: &str) -> String {
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

        let full_url = base_url.join(relative_location).map_err(|e| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: format!(
                    "Failed to join URI '{}' with '{}': {:?}",
                    root, relative_location, e
                ),
            })
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
    async fn run_inline_optimization(&self) -> Result<()> {
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
                log::warn!(
                    "Failed to create BTREE index on object_id for __manifest table: {:?}. Query performance may be impacted.",
                    e
                );
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
                log::warn!(
                    "Failed to create Bitmap index on object_type for __manifest table: {:?}. Query performance may be impacted.",
                    e
                );
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
                log::warn!(
                    "Failed to create LabelList index on base_objects for __manifest table: {:?}. Query performance may be impacted.",
                    e
                );
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
                log::warn!(
                    "Failed to compact files for __manifest table: {:?}. Continuing with optimization.",
                    e
                );
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

    /// Get the manifest schema
    fn manifest_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            // Set unenforced primary key on object_id for bloom filter conflict detection
            Field::new("object_id", DataType::Utf8, false).with_metadata(
                [(
                    LANCE_UNENFORCED_PRIMARY_KEY_POSITION.to_string(),
                    "0".to_string(),
                )]
                .into_iter()
                .collect(),
            ),
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

    /// Get the full manifest schema, including basic fields and any extended fields.
    async fn full_manifest_schema(&self) -> Result<ArrowSchema> {
        let dataset_guard = self.manifest_dataset.get().await?;
        Ok(ArrowSchema::from(dataset_guard.schema()))
    }

    /// Get the extended manifest schema (fields not present in the basic manifest schema).
    async fn extended_manifest_schema(&self) -> Result<ArrowSchema> {
        let full = self.full_manifest_schema().await?;
        let basic = Self::manifest_schema();
        let mut fields: Vec<FieldRef> = vec![];
        for field in full.fields.iter() {
            let name = field.name();
            if basic.column_with_name(name.as_str()).is_none() {
                fields.push(field.clone());
            }
        }
        Ok(ArrowSchema::new(fields))
    }

    fn build_metadata_json(properties: &Option<HashMap<String, String>>) -> Option<String> {
        properties.as_ref().and_then(|props| {
            if props.is_empty() {
                None
            } else {
                let meta_props = props
                    .iter()
                    .filter(|(key, _)| !key.starts_with(EXTENDED_PREFIX))
                    .collect::<HashMap<_, _>>();
                serde_json::to_string(&meta_props).ok()
            }
        })
    }

    /// Get a scanner for the manifest dataset
    async fn manifest_scanner(&self) -> Result<Scanner> {
        let dataset_guard = self.manifest_dataset.get().await?;
        Ok(dataset_guard.scan())
    }

    /// Helper to execute a scanner and collect results into a Vec
    async fn execute_scanner(scanner: Scanner) -> Result<Vec<RecordBatch>> {
        let mut stream = scanner.try_into_stream().await.map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to create stream: {}", e),
            })
        })?;

        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!("Failed to read batch: {}", e),
                })
            })?);
        }

        Ok(batches)
    }

    /// Helper to get a string column from a record batch
    fn get_string_column<'a>(batch: &'a RecordBatch, column_name: &str) -> Result<&'a StringArray> {
        let column = batch.column_by_name(column_name).ok_or_else(|| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Column '{}' not found", column_name),
            })
        })?;
        column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!("Column '{}' is not a string array", column_name),
                })
            })
    }

    /// Check if the manifest contains an object with the given ID
    async fn manifest_contains_object(&self, object_id: &str) -> Result<bool> {
        let escaped_id = object_id.replace('\'', "''");
        let filter = format!("object_id = '{}'", escaped_id);

        let dataset_guard = self.manifest_dataset.get().await?;
        let mut scanner = dataset_guard.scan();

        scanner.filter(&filter).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to filter: {}", e),
            })
        })?;

        // Project no columns and enable row IDs for count_rows to work
        scanner.project::<&str>(&[]).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to project: {}", e),
            })
        })?;

        scanner.with_row_id();

        let count = scanner.count_rows().await.map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to count rows: {}", e),
            })
        })?;

        Ok(count > 0)
    }

    /// Query the manifest for a table with the given object ID
    async fn query_manifest_for_table(&self, object_id: &str) -> Result<Option<TableInfo>> {
        let escaped_id = object_id.replace('\'', "''");
        let filter = format!("object_id = '{}' AND object_type = 'table'", escaped_id);
        let objects = self.query_manifest(&filter).await?;
        let mut found: Option<TableInfo> = None;
        for obj in objects {
            let ManifestObject::Table(t) = obj else {
                continue;
            };
            if found.is_some() {
                return Err(NamespaceError::Internal {
                    message: format!(
                        "Expected exactly 1 table with id '{}', found more than 1",
                        object_id
                    ),
                }
                .into());
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
        scanner.filter(filter).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to filter: {}", e),
            })
        })?;
        scanner.project(&["location"]).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to project: {}", e),
            })
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
        self.insert_into_manifest_with_metadata(
            vec![ManifestEntry {
                object_id,
                object_type,
                location,
                metadata: None,
            }],
            None,
            None,
        )
        .await
    }

    /// Insert one or more entries into the manifest table with metadata and base_objects.
    ///
    /// This is the unified entry point for both single and batch inserts.
    /// Uses a single MergeInsert operation to insert all entries at once.
    /// If any entry already exists (matching object_id), the entire batch fails.
    pub async fn insert_into_manifest_with_metadata(
        &self,
        entries: Vec<ManifestEntry>,
        base_objects: Option<Vec<String>>,
        extended_batch: Option<RecordBatch>,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let schema = Self::manifest_schema();

        let mut object_ids = Vec::with_capacity(entries.len());
        let mut object_types = Vec::with_capacity(entries.len());
        let mut locations: Vec<Option<String>> = Vec::with_capacity(entries.len());
        let mut metadatas: Vec<Option<String>> = Vec::with_capacity(entries.len());

        let string_builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(string_builder).with_field(Arc::new(Field::new(
            "object_id",
            DataType::Utf8,
            true,
        )));

        for (i, entry) in entries.iter().enumerate() {
            object_ids.push(entry.object_id.as_str());
            object_types.push(entry.object_type.as_str());
            locations.push(entry.location.clone());
            metadatas.push(entry.metadata.clone());

            // Only the first entry gets the base_objects (for single-entry inserts
            // with base_objects like view creation); batch entries use null.
            if i == 0 {
                match &base_objects {
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
            } else {
                list_builder.append_null();
            }
        }

        let base_objects_array = list_builder.finish();

        let location_array: Arc<dyn Array> = Arc::new(StringArray::from(
            locations.iter().map(|l| l.as_deref()).collect::<Vec<_>>(),
        ));

        let metadata_array: Arc<dyn Array> = Arc::new(StringArray::from(
            metadatas.iter().map(|m| m.as_deref()).collect::<Vec<_>>(),
        ));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(object_ids)),
                Arc::new(StringArray::from(object_types.to_vec())),
                location_array,
                metadata_array,
                Arc::new(base_objects_array),
            ],
        )
        .map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to create manifest entries: {}", e),
            })
        })?;

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
            MergeInsertBuilder::try_new(dataset_arc, vec!["object_id".to_string()]).map_err(
                |e| {
                    lance_core::Error::from(NamespaceError::Internal {
                        message: format!("Failed to create merge builder: {}", e),
                    })
                },
            )?;
        merge_builder.when_matched(WhenMatched::Fail);
        merge_builder.when_not_matched(WhenNotMatched::InsertAll);
        // Use conflict_retries to handle cross-process races on manifest mutations.
        // When two processes concurrently insert the same object_id, the second one
        // hits a commit conflict. With conflict_retries > 0, the retry re-evaluates
        // the full MergeInsert plan against the latest data, where the join detects
        // the existing row and WhenMatched::Fail fires, producing a clear error.
        merge_builder.conflict_retries(5);
        // TODO: after BTREE index creation on object_id, has_scalar_index=true causes
        // MergeInsert to use V1 path which lacks bloom filters for conflict detection. This
        // results in (Some, None) filter mismatch when rebasing against V2 operations.
        // Setting use_index=false ensures all operations consistently use V2 path.
        merge_builder.use_index(false);
        if let Some(retries) = self.commit_retries {
            merge_builder.commit_retries(retries);
        }

        let (new_dataset_arc, _merge_stats) = merge_builder
            .try_build()
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!("Failed to build merge: {}", e),
                })
            })?
            .execute_reader(Box::new(reader))
            .await
            .map_err(|e| {
                convert_lance_commit_error(&e, "Failed to execute merge insert into manifest", None)
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
        let predicate = format!("object_id = '{}'", object_id);

        // Get dataset and use DeleteBuilder with configured retries
        let dataset_guard = self.manifest_dataset.get().await?;
        let dataset = Arc::new(dataset_guard.clone());
        drop(dataset_guard); // Drop read guard before delete

        let new_dataset = DeleteBuilder::new(dataset, &predicate)
            .execute()
            .await
            .map_err(|e| convert_lance_commit_error(&e, "Failed to delete", None))?;

        // Update the wrapper with the new dataset
        self.manifest_dataset
            .set_latest(
                Arc::try_unwrap(new_dataset.new_dataset).unwrap_or_else(|arc| (*arc).clone()),
            )
            .await;

        // Run inline optimization after delete
        if let Err(e) = self.run_inline_optimization().await {
            log::warn!(
                "Unexpected failure when running inline optimization: {:?}",
                e
            );
        }

        Ok(())
    }

    /// Query the manifest for all versions of a table, sorted by version.
    ///
    /// Returns a list of (version, metadata_json_string) tuples where metadata_json_string
    /// contains the full metadata JSON stored in the manifest (manifest_path, manifest_size,
    /// e_tag, naming_scheme).
    ///
    /// **Known limitation**: All matching rows are loaded into memory, sorted in Rust,
    /// and then truncated. For tables with a very large number of versions this may be
    /// expensive. Pushing sort/limit into the scan is not yet supported by Lance.
    pub async fn query_table_versions(
        &self,
        object_id: &str,
        descending: bool,
        limit: Option<i32>,
    ) -> Result<Vec<(i64, String)>> {
        let escaped_id = object_id.replace('\'', "''");
        // table_version object_ids are formatted as "{object_id}${zero_padded_version}"
        let filter = format!(
            "object_type = 'table_version' AND starts_with(object_id, '{}{}')",
            escaped_id, DELIMITER
        );
        let mut scanner = self.manifest_scanner().await?;
        scanner.filter(&filter).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to filter: {}", e),
            })
        })?;
        scanner.project(&["object_id", "metadata"]).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to project: {}", e),
            })
        })?;
        let batches = Self::execute_scanner(scanner).await?;

        let mut versions: Vec<(i64, String)> = Vec::new();
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let object_id_array = Self::get_string_column(&batch, "object_id")?;
            let metadata_array = Self::get_string_column(&batch, "metadata")?;
            for i in 0..batch.num_rows() {
                let oid = object_id_array.value(i);
                // Parse version from object_id
                if let Some(version) = Self::parse_version_from_object_id(oid) {
                    let metadata_str = metadata_array.value(i).to_string();
                    versions.push((version, metadata_str));
                }
            }
        }

        if descending {
            versions.sort_by(|a, b| b.0.cmp(&a.0));
        } else {
            versions.sort_by(|a, b| a.0.cmp(&b.0));
        }

        if let Some(limit) = limit {
            versions.truncate(limit as usize);
        }

        Ok(versions)
    }

    /// Query the manifest for a specific version of a table.
    ///
    /// Returns the full metadata JSON string if found, which contains
    /// manifest_path, manifest_size, e_tag, and naming_scheme.
    ///
    pub async fn query_table_version(
        &self,
        object_id: &str,
        version: i64,
    ) -> Result<Option<String>> {
        let version_object_id = Self::build_version_object_id(object_id, version);
        self.query_table_version_by_object_id(&version_object_id)
            .await
    }

    /// Query a specific table version by its exact object_id.
    async fn query_table_version_by_object_id(
        &self,
        version_object_id: &str,
    ) -> Result<Option<String>> {
        let escaped_id = version_object_id.replace('\'', "''");
        let filter = format!(
            "object_id = '{}' AND object_type = 'table_version'",
            escaped_id
        );
        let mut scanner = self.manifest_scanner().await?;
        scanner.filter(&filter).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to filter: {}", e),
            })
        })?;
        scanner.project(&["metadata"]).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to project: {}", e),
            })
        })?;
        let batches = Self::execute_scanner(scanner).await?;

        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let metadata_array = Self::get_string_column(&batch, "metadata")?;
            return Ok(Some(metadata_array.value(0).to_string()));
        }

        Ok(None)
    }

    /// Delete table version entries from the manifest for a given table and version ranges.
    ///
    /// Each range is (start_version, end_version) inclusive. Deletes all matching
    /// `object_type = 'table_version'` entries whose object_id matches
    /// `{object_id}${zero_padded_version}`.
    ///
    /// Builds a single filter expression covering all version ranges and executes
    /// one bulk delete operation instead of deleting versions one at a time.
    pub async fn delete_table_versions(
        &self,
        object_id: &str,
        ranges: &[(i64, i64)],
    ) -> Result<i64> {
        if ranges.is_empty() {
            return Ok(0);
        }

        // Collect all object_ids to delete (both new zero-padded and legacy formats)
        let mut object_id_conditions: Vec<String> = Vec::new();
        for (start, end) in ranges {
            for version in *start..=*end {
                let oid = Self::build_version_object_id(object_id, version);
                let escaped = oid.replace('\'', "''");
                object_id_conditions.push(format!("'{}'", escaped));
            }
        }

        if object_id_conditions.is_empty() {
            return Ok(0);
        }

        // First, count how many entries exist so we can report the deleted count
        let in_list = object_id_conditions.join(", ");
        let filter = format!(
            "object_type = 'table_version' AND object_id IN ({})",
            in_list
        );

        let mut scanner = self.manifest_scanner().await?;
        scanner.filter(&filter).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to filter: {}", e),
            })
        })?;
        scanner.project(&["object_id"]).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to project: {}", e),
            })
        })?;
        let batches = Self::execute_scanner(scanner).await?;
        let deleted_count: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

        if deleted_count == 0 {
            return Ok(0);
        }

        // Execute a single bulk delete with the combined filter
        let dataset_guard = self.manifest_dataset.get().await?;
        let dataset = Arc::new(dataset_guard.clone());
        drop(dataset_guard);

        let new_dataset = DeleteBuilder::new(dataset, &filter)
            .execute()
            .await
            .map_err(|e| {
                convert_lance_commit_error(&e, "Failed to batch delete table versions", None)
            })?;

        self.manifest_dataset
            .set_latest(
                Arc::try_unwrap(new_dataset.new_dataset).unwrap_or_else(|arc| (*arc).clone()),
            )
            .await;

        if let Err(e) = self.run_inline_optimization().await {
            log::warn!(
                "Unexpected failure when running inline optimization: {:?}",
                e
            );
        }

        Ok(deleted_count)
    }

    /// Atomically delete table version entries from the manifest by their object_ids.
    ///
    /// This method supports multi-table transactional deletion: all specified
    /// object_ids (which may span multiple tables) are deleted in a single atomic
    /// `DeleteBuilder` operation. Either all entries are removed or none are.
    ///
    /// Object IDs are formatted as `{table_id}${version}`.
    pub async fn batch_delete_table_versions_by_object_ids(
        &self,
        object_ids: &[String],
    ) -> Result<i64> {
        if object_ids.is_empty() {
            return Ok(0);
        }

        let in_list: String = object_ids
            .iter()
            .map(|oid| {
                let escaped = oid.replace('\'', "''");
                format!("'{}'", escaped)
            })
            .collect::<Vec<_>>()
            .join(", ");

        let filter = format!(
            "object_type = 'table_version' AND object_id IN ({})",
            in_list
        );

        // Count how many entries exist so we can report the deleted count
        let mut scanner = self.manifest_scanner().await?;
        scanner.filter(&filter).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to filter: {}", e),
            })
        })?;
        scanner.project(&["object_id"]).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to project: {}", e),
            })
        })?;
        let batches = Self::execute_scanner(scanner).await?;
        let deleted_count: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

        if deleted_count == 0 {
            return Ok(0);
        }

        // Execute a single atomic bulk delete covering all tables
        let dataset_guard = self.manifest_dataset.get().await?;
        let dataset = Arc::new(dataset_guard.clone());
        drop(dataset_guard);

        let new_dataset = DeleteBuilder::new(dataset, &filter)
            .execute()
            .await
            .map_err(|e| {
                convert_lance_commit_error(
                    &e,
                    "Failed to batch delete table versions across multiple tables",
                    None,
                )
            })?;

        self.manifest_dataset
            .set_latest(
                Arc::try_unwrap(new_dataset.new_dataset).unwrap_or_else(|arc| (*arc).clone()),
            )
            .await;

        if let Err(e) = self.run_inline_optimization().await {
            log::warn!(
                "Unexpected failure when running inline optimization: {:?}",
                e
            );
        }

        Ok(deleted_count)
    }

    /// Set a property flag in the __manifest table's metadata key-value map.
    ///
    /// This uses `dataset.update_metadata()` to persist the flag in the
    /// __manifest dataset's table metadata, rather than inserting a row.
    /// If the property already exists with the same value, this is a no-op.
    pub async fn set_property(&self, name: &str, value: &str) -> Result<()> {
        let dataset_guard = self.manifest_dataset.get().await?;
        if dataset_guard.metadata().get(name) == Some(&value.to_string()) {
            return Ok(());
        }
        drop(dataset_guard);

        let mut dataset_guard = self.manifest_dataset.get_mut().await?;
        dataset_guard
            .update_metadata([(name, value)])
            .await
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "Failed to set property '{}' in __manifest metadata: {}",
                        name, e
                    ),
                })
            })?;
        Ok(())
    }

    /// Check if a property flag exists in the __manifest table's metadata key-value map.
    pub async fn has_property(&self, name: &str) -> Result<bool> {
        let dataset_guard = self.manifest_dataset.get().await?;
        Ok(dataset_guard.metadata().contains_key(name))
    }

    /// Parse metadata JSON into a `TableVersion`.
    ///
    /// Returns `None` if metadata is invalid or missing required fields.
    fn parse_table_version(version: i64, metadata_str: &str) -> Option<TableVersion> {
        let meta: serde_json::Value = match serde_json::from_str(metadata_str) {
            Ok(v) => v,
            Err(e) => {
                log::warn!(
                    "Skipping version {} due to invalid metadata JSON: {}",
                    version,
                    e
                );
                return None;
            }
        };
        let manifest_path = match meta.get("manifest_path").and_then(|v| v.as_str()) {
            Some(p) => p.to_string(),
            None => {
                log::warn!(
                    "Skipping version {} due to missing 'manifest_path' in metadata — \
                     this may indicate data corruption",
                    version
                );
                return None;
            }
        };
        let manifest_size = meta.get("manifest_size").and_then(|v| v.as_i64());
        let e_tag = meta
            .get("e_tag")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        Some(TableVersion {
            version,
            manifest_path,
            manifest_size,
            e_tag,
            timestamp_millis: None,
            metadata: None,
        })
    }

    /// List table versions from the __manifest table.
    ///
    /// Queries the manifest for all versions of the given table and returns
    /// them as a `ListTableVersionsResponse`.
    pub async fn list_table_versions(
        &self,
        table_id: &[String],
        descending: bool,
        limit: Option<i32>,
    ) -> Result<ListTableVersionsResponse> {
        let object_id = Self::str_object_id(table_id);
        let manifest_versions = self
            .query_table_versions(&object_id, descending, limit)
            .await?;

        let table_versions: Vec<TableVersion> = manifest_versions
            .into_iter()
            .filter_map(|(version, metadata_str)| Self::parse_table_version(version, &metadata_str))
            .collect();

        Ok(ListTableVersionsResponse {
            versions: table_versions,
            page_token: None,
        })
    }

    /// Describe a specific table version from the __manifest table.
    ///
    /// Queries the manifest for a specific version and returns it as a
    /// `DescribeTableVersionResponse`. Returns an error if the version is not found.
    pub async fn describe_table_version(
        &self,
        table_id: &[String],
        version: i64,
    ) -> Result<DescribeTableVersionResponse> {
        let object_id = Self::str_object_id(table_id);
        if let Some(metadata_str) = self.query_table_version(&object_id, version).await?
            && let Some(tv) = Self::parse_table_version(version, &metadata_str)
        {
            return Ok(DescribeTableVersionResponse {
                version: Box::new(tv),
            });
        }
        Err(NamespaceError::TableVersionNotFound {
            message: format!("version {} for table {:?}", version, table_id),
        }
        .into())
    }

    /// Register a table in the manifest without creating the physical table (internal helper for migration)
    pub async fn register_table(&self, name: &str, location: String) -> Result<()> {
        let object_id = Self::build_object_id(&[], name);
        if self.manifest_contains_object(&object_id).await? {
            return Err(NamespaceError::Internal {
                message: format!("Table '{}' already exists", name),
            }
            .into());
        }

        self.insert_into_manifest(object_id, ObjectType::Table, Some(location))
            .await
    }

    /// Validate that all levels of a namespace path exist
    async fn validate_namespace_levels_exist(&self, namespace_path: &[String]) -> Result<()> {
        for i in 1..=namespace_path.len() {
            let partial_path = &namespace_path[..i];
            let object_id = partial_path.join(DELIMITER);
            if !self.manifest_contains_object(&object_id).await? {
                return Err(NamespaceError::NamespaceNotFound {
                    message: format!("parent namespace '{}'", object_id),
                }
                .into());
            }
        }
        Ok(())
    }

    /// Query the manifest for a namespace with the given object ID
    async fn query_manifest_for_namespace(&self, object_id: &str) -> Result<Option<NamespaceInfo>> {
        let escaped_id = object_id.replace('\'', "''");
        let filter = format!("object_id = '{}' AND object_type = 'namespace'", escaped_id);
        let objects = self.query_manifest(&filter).await?;
        let mut found: Option<NamespaceInfo> = None;
        for obj in objects {
            let ManifestObject::Namespace(ns) = obj else {
                continue;
            };
            if found.is_some() {
                return Err(NamespaceError::Internal {
                    message: format!(
                        "Expected exactly 1 namespace with id '{}', found more than 1",
                        object_id
                    ),
                }
                .into());
            }
            found = Some(ns);
        }
        Ok(found)
    }

    async fn query_manifest(&self, filter: &str) -> Result<Vec<ManifestObject>> {
        let mut scanner = self.manifest_scanner().await?;
        scanner.filter(filter).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to filter: {}", e),
            })
        })?;

        let batches = Self::execute_scanner(scanner).await?;
        let mut objects = Vec::new();
        for batch in batches.iter() {
            for row_idx in 0..batch.num_rows() {
                objects.push(parse_manifest_object(batch, row_idx)?);
            }
        }
        Ok(objects)
    }

    /// Create or load the manifest dataset, ensuring it has the latest schema setup.
    ///
    /// This function will:
    /// 1. Try to load an existing manifest table
    /// 2. If it exists, check and migrate the schema if needed (e.g., add primary key metadata)
    /// 3. If it doesn't exist, create a new manifest table with the current schema
    /// 4. Persist feature flags (e.g., table_version_storage_enabled) if requested
    async fn ensure_manifest_table_up_to_date(
        root: &str,
        storage_options: &Option<HashMap<String, String>>,
        session: Option<Arc<Session>>,
        table_version_storage_enabled: bool,
    ) -> Result<DatasetConsistencyWrapper> {
        let manifest_path = format!("{}/{}", root, MANIFEST_TABLE_NAME);
        log::debug!("Attempting to load manifest from {}", manifest_path);
        let store_options = ObjectStoreParams {
            storage_options_accessor: storage_options.as_ref().map(|opts| {
                Arc::new(
                    lance_io::object_store::StorageOptionsAccessor::with_static_options(
                        opts.clone(),
                    ),
                )
            }),
            ..Default::default()
        };
        let read_params = ReadParams {
            session: session.clone(),
            store_options: Some(store_options.clone()),
            ..Default::default()
        };
        let dataset_result = DatasetBuilder::from_uri(&manifest_path)
            .with_read_params(read_params)
            .load()
            .await;
        if let Ok(mut dataset) = dataset_result {
            // Check if the object_id field has primary key metadata, migrate if not
            let needs_pk_migration = dataset
                .schema()
                .field("object_id")
                .map(|f| {
                    !f.metadata
                        .contains_key(LANCE_UNENFORCED_PRIMARY_KEY_POSITION)
                })
                .unwrap_or(false);

            if needs_pk_migration {
                log::info!("Migrating __manifest table to add primary key metadata on object_id");
                dataset
                    .update_field_metadata()
                    .update("object_id", [(LANCE_UNENFORCED_PRIMARY_KEY_POSITION, "0")])
                    .map_err(|e| {
                        lance_core::Error::from(NamespaceError::Internal {
                            message: format!("Failed to find object_id field for migration: {}", e),
                        })
                    })?
                    .await
                    .map_err(|e| {
                        lance_core::Error::from(NamespaceError::Internal {
                            message: format!("Failed to migrate primary key metadata: {}", e),
                        })
                    })?;
            }

            // Persist table_version_storage_enabled flag in __manifest so that once
            // enabled, it becomes a permanent property of this namespace.
            if table_version_storage_enabled {
                let needs_flag = dataset
                    .metadata()
                    .get("table_version_storage_enabled")
                    .map(|v| v != "true")
                    .unwrap_or(true);

                if needs_flag
                    && let Err(e) = dataset
                        .update_metadata([("table_version_storage_enabled", "true")])
                        .await
                {
                    log::warn!(
                        "Failed to persist table_version_storage_enabled flag in __manifest: {:?}",
                        e
                    );
                }
            }

            Ok(DatasetConsistencyWrapper::new(dataset))
        } else {
            log::info!("Creating new manifest table at {}", manifest_path);
            let schema = Self::manifest_schema();
            let empty_batch = RecordBatch::new_empty(schema.clone());
            let reader = RecordBatchIterator::new(vec![Ok(empty_batch)], schema.clone());

            let store_params = ObjectStoreParams {
                storage_options_accessor: storage_options.as_ref().map(|opts| {
                    Arc::new(
                        lance_io::object_store::StorageOptionsAccessor::with_static_options(
                            opts.clone(),
                        ),
                    )
                }),
                ..Default::default()
            };
            let write_params = WriteParams {
                session: session.clone(),
                store_params: Some(store_params),
                ..Default::default()
            };

            let dataset =
                Dataset::write(Box::new(reader), &manifest_path, Some(write_params)).await;

            // Handle race condition where another process created the manifest concurrently
            match dataset {
                Ok(dataset) => {
                    log::info!(
                        "Successfully created manifest table at {}, version={}, uri={}",
                        manifest_path,
                        dataset.version().version,
                        dataset.uri()
                    );
                    Ok(DatasetConsistencyWrapper::new(dataset))
                }
                Err(ref e)
                    if matches!(
                        e,
                        LanceError::DatasetAlreadyExists { .. }
                            | LanceError::CommitConflict { .. }
                            | LanceError::IncompatibleTransaction { .. }
                            | LanceError::RetryableCommitConflict { .. }
                    ) =>
                {
                    // Another process created the manifest concurrently, try to load it
                    log::info!(
                        "Manifest table was created by another process, loading it: {}",
                        manifest_path
                    );
                    let recovery_store_options = ObjectStoreParams {
                        storage_options_accessor: storage_options.as_ref().map(|opts| {
                            Arc::new(
                                lance_io::object_store::StorageOptionsAccessor::with_static_options(
                                    opts.clone(),
                                ),
                            )
                        }),
                        ..Default::default()
                    };
                    let recovery_read_params = ReadParams {
                        session,
                        store_options: Some(recovery_store_options),
                        ..Default::default()
                    };
                    let dataset = DatasetBuilder::from_uri(&manifest_path)
                        .with_read_params(recovery_read_params)
                        .load()
                        .await
                        .map_err(|e| {
                            lance_core::Error::from(NamespaceError::Internal {
                                message: format!(
                                    "Failed to load manifest dataset after creation conflict: {}",
                                    e
                                ),
                            })
                        })?;
                    Ok(DatasetConsistencyWrapper::new(dataset))
                }
                Err(e) => Err(lance_core::Error::from(NamespaceError::Internal {
                    message: format!("Failed to create manifest dataset: {}", e),
                })),
            }
        }
    }
}

/// Parse one row of the `__manifest` table into a manifest object.
fn parse_manifest_object(batch: &RecordBatch, index: usize) -> Result<ManifestObject> {
    if index >= batch.num_rows() {
        return Err(Error::from(NamespaceError::InvalidInput {
            message: format!(
                "index {} is out of bounds for batch with {} rows",
                index,
                batch.num_rows()
            ),
        }));
    }

    // Parse and merge properties from extended columns and metadata JSON.
    let mut merged = batch_to_extended_props(batch, index)?;
    let metadata_array = ManifestNamespace::get_string_column(batch, "metadata")?;
    if !metadata_array.is_null(index) {
        let metadata_str = metadata_array.value(index);
        let map = serde_json::from_str::<HashMap<String, String>>(metadata_str).map_err(|e| {
            Error::from(NamespaceError::Internal {
                message: format!("Failed to deserialize metadata: {}", e),
            })
        })?;
        merged.extend(map);
    }

    let properties = if merged.is_empty() {
        None
    } else {
        Some(merged)
    };

    let object_type = ManifestNamespace::get_string_column(batch, "object_type")?
        .value(index)
        .to_string();

    match object_type.as_str() {
        "namespace" => {
            let object_id_array = ManifestNamespace::get_string_column(batch, "object_id")?;
            let (namespace, name) =
                ManifestNamespace::parse_object_id(object_id_array.value(index));
            Ok(ManifestObject::Namespace(NamespaceInfo {
                namespace,
                name,
                metadata: properties,
            }))
        }
        "table" => {
            let object_id_array = ManifestNamespace::get_string_column(batch, "object_id")?;
            let location_array = ManifestNamespace::get_string_column(batch, "location")?;
            let location = location_array.value(index).to_string();
            let (namespace, name) =
                ManifestNamespace::parse_object_id(object_id_array.value(index));
            Ok(ManifestObject::Table(TableInfo {
                namespace,
                name,
                location,
                properties,
            }))
        }
        t => Err(NamespaceError::Internal {
            message: format!("Unknown object type {}", t),
        }
        .into()),
    }
}

/// Parse one row of a `RecordBatch` into a map of extended properties.
fn batch_to_extended_props(batch: &RecordBatch, index: usize) -> Result<HashMap<String, String>> {
    let basic_schema = ManifestNamespace::manifest_schema();
    let excluded: HashSet<&str> = basic_schema
        .fields
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    let mut result = HashMap::new();
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let col_name = field.name().to_string();
        if excluded.contains(col_name.as_str()) {
            continue;
        }

        let array = batch.column(i);
        if array.is_null(index) {
            continue;
        }

        let Ok(scalar) = ScalarValue::try_from_array(array.as_ref(), index) else {
            continue;
        };
        let Ok(value_str) = scalar_to_str(&scalar) else {
            continue;
        };

        if let Some(value) = value_str
            && !value.is_empty()
        {
            result.insert(format!("{}{}", EXTENDED_PREFIX, col_name), value);
        }
    }

    Ok(result)
}

fn scalar_to_str(scalar: &ScalarValue) -> Result<Option<String>> {
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
        _ => Err(Error::from(NamespaceError::InvalidInput {
            message: format!("Unsupported extended scalar: {:?}", scalar),
        })),
    }
}

fn scalar_from_str(dt: &DataType, value: &str) -> Result<ScalarValue> {
    match dt {
        DataType::Utf8 => Ok(ScalarValue::Utf8(Some(value.to_string()))),
        DataType::LargeUtf8 => Ok(ScalarValue::LargeUtf8(Some(value.to_string()))),
        DataType::Boolean => Ok(ScalarValue::Boolean(Some(bool::from_str(value).map_err(
            |e| {
                Error::from(NamespaceError::InvalidInput {
                    message: format!("Invalid boolean '{}': {}", value, e),
                })
            },
        )?))),
        DataType::Int32 => Ok(ScalarValue::Int32(Some(i32::from_str(value).map_err(
            |e| {
                Error::from(NamespaceError::InvalidInput {
                    message: format!("Invalid int32 '{}': {}", value, e),
                })
            },
        )?))),
        DataType::Int64 => Ok(ScalarValue::Int64(Some(i64::from_str(value).map_err(
            |e| {
                Error::from(NamespaceError::InvalidInput {
                    message: format!("Invalid int64 '{}': {}", value, e),
                })
            },
        )?))),
        DataType::UInt32 => Ok(ScalarValue::UInt32(Some(u32::from_str(value).map_err(
            |e| {
                Error::from(NamespaceError::InvalidInput {
                    message: format!("Invalid uint32 '{}': {}", value, e),
                })
            },
        )?))),
        DataType::UInt64 => Ok(ScalarValue::UInt64(Some(u64::from_str(value).map_err(
            |e| {
                Error::from(NamespaceError::InvalidInput {
                    message: format!("Invalid uint64 '{}': {}", value, e),
                })
            },
        )?))),
        DataType::Float32 => Ok(ScalarValue::Float32(Some(f32::from_str(value).map_err(
            |e| {
                Error::from(NamespaceError::InvalidInput {
                    message: format!("Invalid float32 '{}': {}", value, e),
                })
            },
        )?))),
        DataType::Float64 => Ok(ScalarValue::Float64(Some(f64::from_str(value).map_err(
            |e| {
                Error::from(NamespaceError::InvalidInput {
                    message: format!("Invalid float64 '{}': {}", value, e),
                })
            },
        )?))),
        DataType::Date32 => Ok(ScalarValue::Date32(Some(i32::from_str(value).map_err(
            |e| {
                Error::from(NamespaceError::InvalidInput {
                    message: format!("Invalid date32 '{}': {}", value, e),
                })
            },
        )?))),
        DataType::Date64 => Ok(ScalarValue::Date64(Some(i64::from_str(value).map_err(
            |e| {
                Error::from(NamespaceError::InvalidInput {
                    message: format!("Invalid date64 '{}': {}", value, e),
                })
            },
        )?))),
        DataType::Binary => Ok(ScalarValue::Binary(Some(hex_to_bytes(value)?))),
        DataType::LargeBinary => Ok(ScalarValue::LargeBinary(Some(hex_to_bytes(value)?))),
        _ => Err(Error::from(NamespaceError::InvalidInput {
            message: format!("Unsupported extended column type: {:?}", dt),
        })),
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
        return Err(Error::from(NamespaceError::InvalidInput {
            message: format!("Invalid hex string length {}", s.len()),
        }));
    }

    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    for i in (0..bytes.len()).step_by(2) {
        let hex = std::str::from_utf8(&bytes[i..i + 2]).map_err(|e| {
            Error::from(NamespaceError::InvalidInput {
                message: format!("Invalid hex string encoding: {}", e),
            })
        })?;
        let v = u8::from_str_radix(hex, 16).map_err(|e| {
            Error::from(NamespaceError::InvalidInput {
                message: format!("Invalid hex byte '{}': {}", hex, e),
            })
        })?;
        out.push(v);
    }
    Ok(out)
}

/// Transform one record into a record batch.
fn record_to_batch(
    record: &HashMap<String, ScalarValue>,
    schema: &ArrowSchema,
) -> Result<RecordBatch> {
    let mut fields: Vec<FieldRef> = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for f in schema.fields() {
        let name = f.name().as_str();
        let Some(scalar) = record.get(name) else {
            continue;
        };
        if scalar.is_null() {
            continue;
        }
        if scalar.data_type() != *f.data_type() {
            return Err(Error::from(NamespaceError::InvalidInput {
                message: format!(
                    "Record '{}' has type {:?}, expected {:?}",
                    name,
                    scalar.data_type(),
                    f.data_type()
                ),
            }));
        }
        let v = scalar.to_array().map_err(|e| {
            Error::from(NamespaceError::Internal {
                message: format!(
                    "Failed to convert scalar for column '{}' to array: {}",
                    name, e
                ),
            })
        })?;
        fields.push(f.clone());
        arrays.push(v);
    }

    let options = RecordBatchOptions::new().with_row_count(Some(1));
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new_with_options(schema, arrays, &options).map_err(|e| {
        Error::from(NamespaceError::Internal {
            message: format!("Failed to create manifest batch: {}", e),
        })
    })
}

fn extended_props_to_record(
    props: &HashMap<String, String>,
    schema: &ArrowSchema,
) -> Result<HashMap<String, ScalarValue>> {
    fn is_nullish_extended_value(v: &str) -> bool {
        v.is_empty() || v.eq_ignore_ascii_case("null")
    }

    let basic_schema = ManifestNamespace::manifest_schema();
    let excluded: HashSet<&str> = basic_schema
        .fields
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    for (k, v) in props.iter() {
        if is_nullish_extended_value(v) {
            continue;
        }
        let Some(col_name) = k.strip_prefix(EXTENDED_PREFIX) else {
            continue;
        };
        if excluded.contains(col_name) {
            return Err(Error::from(NamespaceError::InvalidInput {
                message: format!("Column {} is preserved.", col_name),
            }));
        }
        if schema.column_with_name(col_name).is_none() {
            return Err(Error::from(NamespaceError::InvalidInput {
                message: format!("Column {} does not exist in extended properties", col_name),
            }));
        }
    }

    let mut out = HashMap::new();
    for f in schema.fields() {
        let name = f.name().as_str();
        if excluded.contains(name) {
            continue;
        }
        let Some(v) = props.get(&format!("{}{}", EXTENDED_PREFIX, name)) else {
            continue;
        };
        if is_nullish_extended_value(v) {
            continue;
        }
        out.insert(name.to_string(), scalar_from_str(f.data_type(), v)?);
    }
    Ok(out)
}

#[async_trait]
impl LanceNamespace for ManifestNamespace {
    fn namespace_id(&self) -> String {
        self.root.clone()
    }

    async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
        let namespace_id = request.id.as_ref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Namespace ID is required".to_string(),
            })
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
                prefix,
                DELIMITER,
                prefix.len() + 2
            )
        };

        let mut scanner = self.manifest_scanner().await?;
        scanner.filter(&filter).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to filter: {}", e),
            })
        })?;
        scanner.project(&["object_id"]).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to project: {}", e),
            })
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
        let table_id = request.id.as_ref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Table ID is required".to_string(),
            })
        })?;

        if table_id.is_empty() {
            return Err(NamespaceError::InvalidInput {
                message: "Table ID cannot be empty".to_string(),
            }
            .into());
        }

        let object_id = Self::str_object_id(table_id);
        let table_info = self.query_manifest_for_table(&object_id).boxed().await?;

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

                let properties = info.properties.clone();

                // If not loading detailed metadata, return minimal response with just location
                if !load_detailed_metadata {
                    return Ok(DescribeTableResponse {
                        table: Some(table_name),
                        namespace: Some(namespace_id),
                        location: Some(table_uri.clone()),
                        table_uri: Some(table_uri),
                        storage_options,
                        properties,
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
                            properties: properties.clone(),
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
                            properties: properties.clone(),
                            ..Default::default()
                        })
                    }
                }
            }
            None => Err(NamespaceError::TableNotFound {
                message: object_id.to_string(),
            }
            .into()),
        }
    }

    async fn table_exists(&self, request: TableExistsRequest) -> Result<()> {
        let table_id = request.id.as_ref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Table ID is required".to_string(),
            })
        })?;

        if table_id.is_empty() {
            return Err(NamespaceError::InvalidInput {
                message: "Table ID cannot be empty".to_string(),
            }
            .into());
        }

        let (namespace, table_name) = Self::split_object_id(table_id);
        let object_id = Self::build_object_id(&namespace, &table_name);
        let exists = self.manifest_contains_object(&object_id).await?;
        if exists {
            Ok(())
        } else {
            Err(NamespaceError::TableNotFound {
                message: table_name.to_string(),
            }
            .into())
        }
    }

    async fn create_table(
        &self,
        request: CreateTableRequest,
        data: Bytes,
    ) -> Result<CreateTableResponse> {
        let table_id = request.id.as_ref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Table ID is required".to_string(),
            })
        })?;

        if table_id.is_empty() {
            return Err(NamespaceError::InvalidInput {
                message: "Table ID cannot be empty".to_string(),
            }
            .into());
        }

        let (namespace, table_name) = Self::split_object_id(table_id);
        let object_id = Self::build_object_id(&namespace, &table_name);

        // Check if table already exists in manifest
        if self.manifest_contains_object(&object_id).await? {
            return Err(NamespaceError::Internal {
                message: format!("Table '{}' already exists", table_name),
            }
            .into());
        }

        // Serialize non-extended properties into metadata JSON and store extended properties in
        // dedicated columns.
        let metadata = Self::build_metadata_json(&request.properties);
        let schema = self.extended_manifest_schema().await?;
        let extended_batch = if let Some(props) = &request.properties {
            let record = extended_props_to_record(props, &schema)?;
            if record.is_empty() {
                None
            } else {
                Some(record_to_batch(&record, &schema)?)
            }
        } else {
            None
        };

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
            return Err(NamespaceError::InvalidInput {
                message: "Request data (Arrow IPC stream) is required for create_table".to_string(),
            }
            .into());
        }

        // Write the data using Lance Dataset
        let cursor = Cursor::new(data.to_vec());
        let stream_reader = StreamReader::try_new(cursor, None).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to read IPC stream: {}", e),
            })
        })?;

        let batches: Vec<RecordBatch> = stream_reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to collect batches: {}", e),
            })
        })?;

        if batches.is_empty() {
            return Err(NamespaceError::Internal {
                message: "No data provided for table creation".to_string(),
            }
            .into());
        }

        let schema = batches[0].schema();
        let batch_results: Vec<std::result::Result<RecordBatch, arrow_schema::ArrowError>> =
            batches.into_iter().map(Ok).collect();
        let reader = RecordBatchIterator::new(batch_results, schema);

        let store_params = ObjectStoreParams {
            storage_options_accessor: self.storage_options.as_ref().map(|opts| {
                Arc::new(
                    lance_io::object_store::StorageOptionsAccessor::with_static_options(
                        opts.clone(),
                    ),
                )
            }),
            ..Default::default()
        };
        let write_params = WriteParams {
            session: self.session.clone(),
            store_params: Some(store_params),
            ..Default::default()
        };
        let _dataset = Dataset::write(Box::new(reader), &table_uri, Some(write_params))
            .await
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!("Failed to write dataset: {}", e),
                })
            })?;

        // Register in manifest (store dir_name, not full URI)
        self.insert_into_manifest_with_metadata(
            vec![ManifestEntry {
                object_id,
                object_type: ObjectType::Table,
                location: Some(dir_name),
                metadata,
            }],
            None,
            extended_batch,
        )
        .await?;

        Ok(CreateTableResponse {
            version: Some(1),
            location: Some(table_uri),
            storage_options: self.storage_options.clone(),
            ..Default::default()
        })
    }

    async fn drop_table(&self, request: DropTableRequest) -> Result<DropTableResponse> {
        let table_id = request.id.as_ref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Table ID is required".to_string(),
            })
        })?;

        if table_id.is_empty() {
            return Err(NamespaceError::InvalidInput {
                message: "Table ID cannot be empty".to_string(),
            }
            .into());
        }

        let (namespace, table_name) = Self::split_object_id(table_id);
        let object_id = Self::build_object_id(&namespace, &table_name);

        // Query manifest for table location
        let table_info = self.query_manifest_for_table(&object_id).boxed().await?;

        match table_info {
            Some(info) => {
                // Delete from manifest first
                self.delete_from_manifest(&object_id).boxed().await?;

                // Delete physical data directory using the dir_name from manifest
                let table_path = self.base_path.child(info.location.as_str());
                let table_uri = Self::construct_full_uri(&self.root, &info.location)?;

                // Remove the table directory
                self.object_store
                    .remove_dir_all(table_path)
                    .boxed()
                    .await
                    .map_err(|e| {
                        lance_core::Error::from(NamespaceError::Internal {
                            message: format!("Failed to delete table directory: {}", e),
                        })
                    })?;

                Ok(DropTableResponse {
                    id: request.id.clone(),
                    location: Some(table_uri),
                    ..Default::default()
                })
            }
            None => Err(NamespaceError::TableNotFound {
                message: table_name.to_string(),
            }
            .into()),
        }
    }

    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        let parent_namespace = request.id.as_ref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Namespace ID is required".to_string(),
            })
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
                prefix,
                DELIMITER,
                prefix.len() + 2
            )
        };

        let mut scanner = self.manifest_scanner().await?;
        scanner.filter(&filter).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to filter: {}", e),
            })
        })?;
        scanner.project(&["object_id"]).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to project: {}", e),
            })
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
        let namespace_id = request.id.as_ref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Namespace ID is required".to_string(),
            })
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
            None => Err(NamespaceError::NamespaceNotFound {
                message: object_id.to_string(),
            }
            .into()),
        }
    }

    async fn create_namespace(
        &self,
        request: CreateNamespaceRequest,
    ) -> Result<CreateNamespaceResponse> {
        let namespace_id = request.id.as_ref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Namespace ID is required".to_string(),
            })
        })?;

        // Root namespace always exists and cannot be created
        if namespace_id.is_empty() {
            return Err(NamespaceError::NamespaceAlreadyExists {
                message: "root namespace".to_string(),
            }
            .into());
        }

        // Validate parent namespaces exist (but not the namespace being created)
        if namespace_id.len() > 1 {
            self.validate_namespace_levels_exist(&namespace_id[..namespace_id.len() - 1])
                .await?;
        }

        let object_id = namespace_id.join(DELIMITER);
        if self.manifest_contains_object(&object_id).await? {
            return Err(NamespaceError::NamespaceAlreadyExists {
                message: object_id.to_string(),
            }
            .into());
        }

        // Serialize non-extended properties into metadata JSON and store extended properties in
        // dedicated columns.
        let metadata = Self::build_metadata_json(&request.properties);
        let schema = self.extended_manifest_schema().await?;
        let extended_batch = if let Some(props) = &request.properties {
            let record = extended_props_to_record(props, &schema)?;
            if record.is_empty() {
                None
            } else {
                Some(record_to_batch(&record, &schema)?)
            }
        } else {
            None
        };

        self.insert_into_manifest_with_metadata(
            vec![ManifestEntry {
                object_id,
                object_type: ObjectType::Namespace,
                location: None,
                metadata,
            }],
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
        let namespace_id = request.id.as_ref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Namespace ID is required".to_string(),
            })
        })?;

        // Root namespace always exists and cannot be dropped
        if namespace_id.is_empty() {
            return Err(NamespaceError::InvalidInput {
                message: "Root namespace cannot be dropped".to_string(),
            }
            .into());
        }

        let object_id = namespace_id.join(DELIMITER);

        // Check if namespace exists
        if !self.manifest_contains_object(&object_id).boxed().await? {
            return Err(NamespaceError::NamespaceNotFound {
                message: object_id.to_string(),
            }
            .into());
        }

        // Check for child namespaces
        let escaped_id = object_id.replace('\'', "''");
        let prefix = format!("{}{}", escaped_id, DELIMITER);
        let filter = format!("starts_with(object_id, '{}')", prefix);
        let mut scanner = self.manifest_scanner().boxed().await?;
        scanner.filter(&filter).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to filter: {}", e),
            })
        })?;
        scanner.project::<&str>(&[]).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to project: {}", e),
            })
        })?;
        scanner.with_row_id();
        let count = scanner.count_rows().boxed().await.map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to count rows: {}", e),
            })
        })?;

        if count > 0 {
            return Err(NamespaceError::NamespaceNotEmpty {
                message: format!("'{}' (contains {} child objects)", object_id, count),
            }
            .into());
        }

        self.delete_from_manifest(&object_id).boxed().await?;

        Ok(DropNamespaceResponse::default())
    }

    async fn namespace_exists(&self, request: NamespaceExistsRequest) -> Result<()> {
        let namespace_id = request.id.as_ref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Namespace ID is required".to_string(),
            })
        })?;

        // Root namespace always exists
        if namespace_id.is_empty() {
            return Ok(());
        }

        let object_id = namespace_id.join(DELIMITER);
        if self.manifest_contains_object(&object_id).await? {
            Ok(())
        } else {
            Err(NamespaceError::NamespaceNotFound {
                message: object_id.to_string(),
            }
            .into())
        }
    }

    async fn declare_table(&self, request: DeclareTableRequest) -> Result<DeclareTableResponse> {
        let table_id = request.id.as_ref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Table ID is required".to_string(),
            })
        })?;

        if table_id.is_empty() {
            return Err(NamespaceError::InvalidInput {
                message: "Table ID cannot be empty".to_string(),
            }
            .into());
        }

        let (namespace, table_name) = Self::split_object_id(table_id);
        let object_id = Self::build_object_id(&namespace, &table_name);

        // Check if table already exists in manifest
        let existing = self.query_manifest_for_table(&object_id).await?;
        if existing.is_some() {
            return Err(NamespaceError::TableAlreadyExists {
                message: table_name.to_string(),
            }
            .into());
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
                return Err(NamespaceError::InvalidInput {
                    message: format!(
                        "Cannot declare table {} at location {}, must be at location {}",
                        table_name, req_location, table_uri
                    ),
                }
                .into());
            }
        }

        // Create the .lance-reserved file to mark the table as existing
        let reserved_file_path = table_path.child(".lance-reserved");

        self.object_store
            .create(&reserved_file_path)
            .await
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "Failed to create .lance-reserved file for table {}: {}",
                        table_name, e
                    ),
                })
            })?
            .shutdown()
            .await
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "Failed to finalize .lance-reserved file for table {}: {}",
                        table_name, e
                    ),
                })
            })?;

        // Serialize non-extended properties into metadata JSON and store extended properties in
        // dedicated columns.
        let metadata = Self::build_metadata_json(&request.properties);
        let schema = self.extended_manifest_schema().await?;
        let extended_batch = if let Some(props) = &request.properties {
            let record = extended_props_to_record(props, &schema)?;
            if record.is_empty() {
                None
            } else {
                Some(record_to_batch(&record, &schema)?)
            }
        } else {
            None
        };

        // Add entry to manifest marking this as a declared table (store dir_name, not full path)
        self.insert_into_manifest_with_metadata(
            vec![ManifestEntry {
                object_id,
                object_type: ObjectType::Table,
                location: Some(dir_name),
                metadata,
            }],
            None,
            extended_batch,
        )
        .await?;

        log::info!(
            "Declared table '{}' in manifest at {}",
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

        Ok(DeclareTableResponse {
            location: Some(table_uri),
            storage_options,
            ..Default::default()
        })
    }

    async fn register_table(&self, request: RegisterTableRequest) -> Result<RegisterTableResponse> {
        let table_id = request.id.as_ref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Table ID is required".to_string(),
            })
        })?;

        if table_id.is_empty() {
            return Err(NamespaceError::InvalidInput {
                message: "Table ID cannot be empty".to_string(),
            }
            .into());
        }

        let location = request.location.clone();

        // Validate that location is a relative path within the root directory
        // We don't allow absolute URIs or paths that escape the root
        if location.contains("://") {
            return Err(NamespaceError::InvalidInput {
                message: format!(
                    "Absolute URIs are not allowed for register_table. Location must be a relative path within the root directory: {}",
                    location
                ),
            }
            .into());
        }

        if location.starts_with('/') {
            return Err(NamespaceError::InvalidInput {
                message: format!(
                    "Absolute paths are not allowed for register_table. Location must be a relative path within the root directory: {}",
                    location
                ),
            }
            .into());
        }

        // Check for path traversal attempts
        if location.contains("..") {
            return Err(NamespaceError::InvalidInput {
                message: format!(
                    "Path traversal is not allowed. Location must be a relative path within the root directory: {}",
                    location
                ),
            }
            .into());
        }

        let (namespace, table_name) = Self::split_object_id(table_id);
        let object_id = Self::build_object_id(&namespace, &table_name);

        // Validate that parent namespaces exist (if not root)
        if !namespace.is_empty() {
            self.validate_namespace_levels_exist(&namespace).await?;
        }

        // Check if table already exists
        if self.manifest_contains_object(&object_id).await? {
            return Err(NamespaceError::TableAlreadyExists {
                message: object_id.to_string(),
            }
            .into());
        }

        // Serialize non-extended properties into metadata JSON and store extended properties in
        // dedicated columns.
        let metadata = Self::build_metadata_json(&request.properties);
        let schema = self.extended_manifest_schema().await?;
        let extended_batch = if let Some(props) = &request.properties {
            let record = extended_props_to_record(props, &schema)?;
            if record.is_empty() {
                None
            } else {
                Some(record_to_batch(&record, &schema)?)
            }
        } else {
            None
        };

        // Register the table with its location in the manifest
        self.insert_into_manifest_with_metadata(
            vec![ManifestEntry {
                object_id,
                object_type: ObjectType::Table,
                location: Some(location.clone()),
                metadata,
            }],
            None,
            extended_batch,
        )
        .await?;

        Ok(RegisterTableResponse {
            location: Some(location),
            ..Default::default()
        })
    }

    async fn deregister_table(
        &self,
        request: DeregisterTableRequest,
    ) -> Result<DeregisterTableResponse> {
        let table_id = request.id.as_ref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Table ID is required".to_string(),
            })
        })?;

        if table_id.is_empty() {
            return Err(NamespaceError::InvalidInput {
                message: "Table ID cannot be empty".to_string(),
            }
            .into());
        }

        let (namespace, table_name) = Self::split_object_id(table_id);
        let object_id = Self::build_object_id(&namespace, &table_name);

        // Get table info before deleting
        let table_info = self.query_manifest_for_table(&object_id).await?;

        let table_uri = match table_info {
            Some(info) => {
                // Delete from manifest only (leave physical data intact)
                self.delete_from_manifest(&object_id).boxed().await?;
                Self::construct_full_uri(&self.root, &info.location)?
            }
            None => {
                return Err(NamespaceError::TableNotFound {
                    message: object_id.to_string(),
                }
                .into());
            }
        };

        Ok(DeregisterTableResponse {
            id: request.id.clone(),
            location: Some(table_uri),
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{DirectoryNamespaceBuilder, ManifestNamespace};
    use arrow::datatypes::DataType;
    use bytes::Bytes;
    use lance_core::utils::tempfile::TempStdDir;
    use lance_io::object_store::ObjectStore;
    use lance_namespace::LanceNamespace;
    use lance_namespace::models::{
        CreateNamespaceRequest, CreateTableRequest, DeclareTableRequest, DescribeNamespaceRequest,
        DescribeTableRequest, DropTableRequest, ListTablesRequest, RegisterTableRequest,
        TableExistsRequest,
    };
    use rstest::rstest;
    use std::collections::HashMap;

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
            None,
            false,
        )
        .await
        .unwrap()
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
        use lance_namespace::models::{
            CreateNamespaceRequest, ListNamespacesRequest, NamespaceExistsRequest,
        };

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
        use lance_namespace::models::{
            CreateNamespaceRequest, ListNamespacesRequest, NamespaceExistsRequest,
        };

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
        use lance_namespace::models::CreateNamespaceRequest;

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
        use lance_namespace::models::{
            CreateNamespaceRequest, DropNamespaceRequest, NamespaceExistsRequest,
        };

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
        use lance_namespace::models::{CreateNamespaceRequest, DropNamespaceRequest};

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
        use lance_namespace::models::{
            CreateNamespaceRequest, CreateTableRequest, ListTablesRequest,
        };

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
        use lance_namespace::models::{CreateNamespaceRequest, DescribeNamespaceRequest};

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
    async fn test_concurrent_create_and_drop_single_instance(#[case] inline_optimization: bool) {
        use futures::future::join_all;
        use std::sync::Arc;

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let dir_namespace = Arc::new(
            DirectoryNamespaceBuilder::new(temp_path)
                .inline_optimization_enabled(inline_optimization)
                .build()
                .await
                .unwrap(),
        );

        // Initialize namespace first - create parent namespace to ensure __manifest table
        // is created before concurrent operations
        let mut create_ns_request = CreateNamespaceRequest::new();
        create_ns_request.id = Some(vec!["test_ns".to_string()]);
        dir_namespace
            .create_namespace(create_ns_request)
            .await
            .unwrap();

        let num_tables = 10;
        let mut handles = Vec::new();

        for i in 0..num_tables {
            let ns = dir_namespace.clone();
            let handle = async move {
                let table_name = format!("concurrent_table_{}", i);
                let table_id = vec!["test_ns".to_string(), table_name.clone()];
                let buffer = create_test_ipc_data();

                // Create table
                let mut create_request = CreateTableRequest::new();
                create_request.id = Some(table_id.clone());
                ns.create_table(create_request, Bytes::from(buffer))
                    .await
                    .unwrap_or_else(|e| panic!("Failed to create table {}: {}", table_name, e));

                // Drop table
                let mut drop_request = DropTableRequest::new();
                drop_request.id = Some(table_id);
                ns.drop_table(drop_request)
                    .await
                    .unwrap_or_else(|e| panic!("Failed to drop table {}: {}", table_name, e));

                Ok::<_, lance_core::Error>(())
            };
            handles.push(handle);
        }

        let results = join_all(handles).await;
        for result in results {
            assert!(result.is_ok(), "All concurrent operations should succeed");
        }

        // Verify all tables are dropped
        let mut request = ListTablesRequest::new();
        request.id = Some(vec!["test_ns".to_string()]);
        let response = dir_namespace.list_tables(request).await.unwrap();
        assert_eq!(response.tables.len(), 0, "All tables should be dropped");
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_concurrent_create_and_drop_multiple_instances(#[case] inline_optimization: bool) {
        use futures::future::join_all;

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap().to_string();

        // Initialize namespace first with a single instance to ensure __manifest
        // table is created and parent namespace exists before concurrent operations
        let init_ns = DirectoryNamespaceBuilder::new(&temp_path)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();
        let mut create_ns_request = CreateNamespaceRequest::new();
        create_ns_request.id = Some(vec!["test_ns".to_string()]);
        init_ns.create_namespace(create_ns_request).await.unwrap();

        let num_tables = 10;
        let mut handles = Vec::new();

        for i in 0..num_tables {
            let path = temp_path.clone();
            let handle = async move {
                // Each task creates its own namespace instance
                let ns = DirectoryNamespaceBuilder::new(&path)
                    .inline_optimization_enabled(inline_optimization)
                    .build()
                    .await
                    .unwrap();

                let table_name = format!("multi_ns_table_{}", i);
                let table_id = vec!["test_ns".to_string(), table_name.clone()];
                let buffer = create_test_ipc_data();

                // Create table
                let mut create_request = CreateTableRequest::new();
                create_request.id = Some(table_id.clone());
                ns.create_table(create_request, Bytes::from(buffer))
                    .await
                    .unwrap_or_else(|e| panic!("Failed to create table {}: {}", table_name, e));

                // Drop table
                let mut drop_request = DropTableRequest::new();
                drop_request.id = Some(table_id);
                ns.drop_table(drop_request)
                    .await
                    .unwrap_or_else(|e| panic!("Failed to drop table {}: {}", table_name, e));

                Ok::<_, lance_core::Error>(())
            };
            handles.push(handle);
        }

        let results = join_all(handles).await;
        for result in results {
            assert!(result.is_ok(), "All concurrent operations should succeed");
        }

        // Verify with a fresh namespace instance
        let verify_ns = DirectoryNamespaceBuilder::new(&temp_path)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();

        let mut request = ListTablesRequest::new();
        request.id = Some(vec!["test_ns".to_string()]);
        let response = verify_ns.list_tables(request).await.unwrap();
        assert_eq!(response.tables.len(), 0, "All tables should be dropped");
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_concurrent_create_then_drop_from_different_instance(
        #[case] inline_optimization: bool,
    ) {
        use futures::future::join_all;

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap().to_string();

        // Initialize namespace first with a single instance to ensure __manifest
        // table is created and parent namespace exists before concurrent operations
        let init_ns = DirectoryNamespaceBuilder::new(&temp_path)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();
        let mut create_ns_request = CreateNamespaceRequest::new();
        create_ns_request.id = Some(vec!["test_ns".to_string()]);
        init_ns.create_namespace(create_ns_request).await.unwrap();

        let num_tables = 10;

        // Phase 1: Create all tables concurrently using separate namespace instances
        let mut create_handles = Vec::new();
        for i in 0..num_tables {
            let path = temp_path.clone();
            let handle = async move {
                let ns = DirectoryNamespaceBuilder::new(&path)
                    .inline_optimization_enabled(inline_optimization)
                    .build()
                    .await
                    .unwrap();

                let table_name = format!("cross_instance_table_{}", i);
                let table_id = vec!["test_ns".to_string(), table_name.clone()];
                let buffer = create_test_ipc_data();

                let mut create_request = CreateTableRequest::new();
                create_request.id = Some(table_id);
                ns.create_table(create_request, Bytes::from(buffer))
                    .await
                    .unwrap_or_else(|e| panic!("Failed to create table {}: {}", table_name, e));

                Ok::<_, lance_core::Error>(())
            };
            create_handles.push(handle);
        }

        let create_results = join_all(create_handles).await;
        for result in create_results {
            assert!(result.is_ok(), "All create operations should succeed");
        }

        // Phase 2: Drop all tables concurrently using NEW namespace instances
        let mut drop_handles = Vec::new();
        for i in 0..num_tables {
            let path = temp_path.clone();
            let handle = async move {
                let ns = DirectoryNamespaceBuilder::new(&path)
                    .inline_optimization_enabled(inline_optimization)
                    .build()
                    .await
                    .unwrap();

                let table_name = format!("cross_instance_table_{}", i);
                let table_id = vec!["test_ns".to_string(), table_name.clone()];

                let mut drop_request = DropTableRequest::new();
                drop_request.id = Some(table_id);
                ns.drop_table(drop_request)
                    .await
                    .unwrap_or_else(|e| panic!("Failed to drop table {}: {}", table_name, e));

                Ok::<_, lance_core::Error>(())
            };
            drop_handles.push(handle);
        }

        let drop_results = join_all(drop_handles).await;
        for result in drop_results {
            assert!(result.is_ok(), "All drop operations should succeed");
        }

        // Verify all tables are dropped
        let verify_ns = DirectoryNamespaceBuilder::new(&temp_path)
            .inline_optimization_enabled(inline_optimization)
            .build()
            .await
            .unwrap();

        let mut request = ListTablesRequest::new();
        request.id = Some(vec!["test_ns".to_string()]);
        let response = verify_ns.list_tables(request).await.unwrap();
        assert_eq!(response.tables.len(), 0, "All tables should be dropped");
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

    /// Test that concurrent create_table calls for the same table name don't
    /// create duplicate entries in the manifest. Uses two independent
    /// ManifestNamespace instances pointing at the same directory to simulate
    /// two separate OS processes racing on table creation. The conflict_retries
    /// setting on the MergeInsert ensures the second operation properly detects
    /// the duplicate via WhenMatched::Fail after retrying against the latest data.
    #[tokio::test]
    async fn test_concurrent_create_table_no_duplicates() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        // Two independent namespace instances = two separate "processes"
        // sharing the same underlying filesystem directory.
        let ns1 = DirectoryNamespaceBuilder::new(temp_path)
            .inline_optimization_enabled(false)
            .build()
            .await
            .unwrap();
        let ns2 = DirectoryNamespaceBuilder::new(temp_path)
            .inline_optimization_enabled(false)
            .build()
            .await
            .unwrap();

        let buffer = create_test_ipc_data();

        let mut req1 = CreateTableRequest::new();
        req1.id = Some(vec!["race_table".to_string()]);
        let mut req2 = CreateTableRequest::new();
        req2.id = Some(vec!["race_table".to_string()]);

        // Launch both create_table calls concurrently
        let (result1, result2) = tokio::join!(
            ns1.create_table(req1, Bytes::from(buffer.clone())),
            ns2.create_table(req2, Bytes::from(buffer.clone())),
        );

        // Exactly one should succeed and one should fail
        let success_count = [&result1, &result2].iter().filter(|r| r.is_ok()).count();
        let failure_count = [&result1, &result2].iter().filter(|r| r.is_err()).count();
        assert_eq!(
            success_count, 1,
            "Exactly one create should succeed, got: result1={:?}, result2={:?}",
            result1, result2
        );
        assert_eq!(
            failure_count, 1,
            "Exactly one create should fail, got: result1={:?}, result2={:?}",
            result1, result2
        );

        // Verify only one table entry exists in the manifest
        let ns_check = DirectoryNamespaceBuilder::new(temp_path)
            .inline_optimization_enabled(false)
            .build()
            .await
            .unwrap();
        let mut list_request = ListTablesRequest::new();
        list_request.id = Some(vec![]);
        let response = ns_check.list_tables(list_request).await.unwrap();
        assert_eq!(
            response.tables.len(),
            1,
            "Should have exactly 1 table, found: {:?}",
            response.tables
        );
        assert_eq!(response.tables[0], "race_table");

        // Also verify describe_table works (no "found 2" error)
        let mut describe_request = DescribeTableRequest::new();
        describe_request.id = Some(vec!["race_table".to_string()]);
        let describe_result = ns_check.describe_table(describe_request).await;
        assert!(
            describe_result.is_ok(),
            "describe_table should not fail with duplicate entries: {:?}",
            describe_result
        );
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_add_extended_properties_creates_columns_and_rejects_duplicates(
        #[case] inline_optimization: bool,
    ) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let manifest_ns = create_manifest_namespace_for_test(temp_path, inline_optimization).await;

        let schema = manifest_ns.full_manifest_schema().await.unwrap();
        assert_eq!(
            ManifestNamespace::manifest_schema().fields().len(),
            schema.fields().len()
        );

        manifest_ns
            .add_extended_properties(&vec![
                ("lance.manifest.extended.user_id", DataType::Utf8),
                ("lance.manifest.extended.score", DataType::Int32),
            ])
            .await
            .unwrap();

        let schema = manifest_ns.full_manifest_schema().await.unwrap();
        assert_eq!(
            schema.field_with_name("user_id").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("score").unwrap().data_type(),
            &DataType::Int32
        );

        // Adding the same properties again should fail with an "already exists" error.
        let result = manifest_ns
            .add_extended_properties(&vec![
                ("lance.manifest.extended.user_id", DataType::Utf8),
                ("lance.manifest.extended.score", DataType::Int32),
            ])
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("already exists"), "unexpected error: {}", err);
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_remove_extended_properties_drops_columns_and_errors_on_missing(
        #[case] inline_optimization: bool,
    ) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let mut manifest_ns =
            create_manifest_namespace_for_test(temp_path, inline_optimization).await;

        manifest_ns
            .add_extended_properties(&vec![
                ("lance.manifest.extended.user_id", DataType::Utf8),
                ("lance.manifest.extended.score", DataType::Int32),
            ])
            .await
            .unwrap();

        let schema = manifest_ns.full_manifest_schema().await.unwrap();
        assert!(schema.field_with_name("user_id").is_ok());
        assert!(schema.field_with_name("score").is_ok());

        // Remove an existing column.
        manifest_ns
            .remove_extended_properties(&vec!["lance.manifest.extended.user_id"])
            .await
            .unwrap();
        let schema = manifest_ns.full_manifest_schema().await.unwrap();
        assert!(schema.field_with_name("user_id").is_err());
        assert!(schema.field_with_name("score").is_ok());

        // Removing a missing column should return an error.
        let result = manifest_ns
            .remove_extended_properties(&vec!["lance.manifest.extended.user_id"])
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("doesn't exist"), "unexpected error: {}", err);
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
        let manifest_ns = create_manifest_namespace_for_test(temp_path, inline_optimization).await;

        let mut properties = HashMap::new();
        properties.insert(
            "lance.manifest.extended.user_id".to_string(),
            "123".to_string(),
        );

        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec!["ns1".to_string()]);
        create_req.properties = Some(properties);

        let result = manifest_ns.create_namespace(create_req).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("does not exist in extended properties"));
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

        let mut properties = HashMap::new();
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

        // Ensure extended properties are not duplicated into the metadata JSON.
        let mut scanner = manifest_ns.manifest_scanner().await.unwrap();
        scanner
            .filter("object_type = 'namespace' AND object_id = 'ns1'")
            .unwrap();
        scanner.project(&["metadata", "user_id"]).unwrap();
        let batches = ManifestNamespace::execute_scanner(scanner).await.unwrap();
        let batch = batches
            .into_iter()
            .find(|b| b.num_rows() > 0)
            .expect("expected a non-empty batch");
        let metadata_array = ManifestNamespace::get_string_column(&batch, "metadata").unwrap();
        let metadata_map: HashMap<String, String> =
            serde_json::from_str(metadata_array.value(0)).unwrap();
        assert_eq!(metadata_map.get("owner"), Some(&"alice".to_string()));
        assert!(
            !metadata_map.contains_key("lance.manifest.extended.user_id"),
            "extended keys must not be stored in metadata"
        );
        let user_id_array = ManifestNamespace::get_string_column(&batch, "user_id").unwrap();
        assert_eq!(user_id_array.value(0), "123");
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

        let mut properties = HashMap::new();
        properties.insert(
            "lance.manifest.extended.user_id".to_string(),
            "123".to_string(),
        );
        let buffer = create_test_ipc_data();
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["t1".to_string()]);
        create_req.properties = Some(properties);
        let result = manifest_ns
            .create_table(create_req, Bytes::from(buffer))
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("does not exist in extended properties"),
            "{}",
            err
        );
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_create_table_with_extended_properties_succeeds_and_describe_unified(
        #[case] inline_optimization: bool,
    ) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let manifest_ns = create_manifest_namespace_for_test(temp_path, inline_optimization).await;

        manifest_ns
            .add_extended_properties(&vec![("lance.manifest.extended.user_id", DataType::Utf8)])
            .await
            .unwrap();

        let mut properties = HashMap::new();
        properties.insert("owner".to_string(), "alice".to_string());
        properties.insert(
            "lance.manifest.extended.user_id".to_string(),
            "123".to_string(),
        );

        let buffer = create_test_ipc_data();
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        create_req.properties = Some(properties);
        manifest_ns
            .create_table(create_req, Bytes::from(buffer))
            .await
            .unwrap();

        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_table".to_string()]);
        let resp = manifest_ns.describe_table(describe_req).await.unwrap();
        let props = resp.properties.expect("properties should be present");
        assert_eq!(props.get("owner"), Some(&"alice".to_string()));
        assert_eq!(
            props.get("lance.manifest.extended.user_id"),
            Some(&"123".to_string())
        );

        // Ensure extended properties are not duplicated into the metadata JSON.
        let mut scanner = manifest_ns.manifest_scanner().await.unwrap();
        scanner
            .filter("object_type = 'table' AND object_id = 'test_table'")
            .unwrap();
        scanner.project(&["metadata", "user_id"]).unwrap();
        let batches = ManifestNamespace::execute_scanner(scanner).await.unwrap();
        let batch = batches
            .into_iter()
            .find(|b| b.num_rows() > 0)
            .expect("expected a non-empty batch");
        let metadata_array = ManifestNamespace::get_string_column(&batch, "metadata").unwrap();
        let metadata_map: HashMap<String, String> =
            serde_json::from_str(metadata_array.value(0)).unwrap();
        assert_eq!(metadata_map.get("owner"), Some(&"alice".to_string()));
        assert!(
            !metadata_map.contains_key("lance.manifest.extended.user_id"),
            "extended keys must not be stored in metadata"
        );
        let user_id_array = ManifestNamespace::get_string_column(&batch, "user_id").unwrap();
        assert_eq!(user_id_array.value(0), "123");
    }

    #[rstest]
    #[case::with_optimization(true)]
    #[case::without_optimization(false)]
    #[tokio::test]
    async fn test_register_and_declare_table_with_extended_properties_without_columns_fails(
        #[case] inline_optimization: bool,
    ) {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let manifest_ns = create_manifest_namespace_for_test(temp_path, inline_optimization).await;

        let properties = HashMap::from([(
            "lance.manifest.extended.user_id".to_string(),
            "123".to_string(),
        )]);

        // register_table: should reject unknown extended columns.
        let mut register_req = RegisterTableRequest::new("registered_table.lance".to_string());
        register_req.id = Some(vec!["registered_table".to_string()]);
        register_req.properties = Some(properties.clone());
        let result = LanceNamespace::register_table(&manifest_ns, register_req).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("does not exist in extended properties"),
            "{}",
            err
        );

        // declare_table: should reject unknown extended columns.
        let mut declare_req = DeclareTableRequest::new();
        declare_req.id = Some(vec!["declared_table".to_string()]);
        declare_req.properties = Some(properties);
        let result = manifest_ns.declare_table(declare_req).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("does not exist in extended properties"),
            "{}",
            err
        );
    }
}
