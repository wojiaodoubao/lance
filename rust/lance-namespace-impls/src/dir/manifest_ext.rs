// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::dir::manifest::ObjectType;
use crate::ManifestNamespace;
use arrow::datatypes::Schema as ArrowSchema;
use async_trait::async_trait;
use lance::deps::arrow_array::{
    new_null_array, ArrayRef, RecordBatch, RecordBatchIterator, StringArray,
};
use lance::deps::datafusion::common::ScalarValue;
use lance_core::{box_error, Error};
use lance_namespace_reqwest_client::models::{
    CreateNamespaceRequest, DeclareTableRequest, DeclareTableResponse,
};
use snafu::location;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

/// Request for creating multiple namespaces with a single merge insert.
#[derive(Debug, Clone)]
pub struct CreateMultiNamespacesRequest {
    pub namespaces: Vec<CreateNamespaceRequest>,
    /// Columns used for merge insert deduplication.
    pub on: Vec<String>,
}

#[derive(Debug, Default, Clone)]
pub struct CreateMultiNamespacesRequestBuilder {
    namespaces: Vec<CreateNamespaceRequest>,
    on: Option<Vec<String>>,
}

impl CreateMultiNamespacesRequestBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn namespaces(mut self, namespaces: Vec<CreateNamespaceRequest>) -> Self {
        self.namespaces = namespaces;
        self
    }

    pub fn push_namespace(mut self, namespace: CreateNamespaceRequest) -> Self {
        self.namespaces.push(namespace);
        self
    }

    pub fn on(mut self, on: Vec<String>) -> Self {
        self.on = Some(on);
        self
    }

    pub fn build(self) -> CreateMultiNamespacesRequest {
        let on = self.on.unwrap_or_else(|| vec!["object_id".to_string()]);
        CreateMultiNamespacesRequest {
            namespaces: self.namespaces,
            on,
        }
    }
}

#[async_trait]
pub trait ManifestNamespaceExt {
    /// Create multiple namespaces atomically with extended properties.
    async fn create_multi_namespaces_extended(
        &self,
        request: CreateMultiNamespacesRequest,
        extended_records: Vec<Option<RecordBatch>>,
    ) -> lance_core::Result<()>;

    /// Declare a table with extended properties (metadata only operation).
    async fn declare_table_extended(
        &self,
        request: DeclareTableRequest,
        extended_record: Option<RecordBatch>,
    ) -> lance_core::Result<DeclareTableResponse>;
}

#[async_trait]
impl ManifestNamespaceExt for ManifestNamespace {
    async fn create_multi_namespaces_extended(
        &self,
        request: CreateMultiNamespacesRequest,
        extended_records: Vec<Option<RecordBatch>>,
    ) -> lance_core::Result<()> {
        if request.namespaces.is_empty() {
            return Ok(());
        }

        if !extended_records.is_empty() && extended_records.len() != request.namespaces.len() {
            return Err(Error::InvalidInput {
                source: format!(
                    "extended_records length {} must match namespaces length {}",
                    extended_records.len(),
                    request.namespaces.len()
                )
                .into(),
                location: location!(),
            });
        }

        let mut object_id_vec: Vec<String> = Vec::with_capacity(request.namespaces.len());
        let mut metadata_vec: Vec<Option<String>> = Vec::with_capacity(request.namespaces.len());
        let mut props_vec: Vec<Option<HashMap<String, String>>> =
            Vec::with_capacity(request.namespaces.len());

        // Allow creating nested namespaces in the same batch: parent namespaces that are
        // also part of this request are treated as existing for validation.
        let mut to_create: HashSet<String> = HashSet::with_capacity(request.namespaces.len());
        for ns_req in request.namespaces.iter() {
            if let Some(id) = ns_req.id.as_ref() {
                if !id.is_empty() {
                    to_create.insert(id.join(crate::dir::manifest::DELIMITER));
                }
            }
        }

        for ns_req in request.namespaces.iter() {
            let namespace_id = ns_req.id.as_ref().ok_or_else(|| Error::InvalidInput {
                source: "Namespace ID is required".into(),
                location: location!(),
            })?;

            if namespace_id.is_empty() {
                return Err(Error::Namespace {
                    source: "Root namespace already exists and cannot be created".into(),
                    location: location!(),
                });
            }

            if namespace_id.len() > 1 {
                // Validate parent namespaces that are NOT included in this batch.
                // If a parent is included in this batch, it will be inserted together.
                for i in 1..namespace_id.len() {
                    let parent = &namespace_id[..i];
                    let parent_object_id = parent.join(crate::dir::manifest::DELIMITER);
                    if !to_create.contains(&parent_object_id) {
                        self.validate_namespace_levels_exist(parent).await?;
                    }
                }
            }

            object_id_vec.push(namespace_id.join(crate::dir::manifest::DELIMITER));
            metadata_vec.push(Self::build_metadata_json(&ns_req.properties));
            props_vec.push(ns_req.properties.clone());
        }

        let dataset_guard = self.manifest_dataset.get().await?;
        let full_schema = Arc::new(ArrowSchema::from(dataset_guard.schema()));
        drop(dataset_guard);

        let n = object_id_vec.len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(full_schema.fields().len());

        // Ensure each extended record has exactly 1 row if provided.
        if !extended_records.is_empty() {
            for (idx, r) in extended_records.iter().enumerate() {
                if let Some(r) = r {
                    if r.num_rows() != 1 {
                        return Err(Error::InvalidInput {
                            source: format!(
                                "extended_records[{}] must have exactly 1 row, got {}",
                                idx,
                                r.num_rows()
                            )
                            .into(),
                            location: location!(),
                        });
                    }
                }
            }
        }

        for f in full_schema.fields() {
            let name = f.name().as_str();
            match name {
                "object_id" => columns.push(Arc::new(StringArray::from(object_id_vec.clone()))),
                "object_type" => columns.push(Arc::new(StringArray::from(vec!["namespace"; n]))),
                "location" => columns.push(Arc::new(StringArray::from(vec![None::<String>; n]))),
                "metadata" => columns.push(Arc::new(StringArray::from(metadata_vec.clone()))),
                "base_objects" => columns.push(new_null_array(f.data_type(), n)),
                _ => {
                    let key = format!("{}{}", crate::dir::manifest::EXTENDED_PREFIX, name);
                    let null_scalar =
                        ScalarValue::try_from(f.data_type()).map_err(|e| Error::IO {
                            source: box_error(std::io::Error::other(format!(
                                "Failed to create null scalar for column {}: {}",
                                name, e
                            ))),
                            location: location!(),
                        })?;

                    let mut scalars: Vec<ScalarValue> = Vec::with_capacity(n);
                    for i in 0..n {
                        // Prefer the value from extended_records if present.
                        let mut from_extended: Option<ScalarValue> = None;
                        if !extended_records.is_empty() {
                            if let Some(record) = extended_records[i].as_ref() {
                                if let Some((col_idx, field)) =
                                    record.schema().column_with_name(name)
                                {
                                    if field.data_type() != f.data_type() {
                                        return Err(Error::InvalidInput {
                                            source: format!(
                                                "extended_records[{}] column '{}' has type {:?}, expected {:?}",
                                                i,
                                                name,
                                                field.data_type(),
                                                f.data_type()
                                            )
                                            .into(),
                                            location: location!(),
                                        });
                                    }
                                    let col = record.column(col_idx);
                                    let scalar = ScalarValue::try_from_array(col.as_ref(), 0)
                                        .map_err(|e| Error::Internal {
                                            message: format!(
                                                "Failed to convert extended_records[{}] column '{}' to scalar: {}",
                                                i, name, e
                                            ),
                                            location: location!(),
                                        })?;
                                    from_extended = Some(scalar);
                                }
                            }
                        }

                        if let Some(s) = from_extended {
                            scalars.push(s);
                            continue;
                        }

                        // Fallback to properties-based extended values.
                        let v = props_vec[i].as_ref().and_then(|m| m.get(&key));
                        match v {
                            Some(s) if s != "null" && !s.is_empty() => {
                                scalars
                                    .push(crate::dir::manifest::scalar_from_str(f.data_type(), s)?);
                            }
                            _ => scalars.push(null_scalar.clone()),
                        }
                    }

                    let array =
                        ScalarValue::iter_to_array(scalars.into_iter()).map_err(|e| Error::IO {
                            source: box_error(std::io::Error::other(format!(
                                "Failed to build array for column {}: {}",
                                name, e
                            ))),
                            location: location!(),
                        })?;
                    columns.push(array);
                }
            }
        }

        let batch = RecordBatch::try_new(full_schema.clone(), columns).map_err(|e| {
            Error::io(
                format!("Failed to create manifest batch: {}", e),
                location!(),
            )
        })?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], full_schema);

        let dataset_guard = self.manifest_dataset.get().await?;
        let dataset_arc = Arc::new(dataset_guard.clone());
        drop(dataset_guard);

        let mut merge_builder =
            lance::dataset::MergeInsertBuilder::try_new(dataset_arc, request.on).map_err(|e| {
                Error::IO {
                    source: box_error(std::io::Error::other(format!(
                        "Failed to create merge builder: {}",
                        e
                    ))),
                    location: location!(),
                }
            })?;
        merge_builder.when_matched(lance::dataset::WhenMatched::DoNothing);
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
            .map_err(|e| Error::IO {
                source: box_error(std::io::Error::other(format!(
                    "Failed to execute merge: {}",
                    e
                ))),
                location: location!(),
            })?;

        let new_dataset = Arc::try_unwrap(new_dataset_arc).unwrap_or_else(|arc| (*arc).clone());
        self.manifest_dataset.set_latest(new_dataset).await;
        if let Err(e) = self.run_inline_optimization().await {
            log::warn!(
                "Unexpected failure when running inline optimization: {:?}",
                e
            );
        }

        Ok(())
    }

    async fn declare_table_extended(
        &self,
        request: DeclareTableRequest,
        extended_record: Option<RecordBatch>,
    ) -> lance_core::Result<DeclareTableResponse> {
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

        // Check if table already exists in manifest
        let existing = self.query_manifest_for_table(&object_id).await?;
        if existing.is_some() {
            return Err(Error::Namespace {
                source: format!("Table '{}' already exists", table_name).into(),
                location: location!(),
            });
        }

        // Serialize properties and compute extended batch if provided
        let metadata = Self::build_metadata_json(&request.properties);

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
                        "Cannot declare table {} at location {}, must be at location {}",
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

        // Add entry to manifest marking this as a declared table (store dir_name, not full path)
        self.insert_into_manifest_with_metadata(
            object_id,
            ObjectType::Table,
            Some(dir_name),
            metadata,
            None,
            extended_record,
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
            properties: request.properties.clone(),
            ..Default::default()
        })
    }
}
