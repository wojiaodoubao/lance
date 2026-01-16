// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::io::Cursor;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::ipc::reader::StreamReader;
use bytes::Bytes;
use jni::objects::JValue;
use jni::objects::{JByteArray, JMap, JObject, JString};
use jni::sys::{jbyteArray, jlong, jstring};
use jni::JNIEnv;
use lance_namespace::models::*;
use lance_namespace::LanceNamespace as LanceNamespaceTrait;
use lance_namespace_impls::partition::{
    parse_filter_expr_from_sql, PartitionField, PartitionTable, PartitionedNamespace,
    PartitionedNamespaceBuilder,
};
use lance_namespace_impls::DirectoryNamespaceBuilder;
use lance_namespace_reqwest_client::models::PartitionField as JsonPartitionField;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::namespace::JavaDynamicContextProvider;
use crate::utils::to_rust_map;
use crate::RT;

fn java_schema_to_rust_schema(
    env: &mut JNIEnv,
    partitioned_ns_obj: &JObject,
    schema_obj: &JObject,
) -> Result<ArrowSchema> {
    let schema_ipc_obj = env
        .call_method(
            partitioned_ns_obj,
            "schemaToIpc",
            "(Lorg/apache/arrow/vector/types/pojo/Schema;)[B",
            &[JValue::Object(schema_obj)],
        )
        .map_err(|e| Error::runtime_error(format!("Failed to call schemaToIpc: {}", e)))?
        .l()
        .map_err(|e| Error::runtime_error(format!("schemaToIpc did not return object: {}", e)))?;
    let schema_ipc = env
        .convert_byte_array(JByteArray::from(schema_ipc_obj))
        .map_err(|e| Error::runtime_error(format!("Failed to read schema IPC bytes: {}", e)))?;

    let reader = StreamReader::try_new(Cursor::new(schema_ipc), None)
        .map_err(|e| Error::runtime_error(format!("Failed to decode schema IPC: {}", e)))?;
    Ok(reader.schema().as_ref().clone())
}

fn java_partition_fields_to_rust_partition_fields(
    env: &mut JNIEnv,
    partition_fields_list: &JObject,
) -> Result<Vec<PartitionField>> {
    if partition_fields_list.is_null() {
        return Err(Error::input_error("partitionFields is null".to_string()));
    }

    let pf_json = java_object_to_json(env, partition_fields_list)?;
    let partition_fields: Vec<JsonPartitionField> = serde_json::from_str(&pf_json)
        .map_err(|e| Error::input_error(format!("Invalid partition fields JSON: {}", e)))?;

    let mut fields = Vec::with_capacity(partition_fields.len());
    for jf in &partition_fields {
        fields.push(
            PartitionField::from_json(jf)
                .map_err(|e| Error::input_error(format!("Invalid partition field: {}", e)))?,
        );
    }
    Ok(fields)
}

fn java_object_to_json(env: &mut JNIEnv, obj: &JObject) -> Result<String> {
    let object_mapper_class = env
        .find_class("com/fasterxml/jackson/databind/ObjectMapper")
        .map_err(|e| Error::runtime_error(format!("Failed to find ObjectMapper class: {}", e)))?;
    let object_mapper = env
        .new_object(&object_mapper_class, "()V", &[])
        .map_err(|e| Error::runtime_error(format!("Failed to create ObjectMapper: {}", e)))?;

    let json_obj = env
        .call_method(
            &object_mapper,
            "writeValueAsString",
            "(Ljava/lang/Object;)Ljava/lang/String;",
            &[JValue::Object(obj)],
        )
        .map_err(|e| {
            Error::runtime_error(format!(
                "Failed to serialize object via ObjectMapper: {}",
                e
            ))
        })?
        .l()
        .map_err(|e| {
            Error::runtime_error(format!("writeValueAsString did not return object: {}", e))
        })?;
    let json: String = env
        .get_string(&JString::from(json_obj))
        .map_err(|e| Error::runtime_error(format!("Failed to convert JSON string: {}", e)))?
        .into();
    Ok(json)
}

/// Blocking wrapper for PartitionedNamespace
pub struct BlockingPartitionedNamespace {
    pub(crate) inner: PartitionedNamespace,
}

#[derive(Debug, Serialize)]
struct JavaPartitionTable {
    id: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    read_version: Option<u64>,
}

#[derive(Debug, Serialize)]
struct JavaPlanScanItem {
    table: JavaPartitionTable,
    refine_expr: String,
}

fn to_java_partition_table(t: &PartitionTable) -> JavaPartitionTable {
    JavaPartitionTable {
        id: t.id.clone(),
        read_version: t.read_version,
    }
}

/// Helper function to call namespace methods that return a response object (PartitionedNamespace)
fn call_partitioned_namespace_method<'local, Req, Resp, F>(
    env: &mut JNIEnv<'local>,
    handle: jlong,
    request_json: JString,
    f: F,
) -> Result<JString<'local>>
where
    Req: for<'de> Deserialize<'de>,
    Resp: Serialize,
    F: FnOnce(&BlockingPartitionedNamespace, Req) -> lance_core::Result<Resp>,
{
    let namespace = unsafe { &*(handle as *const BlockingPartitionedNamespace) };
    let request_str: String = env.get_string(&request_json)?.into();
    let request: Req = serde_json::from_str(&request_str)
        .map_err(|e| Error::input_error(format!("Failed to parse request JSON: {}", e)))?;

    let response = f(namespace, request)
        .map_err(|e| Error::runtime_error(format!("Namespace operation failed: {}", e)))?;

    let response_json = serde_json::to_string(&response)
        .map_err(|e| Error::runtime_error(format!("Failed to serialize response: {}", e)))?;

    env.new_string(response_json).map_err(Into::into)
}

/// Helper function for void methods (PartitionedNamespace)
fn call_partitioned_namespace_void_method<Req, F>(
    env: &mut JNIEnv,
    handle: jlong,
    request_json: JString,
    f: F,
) -> Result<()>
where
    Req: for<'de> Deserialize<'de>,
    F: FnOnce(&BlockingPartitionedNamespace, Req) -> lance_core::Result<()>,
{
    let namespace = unsafe { &*(handle as *const BlockingPartitionedNamespace) };
    let request_str: String = env.get_string(&request_json)?.into();
    let request: Req = serde_json::from_str(&request_str)
        .map_err(|e| Error::input_error(format!("Failed to parse request JSON: {}", e)))?;

    f(namespace, request)
        .map_err(|e| Error::runtime_error(format!("Namespace operation failed: {}", e)))?;

    Ok(())
}

/// Helper function for count methods (PartitionedNamespace)
fn call_partitioned_namespace_count_method(
    env: &mut JNIEnv,
    handle: jlong,
    request_json: JString,
) -> Result<jlong> {
    let namespace = unsafe { &*(handle as *const BlockingPartitionedNamespace) };
    let request_str: String = env.get_string(&request_json)?.into();
    let request: CountTableRowsRequest = serde_json::from_str(&request_str)
        .map_err(|e| Error::input_error(format!("Failed to parse request JSON: {}", e)))?;

    let count = RT
        .block_on(namespace.inner.count_table_rows(request))
        .map_err(|e| Error::runtime_error(format!("Count table rows failed: {}", e)))?;

    Ok(count)
}

/// Helper function for methods with data parameter (PartitionedNamespace)
fn call_partitioned_namespace_with_data_method<'local, Req, Resp, F>(
    env: &mut JNIEnv<'local>,
    handle: jlong,
    request_json: JString,
    request_data: JByteArray,
    f: F,
) -> Result<JString<'local>>
where
    Req: for<'de> Deserialize<'de>,
    Resp: Serialize,
    F: FnOnce(&BlockingPartitionedNamespace, Req, Bytes) -> lance_core::Result<Resp>,
{
    let namespace = unsafe { &*(handle as *const BlockingPartitionedNamespace) };
    let request_str: String = env.get_string(&request_json)?.into();
    let request: Req = serde_json::from_str(&request_str)
        .map_err(|e| Error::input_error(format!("Failed to parse request JSON: {}", e)))?;

    let data_vec = env.convert_byte_array(request_data)?;
    let data = bytes::Bytes::from(data_vec);

    let response = f(namespace, request, data)
        .map_err(|e| Error::runtime_error(format!("Namespace operation failed: {}", e)))?;

    let response_json = serde_json::to_string(&response)
        .map_err(|e| Error::runtime_error(format!("Failed to serialize response: {}", e)))?;

    env.new_string(response_json).map_err(Into::into)
}

/// Helper function for query methods that return byte arrays (PartitionedNamespace)
fn call_partitioned_namespace_query_method<'local>(
    env: &mut JNIEnv<'local>,
    handle: jlong,
    request_json: JString,
) -> Result<JByteArray<'local>> {
    let namespace = unsafe { &*(handle as *const BlockingPartitionedNamespace) };
    let request_str: String = env.get_string(&request_json)?.into();
    let request: QueryTableRequest = serde_json::from_str(&request_str)
        .map_err(|e| Error::input_error(format!("Failed to parse request JSON: {}", e)))?;

    let result_bytes = RT
        .block_on(namespace.inner.query_table(request))
        .map_err(|e| Error::runtime_error(format!("Query table failed: {}", e)))?;

    let byte_array = env.byte_array_from_slice(&result_bytes)?;
    Ok(byte_array)
}

// ============================================================================
// PartitionedNamespace JNI Functions
// ============================================================================

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_createNative(
    mut env: JNIEnv,
    _obj: JObject,
    properties_map: JObject,
) -> jlong {
    ok_or_throw_with_return!(
        env,
        create_partitioned_namespace_internal(&mut env, properties_map, None),
        0
    )
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_createNativeWithProvider(
    mut env: JNIEnv,
    _obj: JObject,
    properties_map: JObject,
    context_provider: JObject,
) -> jlong {
    ok_or_throw_with_return!(
        env,
        create_partitioned_namespace_internal(&mut env, properties_map, Some(context_provider)),
        0
    )
}

fn create_partitioned_namespace_internal(
    env: &mut JNIEnv,
    properties_map: JObject,
    context_provider: Option<JObject>,
) -> Result<jlong> {
    // Convert Java HashMap to Rust HashMap
    let jmap = JMap::from_env(env, &properties_map)?;
    let mut properties = to_rust_map(env, &jmap)?;

    // Use the same key as DirectoryNamespace to locate root.
    if !properties.contains_key("root") {
        if let Some(location) = properties.get("location").cloned() {
            properties.insert("root".to_string(), location);
        }
    }
    let location = properties.get("root").cloned().ok_or_else(|| {
        Error::input_error("Missing 'root' (or 'location') in configProperties".to_string())
    })?;

    // Build DirectoryNamespace using properties so we can reuse storage options, credential vending,
    // and the Java dynamic context provider.
    let mut dir_builder = DirectoryNamespaceBuilder::from_properties(properties, None)
        .map_err(|e| {
            Error::runtime_error(format!("Failed to create DirectoryNamespaceBuilder: {}", e))
        })?
        .manifest_enabled(true)
        .dir_listing_enabled(false)
        .inline_optimization_enabled(true);

    if let Some(provider_obj) = context_provider {
        if !provider_obj.is_null() {
            let java_provider = JavaDynamicContextProvider::new(env, &provider_obj)?;
            dir_builder = dir_builder.context_provider(Arc::new(java_provider));
        }
    }

    let directory = RT
        .block_on(dir_builder.build())
        .map_err(|e| Error::runtime_error(format!("Failed to build DirectoryNamespace: {}", e)))?;

    let builder = PartitionedNamespaceBuilder::new(location).directory(directory);
    let ns = RT
        .block_on(builder.load())
        .map_err(|e| Error::runtime_error(format!("Failed to load PartitionedNamespace: {}", e)))?;

    Ok(Box::into_raw(Box::new(BlockingPartitionedNamespace { inner: ns })) as jlong)
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_releaseNative(
    _env: JNIEnv,
    _obj: JObject,
    handle: jlong,
) {
    if handle != 0 {
        unsafe {
            let _ = Box::from_raw(handle as *mut BlockingPartitionedNamespace);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_namespaceIdNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
) -> jstring {
    let ns = unsafe { &*(handle as *const BlockingPartitionedNamespace) };
    let namespace_id = ns.inner.namespace_id();
    ok_or_throw_with_return!(
        env,
        env.new_string(namespace_id).map_err(Error::from),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_listNamespacesNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_method(&mut env, handle, request_json, |ns, req| {
            RT.block_on(ns.inner.list_namespaces(req))
        }),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_describeNamespaceNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_method(&mut env, handle, request_json, |ns, req| {
            RT.block_on(ns.inner.describe_namespace(req))
        }),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_createNamespaceNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_method(&mut env, handle, request_json, |ns, req| {
            RT.block_on(ns.inner.create_namespace(req))
        }),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_dropNamespaceNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_method(&mut env, handle, request_json, |ns, req| {
            RT.block_on(ns.inner.drop_namespace(req))
        }),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_namespaceExistsNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) {
    ok_or_throw_without_return!(
        env,
        call_partitioned_namespace_void_method(&mut env, handle, request_json, |ns, req| {
            RT.block_on(ns.inner.namespace_exists(req))
        })
    )
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_listTablesNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_method(&mut env, handle, request_json, |ns, req| {
            RT.block_on(ns.inner.list_tables(req))
        }),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_describeTableNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_method(&mut env, handle, request_json, |ns, req| {
            RT.block_on(ns.inner.describe_table(req))
        }),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_registerTableNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_method(&mut env, handle, request_json, |ns, req| {
            RT.block_on(ns.inner.register_table(req))
        }),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_tableExistsNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) {
    ok_or_throw_without_return!(
        env,
        call_partitioned_namespace_void_method(&mut env, handle, request_json, |ns, req| {
            RT.block_on(ns.inner.table_exists(req))
        })
    )
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_dropTableNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_method(&mut env, handle, request_json, |ns, req| {
            RT.block_on(ns.inner.drop_table(req))
        }),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_deregisterTableNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_method(&mut env, handle, request_json, |ns, req| {
            RT.block_on(ns.inner.deregister_table(req))
        }),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_countTableRowsNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) -> jlong {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_count_method(&mut env, handle, request_json),
        0
    )
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_createTableNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
    request_data: JByteArray,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_with_data_method(
            &mut env,
            handle,
            request_json,
            request_data,
            |ns, req, data| { RT.block_on(ns.inner.create_table(req, data)) }
        ),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
#[allow(deprecated)]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_createEmptyTableNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_method(&mut env, handle, request_json, |ns, req| {
            RT.block_on(ns.inner.create_empty_table(req))
        }),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_declareTableNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_method(&mut env, handle, request_json, |ns, req| {
            RT.block_on(ns.inner.declare_table(req))
        }),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_insertIntoTableNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
    request_data: JByteArray,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_with_data_method(
            &mut env,
            handle,
            request_json,
            request_data,
            |ns, req, data| { RT.block_on(ns.inner.insert_into_table(req, data)) }
        ),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_mergeInsertIntoTableNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
    request_data: JByteArray,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_with_data_method(
            &mut env,
            handle,
            request_json,
            request_data,
            |ns, req, data| { RT.block_on(ns.inner.merge_insert_into_table(req, data)) }
        ),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_updateTableNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_method(&mut env, handle, request_json, |ns, req| {
            RT.block_on(ns.inner.update_table(req))
        }),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_deleteFromTableNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_method(&mut env, handle, request_json, |ns, req| {
            RT.block_on(ns.inner.delete_from_table(req))
        }),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_queryTableNative<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) -> jbyteArray {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_query_method(&mut env, handle, request_json),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_createTableIndexNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_method(&mut env, handle, request_json, |ns, req| {
            RT.block_on(ns.inner.create_table_index(req))
        }),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_listTableIndicesNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_method(&mut env, handle, request_json, |ns, req| {
            RT.block_on(ns.inner.list_table_indices(req))
        }),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_describeTableIndexStatsNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_method(&mut env, handle, request_json, |ns, req| {
            RT.block_on(ns.inner.describe_table_index_stats(req))
        }),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_describeTransactionNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_method(&mut env, handle, request_json, |ns, req| {
            RT.block_on(ns.inner.describe_transaction(req))
        }),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_alterTransactionNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    request_json: JString,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        call_partitioned_namespace_method(&mut env, handle, request_json, |ns, req| {
            RT.block_on(ns.inner.alter_transaction(req))
        }),
        std::ptr::null_mut()
    )
    .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_schemaNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
) -> jbyteArray {
    ok_or_throw_with_return!(
        env,
        (|| {
            use arrow::ipc::writer::StreamWriter;
            let ns = unsafe { &*(handle as *const BlockingPartitionedNamespace) };
            let schema = Arc::new(ns.inner.schema()?);

            // Write a schema-only Arrow IPC stream.
            let mut data = Vec::new();
            let mut writer = StreamWriter::try_new(&mut data, &schema).map_err(|e| {
                Error::runtime_error(format!("Failed to create StreamWriter: {}", e))
            })?;
            writer.finish().map_err(|e| {
                Error::runtime_error(format!("Failed to finish schema stream: {}", e))
            })?;

            Ok::<jbyteArray, Error>(env.byte_array_from_slice(&data)?.into_raw())
        })(),
        std::ptr::null_mut()
    )
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_planScanNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    filter: JString,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        (|| {
            let ns = unsafe { &*(handle as *const BlockingPartitionedNamespace) };
            let filter: String = env.get_string(&filter)?.into();
            let arrow_schema: ArrowSchema = ns.inner.schema()?;
            let expr = RT
                .block_on(parse_filter_expr_from_sql(&filter, &arrow_schema))
                .map_err(|e| Error::runtime_error(format!("Failed to parse filter SQL: {}", e)))?;
            let planned = RT
                .block_on(ns.inner.plan_scan(&expr))
                .map_err(|e| Error::runtime_error(format!("plan_scan failed: {}", e)))?;

            let items: Vec<JavaPlanScanItem> = planned
                .into_iter()
                .map(|(t, refine)| JavaPlanScanItem {
                    table: to_java_partition_table(&t),
                    refine_expr: refine.to_string(),
                })
                .collect();

            let json = serde_json::to_string(&items)
                .map_err(|e| Error::runtime_error(format!("Failed to serialize plan: {}", e)))?;
            Ok::<jstring, Error>(env.new_string(json)?.into_raw())
        })(),
        std::ptr::null_mut()
    )
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_partitionSpecNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        (|| {
            let ns = unsafe { &*(handle as *const BlockingPartitionedNamespace) };
            let spec = RT
                .block_on(ns.inner.partition_spec())
                .map_err(|e| Error::runtime_error(format!("partition_spec failed: {}", e)))?
                .to_json()?;
            let json = serde_json::to_string(&spec).map_err(|e| {
                Error::runtime_error(format!("Failed to serialize partition spec: {}", e))
            })?;
            Ok::<jstring, Error>(env.new_string(json)?.into_raw())
        })(),
        std::ptr::null_mut()
    )
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_updateTableSpecNative(
    mut env: JNIEnv,
    obj: JObject,
    handle: jlong,
    schema_obj: JObject,
    partition_fields_obj: JObject,
) {
    ok_or_throw_without_return!(
        env,
        (|| {
            let ns = unsafe { &mut *(handle as *mut BlockingPartitionedNamespace) };

            if schema_obj.is_null() {
                return Err(Error::input_error("schema is null".to_string()));
            }

            let schema = java_schema_to_rust_schema(&mut env, &obj, &schema_obj)?;
            let partition_fields =
                java_partition_fields_to_rust_partition_fields(&mut env, &partition_fields_obj)?;

            RT.block_on(ns.inner.update_table_spec(schema, partition_fields))
                .map_err(|e| Error::runtime_error(format!("update_table_spec failed: {}", e)))?;
            Ok(())
        })()
    )
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_resolveOrCreatePartitionTableNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    arrow_array_stream_addr: jlong,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        (|| {
            use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
            let ns = unsafe { &*(handle as *const BlockingPartitionedNamespace) };
            let stream_ptr = arrow_array_stream_addr as *mut FFI_ArrowArrayStream;
            let mut reader =
                unsafe { ArrowArrayStreamReader::from_raw(stream_ptr) }.map_err(|e| {
                    Error::runtime_error(format!("Failed to import ArrowArrayStream: {}", e))
                })?;

            let batch = reader
                .next()
                .transpose()
                .map_err(|e| Error::runtime_error(format!("Failed to read record batch: {}", e)))?
                .ok_or_else(|| Error::input_error("Empty ArrowArrayStream".to_string()))?;

            if batch.num_rows() != 1 {
                return Err(Error::input_error(format!(
                    "resolve_or_create_partition_table expects exactly 1 row, got {}",
                    batch.num_rows()
                )));
            }

            let table = RT
                .block_on(ns.inner.resolve_or_create_partition_table(&batch))
                .map_err(|e| {
                    Error::runtime_error(format!("resolve_or_create_partition_table failed: {}", e))
                })?;
            let json = serde_json::to_string(&to_java_partition_table(&table))
                .map_err(|e| Error::runtime_error(format!("Failed to serialize table: {}", e)))?;
            Ok::<jstring, Error>(env.new_string(json)?.into_raw())
        })(),
        std::ptr::null_mut()
    )
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_tablesNative(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
) -> jstring {
    ok_or_throw_with_return!(
        env,
        (|| {
            let ns = unsafe { &*(handle as *const BlockingPartitionedNamespace) };
            let tables = RT.block_on(ns.inner.tables()).map_err(|e| {
                Error::runtime_error(format!("PartitionedNamespace.tables failed: {}", e))
            })?;

            let java_tables: Vec<JavaPartitionTable> =
                tables.iter().map(to_java_partition_table).collect();
            let json = serde_json::to_string(&java_tables)
                .map_err(|e| Error::runtime_error(format!("Failed to serialize tables: {}", e)))?;

            Ok::<jstring, Error>(env.new_string(json)?.into_raw())
        })(),
        std::ptr::null_mut()
    )
}

#[no_mangle]
pub extern "system" fn Java_org_lance_namespace_PartitionedNamespace_commitNative(
    mut env: JNIEnv,
    _obj: JObject,
    _handle: jlong,
    _read_version_json: JObject,
    _new_version_json: JObject,
) -> jstring {
    let err = Error::runtime_error("PartitionedNamespace.commit is not implemented".to_string());
    err.throw(&mut env);
    std::ptr::null_mut()
}
