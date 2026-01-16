/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.namespace;

import org.lance.JniLoader;
import org.lance.namespace.model.*;
import org.lance.schema.LanceSchema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.memory.BufferAllocator;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/** Java wrapper for the native Rust PartitionedNamespace implementation. */
public final class PartitionedNamespace implements LanceNamespace, Closeable {
  static {
    JniLoader.ensureLoaded();
  }

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private long nativeHandle;

  public PartitionedNamespace() {}

  PartitionedNamespace(long nativeHandle) {
    Preconditions.checkArgument(nativeHandle != 0, "nativeHandle is 0");
    this.nativeHandle = nativeHandle;
  }

  @Override
  public void initialize(Map<String, String> configProperties, BufferAllocator allocator) {
    initialize(configProperties, allocator, null);
  }

  /** Initialize with a dynamic context provider. */
  public void initialize(
      Map<String, String> configProperties,
      BufferAllocator allocator,
      DynamicContextProvider contextProvider) {
    Preconditions.checkNotNull(configProperties, "configProperties is null");
    Preconditions.checkNotNull(allocator, "allocator is null");
    Preconditions.checkArgument(nativeHandle == 0, "PartitionedNamespace already initialized");
    if (contextProvider != null) {
      this.nativeHandle = createNativeWithProvider(configProperties, contextProvider);
    } else {
      this.nativeHandle = createNative(configProperties);
    }
  }

  @Override
  public String namespaceId() {
    ensureOpen();
    return namespaceIdNative(nativeHandle);
  }

  @Override
  public ListNamespacesResponse listNamespaces(ListNamespacesRequest request) {
    ensureOpen();
    String json = listNamespacesNative(nativeHandle, toJson(request));
    return fromJson(json, ListNamespacesResponse.class);
  }

  @Override
  public DescribeNamespaceResponse describeNamespace(DescribeNamespaceRequest request) {
    ensureOpen();
    String json = describeNamespaceNative(nativeHandle, toJson(request));
    return fromJson(json, DescribeNamespaceResponse.class);
  }

  @Override
  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    ensureOpen();
    String json = createNamespaceNative(nativeHandle, toJson(request));
    return fromJson(json, CreateNamespaceResponse.class);
  }

  @Override
  public DropNamespaceResponse dropNamespace(DropNamespaceRequest request) {
    ensureOpen();
    String json = dropNamespaceNative(nativeHandle, toJson(request));
    return fromJson(json, DropNamespaceResponse.class);
  }

  @Override
  public void namespaceExists(NamespaceExistsRequest request) {
    ensureOpen();
    namespaceExistsNative(nativeHandle, toJson(request));
  }

  @Override
  public ListTablesResponse listTables(ListTablesRequest request) {
    ensureOpen();
    String json = listTablesNative(nativeHandle, toJson(request));
    return fromJson(json, ListTablesResponse.class);
  }

  @Override
  public DescribeTableResponse describeTable(DescribeTableRequest request) {
    ensureOpen();
    String json = describeTableNative(nativeHandle, toJson(request));
    return fromJson(json, DescribeTableResponse.class);
  }

  @Override
  public RegisterTableResponse registerTable(RegisterTableRequest request) {
    ensureOpen();
    String json = registerTableNative(nativeHandle, toJson(request));
    return fromJson(json, RegisterTableResponse.class);
  }

  @Override
  public void tableExists(TableExistsRequest request) {
    ensureOpen();
    tableExistsNative(nativeHandle, toJson(request));
  }

  @Override
  public DropTableResponse dropTable(DropTableRequest request) {
    ensureOpen();
    String json = dropTableNative(nativeHandle, toJson(request));
    return fromJson(json, DropTableResponse.class);
  }

  @Override
  public DeregisterTableResponse deregisterTable(DeregisterTableRequest request) {
    ensureOpen();
    String json = deregisterTableNative(nativeHandle, toJson(request));
    return fromJson(json, DeregisterTableResponse.class);
  }

  @Override
  public Long countTableRows(CountTableRowsRequest request) {
    ensureOpen();
    return countTableRowsNative(nativeHandle, toJson(request));
  }

  @Override
  public CreateTableResponse createTable(CreateTableRequest request, byte[] requestData) {
    ensureOpen();
    Preconditions.checkNotNull(requestData, "requestData is null");
    String json = createTableNative(nativeHandle, toJson(request), requestData);
    return fromJson(json, CreateTableResponse.class);
  }

  @Override
  public CreateEmptyTableResponse createEmptyTable(CreateEmptyTableRequest request) {
    ensureOpen();
    String json = createEmptyTableNative(nativeHandle, toJson(request));
    return fromJson(json, CreateEmptyTableResponse.class);
  }

  @Override
  public DeclareTableResponse declareTable(DeclareTableRequest request) {
    ensureOpen();
    String json = declareTableNative(nativeHandle, toJson(request));
    return fromJson(json, DeclareTableResponse.class);
  }

  @Override
  public InsertIntoTableResponse insertIntoTable(
      InsertIntoTableRequest request, byte[] requestData) {
    ensureOpen();
    Preconditions.checkNotNull(requestData, "requestData is null");
    String json = insertIntoTableNative(nativeHandle, toJson(request), requestData);
    return fromJson(json, InsertIntoTableResponse.class);
  }

  @Override
  public MergeInsertIntoTableResponse mergeInsertIntoTable(
      MergeInsertIntoTableRequest request, byte[] requestData) {
    ensureOpen();
    Preconditions.checkNotNull(requestData, "requestData is null");
    String json = mergeInsertIntoTableNative(nativeHandle, toJson(request), requestData);
    return fromJson(json, MergeInsertIntoTableResponse.class);
  }

  @Override
  public UpdateTableResponse updateTable(UpdateTableRequest request) {
    ensureOpen();
    String json = updateTableNative(nativeHandle, toJson(request));
    return fromJson(json, UpdateTableResponse.class);
  }

  @Override
  public DeleteFromTableResponse deleteFromTable(DeleteFromTableRequest request) {
    ensureOpen();
    String json = deleteFromTableNative(nativeHandle, toJson(request));
    return fromJson(json, DeleteFromTableResponse.class);
  }

  @Override
  public byte[] queryTable(QueryTableRequest request) {
    ensureOpen();
    return queryTableNative(nativeHandle, toJson(request));
  }

  @Override
  public CreateTableIndexResponse createTableIndex(CreateTableIndexRequest request) {
    ensureOpen();
    String json = createTableIndexNative(nativeHandle, toJson(request));
    return fromJson(json, CreateTableIndexResponse.class);
  }

  @Override
  public ListTableIndicesResponse listTableIndices(ListTableIndicesRequest request) {
    ensureOpen();
    String json = listTableIndicesNative(nativeHandle, toJson(request));
    return fromJson(json, ListTableIndicesResponse.class);
  }

  @Override
  public DescribeTableIndexStatsResponse describeTableIndexStats(
      DescribeTableIndexStatsRequest request, String indexName) {
    ensureOpen();
    String json = describeTableIndexStatsNative(nativeHandle, toJson(request));
    return fromJson(json, DescribeTableIndexStatsResponse.class);
  }

  @Override
  public DescribeTransactionResponse describeTransaction(DescribeTransactionRequest request) {
    ensureOpen();
    String json = describeTransactionNative(nativeHandle, toJson(request));
    return fromJson(json, DescribeTransactionResponse.class);
  }

  @Override
  public AlterTransactionResponse alterTransaction(AlterTransactionRequest request) {
    ensureOpen();
    String json = alterTransactionNative(nativeHandle, toJson(request));
    return fromJson(json, AlterTransactionResponse.class);
  }

  /** Shared logical schema enforced across all partition tables. */
  public LanceSchema schema() {
    ensureOpen();
    return schemaNative(nativeHandle);
  }

  /**
   * Partition pruning for the given filter expression.
   *
   * @param filter SQL expression used in a WHERE clause (empty means TRUE)
   */
  public List<PlanScanItem> planScan(String filter) {
    ensureOpen();
    Preconditions.checkNotNull(filter, "filter is null");
    String json = planScanNative(nativeHandle, filter);
    return fromJson(json, new TypeReference<List<PlanScanItem>>() {});
  }

  /** Get all partition specs as JSON objects. */
  public List<Map<String, Object>> partitioning() {
    ensureOpen();
    String json = partitioningNative(nativeHandle);
    return fromJson(json, new TypeReference<List<Map<String, Object>>>() {});
  }

  /**
   * Update the current partition spec.
   *
   * @param partitionFieldsJson JSON array of partition field definitions
   * @return the new partition spec as a JSON object
   */
  public Map<String, Object> updatePartitionSpec(String partitionFieldsJson) {
    ensureOpen();
    Preconditions.checkNotNull(partitionFieldsJson, "partitionFieldsJson is null");
    String json = updatePartitionSpecNative(nativeHandle, partitionFieldsJson);
    return fromJson(json, new TypeReference<Map<String, Object>>() {});
  }

  /**
   * Resolve the target partition table for the input row. Create it (empty table) if not exists.
   *
   * <p>The stream must contain exactly one record batch with exactly one row.
   */
  public PartitionTable resolveOrCreatePartitionTable(ArrowArrayStream recordStream) {
    ensureOpen();
    Preconditions.checkNotNull(recordStream, "recordStream is null");
    String json = resolveOrCreatePartitionTableNative(nativeHandle, recordStream.memoryAddress());
    return fromJson(json, new TypeReference<PartitionTable>() {});
  }

  /** List all partition tables in this partitioned namespace. */
  public List<PartitionTable> tables() {
    ensureOpen();
    String json = tablesNative(nativeHandle);
    return fromJson(json, new TypeReference<List<PartitionTable>>() {});
  }

  /** Commit (currently not implemented on the Rust side). */
  public String commit(Object readVersionJson, Object newVersionJson) {
    ensureOpen();
    return commitNative(nativeHandle, readVersionJson, newVersionJson);
  }

  @Override
  public void close() {
    if (nativeHandle != 0) {
      releaseNative(nativeHandle);
      nativeHandle = 0;
    }
  }

  private void ensureOpen() {
    Preconditions.checkArgument(nativeHandle != 0, "PartitionedNamespace is closed");
  }

  private static <T> T fromJson(String json, TypeReference<T> typeRef) {
    try {
      return OBJECT_MAPPER.readValue(json, typeRef);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize JSON", e);
    }
  }

  private static String toJson(Object obj) {
    try {
      return OBJECT_MAPPER.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize request to JSON", e);
    }
  }

  private static <T> T fromJson(String json, Class<T> clazz) {
    try {
      return OBJECT_MAPPER.readValue(json, clazz);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize JSON", e);
    }
  }

  /** PlanScan result item. */
  public static final class PlanScanItem {
    @JsonProperty("table")
    private PartitionTable table;

    @JsonProperty("refine_expr")
    private String refineExpr;

    public PlanScanItem() {}

    public PartitionTable table() {
      return table;
    }

    public String refineExpr() {
      return refineExpr;
    }
  }

  /** Partition table identifier. */
  public static final class PartitionTable {
    @JsonProperty("id")
    private List<String> id;

    @JsonProperty("read_version")
    private Long readVersion;

    public PartitionTable() {}

    public List<String> id() {
      return id;
    }

    public Long readVersion() {
      return readVersion;
    }
  }

  // Native methods
  private native long createNative(Map<String, String> configProperties);

  private native long createNativeWithProvider(
      Map<String, String> configProperties, DynamicContextProvider contextProvider);

  private native void releaseNative(long handle);

  private native String namespaceIdNative(long handle);

  private native String listNamespacesNative(long handle, String requestJson);

  private native String describeNamespaceNative(long handle, String requestJson);

  private native String createNamespaceNative(long handle, String requestJson);

  private native String dropNamespaceNative(long handle, String requestJson);

  private native void namespaceExistsNative(long handle, String requestJson);

  private native String listTablesNative(long handle, String requestJson);

  private native String describeTableNative(long handle, String requestJson);

  private native String registerTableNative(long handle, String requestJson);

  private native void tableExistsNative(long handle, String requestJson);

  private native String dropTableNative(long handle, String requestJson);

  private native String deregisterTableNative(long handle, String requestJson);

  private native long countTableRowsNative(long handle, String requestJson);

  private native String createTableNative(long handle, String requestJson, byte[] requestData);

  private native String createEmptyTableNative(long handle, String requestJson);

  private native String declareTableNative(long handle, String requestJson);

  private native String insertIntoTableNative(long handle, String requestJson, byte[] requestData);

  private native String mergeInsertIntoTableNative(
      long handle, String requestJson, byte[] requestData);

  private native String updateTableNative(long handle, String requestJson);

  private native String deleteFromTableNative(long handle, String requestJson);

  private native byte[] queryTableNative(long handle, String requestJson);

  private native String createTableIndexNative(long handle, String requestJson);

  private native String listTableIndicesNative(long handle, String requestJson);

  private native String describeTableIndexStatsNative(long handle, String requestJson);

  private native String describeTransactionNative(long handle, String requestJson);

  private native String alterTransactionNative(long handle, String requestJson);

  private native LanceSchema schemaNative(long handle);

  private native String planScanNative(long handle, String filter);

  private native String partitioningNative(long handle);

  private native String updatePartitionSpecNative(long handle, String partitionFieldsJson);

  private native String resolveOrCreatePartitionTableNative(long handle, long arrowArrayStreamAddr);

  private native String tablesNative(long handle);

  private native String commitNative(long handle, Object readVersionJson, Object newVersionJson);
}
