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
package org.lance.schema;

import org.lance.test.JniTestHelper;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class LanceFieldConversionTest {
  private BufferAllocator allocator;

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    if (allocator != null) {
      allocator.close();
    }
  }

  @Test
  public void testLanceFieldRoundTripPreservesAllProperties() throws Exception {
    byte[] dictionaryBytes = buildDictionaryValuesBytes();
    LanceField original = buildNestedLanceField(dictionaryBytes);
    LanceField roundTripped = JniTestHelper.roundTripLanceField(original);
    assertNotNull(roundTripped);
    assertEquals(original, roundTripped);
  }

  @Test
  public void testLanceFieldRoundTripListStruct() throws Exception {
    Map<String, String> listMetadata = new HashMap<>();
    listMetadata.put("list-key", "list-value");

    LanceField structElement =
        new LanceField.Builder()
            .id(2)
            .parentId(1)
            .name("elem_struct")
            .nullable(true)
            .type(ArrowType.Struct.INSTANCE)
            .children(
                List.of(
                    new LanceField.Builder()
                        .id(3)
                        .parentId(2)
                        .name("f1")
                        .nullable(true)
                        .type(ArrowType.Utf8.INSTANCE)
                        .build(),
                    new LanceField.Builder()
                        .id(4)
                        .parentId(2)
                        .name("f2")
                        .nullable(false)
                        .type(new ArrowType.Int(32, true))
                        .build()))
            .build();

    LanceField listField =
        new LanceField.Builder()
            .id(1)
            .parentId(0)
            .name("list_root")
            .nullable(false)
            .type(new ArrowType.List())
            .metadata(listMetadata)
            .children(Collections.singletonList(structElement))
            .build();

    LanceField roundTripped = JniTestHelper.roundTripLanceField(listField);
    assertNotNull(roundTripped);
    assertEquals(listField, roundTripped);
  }

  private LanceField buildNestedLanceField(byte[] dictionaryBytes) {
    Map<String, String> rootMetadata = new HashMap<>();
    rootMetadata.put("root-key", "root-value");
    Map<String, String> childMetadata = new HashMap<>();
    childMetadata.put("child-key", "child-value");
    LanceField leaf =
        new LanceField.Builder()
            .id(3)
            .parentId(2)
            .name("leaf")
            .nullable(true)
            .type(ArrowType.Utf8.INSTANCE)
            .metadata(childMetadata)
            .encoding(LanceField.Encoding.DICTIONARY)
            .dictionary(new LanceField.LanceDictionary(5L, 3L, dictionaryBytes))
            .unenforcedPrimaryKey(true)
            .unenforcedPrimaryKeyPosition(1)
            .build();
    LanceField structChild =
        new LanceField.Builder()
            .id(2)
            .parentId(1)
            .name("nested")
            .nullable(false)
            .type(ArrowType.Struct.INSTANCE)
            .children(Collections.singletonList(leaf))
            .build();
    return new LanceField.Builder()
        .id(1)
        .parentId(0)
        .name("root")
        .nullable(false)
        .type(ArrowType.Struct.INSTANCE)
        .metadata(rootMetadata)
        .children(Collections.singletonList(structChild))
        .build();
  }

  private byte[] buildDictionaryValuesBytes() throws IOException {
    List<Field> fields =
        Collections.singletonList(
            new Field("value", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
    Schema schema = new Schema(fields);
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      VarCharVector vector = (VarCharVector) root.getVector("value");
      root.allocateNew();
      vector.setSafe(0, "alpha".getBytes(StandardCharsets.UTF_8));
      vector.setSafe(1, "beta".getBytes(StandardCharsets.UTF_8));
      vector.setSafe(2, "gamma".getBytes(StandardCharsets.UTF_8));
      root.setRowCount(3);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }
      return out.toByteArray();
    }
  }
}
