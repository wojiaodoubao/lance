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
package org.lance.operation;

import org.lance.Dataset;
import org.lance.FragmentMetadata;
import org.lance.TestUtils;
import org.lance.Transaction;
import org.lance.fragment.DataFile;
import org.lance.ipc.LanceScanner;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MergeTest extends OperationTestBase {

  @Test
  void testMergeNewColumn(@TempDir Path tempDir) throws Exception {
    String datasetPath = tempDir.resolve("testMergeNewColumn").toString();
    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    try {
      TestUtils.SimpleTestDataset testDataset =
          new TestUtils.SimpleTestDataset(allocator, datasetPath);

      int rowCount = 15;
      try (Dataset initialDataset = createAndAppendRows(testDataset, rowCount)) {
        // Add a new column with different data type
        Field ageField = Field.nullable("age", new ArrowType.Int(32, true));

        // New lance schema
        LanceSchema baseSchema = initialDataset.getLanceSchema();
        LanceField idLanceField = findField(baseSchema, "id");
        LanceField nameLanceField = findField(baseSchema, "name");

        int ageFieldId = initialDataset.maxFieldId() + 1;
        LanceField ageLanceField =
            LanceField.builder()
                .name("age")
                .id(ageFieldId)
                .parentId(-1)
                .nullable(true)
                .type(new ArrowType.Int(32, true))
                .encoding(LanceField.Encoding.PLAIN)
                .build();

        LanceSchema evolvedSchema =
            LanceSchema.builder()
                .withFields(Arrays.asList(idLanceField, nameLanceField, ageLanceField))
                .build();

        try (VectorSchemaRoot ageRoot =
            VectorSchemaRoot.create(
                new Schema(Collections.singletonList(ageField), null), allocator)) {
          // Write new fragment
          ageRoot.allocateNew();
          IntVector ageVector = (IntVector) ageRoot.getVector("age");

          for (int i = 0; i < rowCount; i++) {
            ageVector.setSafe(i, 20 + i);
          }
          ageRoot.setRowCount(rowCount);

          DataFile ageDataFile =
              writeLanceDataFile(
                  allocator, datasetPath, ageRoot, new int[] {ageFieldId}, new int[] {0});

          FragmentMetadata fragmentMeta = initialDataset.getFragment(0).metadata();
          List<DataFile> dataFiles = fragmentMeta.getFiles();
          dataFiles.add(ageDataFile);
          FragmentMetadata evolvedFragment =
              new FragmentMetadata(
                  fragmentMeta.getId(),
                  dataFiles,
                  fragmentMeta.getPhysicalRows(),
                  fragmentMeta.getDeletionFile(),
                  fragmentMeta.getRowIdMeta());

          Transaction mergeTransaction =
              initialDataset
                  .newTransactionBuilder()
                  .operation(
                      Merge.builder()
                          .fragments(Collections.singletonList(evolvedFragment))
                          .schema(evolvedSchema)
                          .build())
                  .build();

          try (Dataset evolvedDataset = mergeTransaction.commit()) {
            Assertions.assertEquals(3, evolvedDataset.version());
            Assertions.assertEquals(rowCount, evolvedDataset.countRows());
            Assertions.assertEquals(evolvedSchema, evolvedDataset.getLanceSchema());
            Assertions.assertEquals(3, evolvedDataset.getSchema().getFields().size());
            // Verify merged data
            try (LanceScanner scanner = evolvedDataset.newScan()) {
              try (ArrowReader resultReader = scanner.scanBatches()) {
                Assertions.assertTrue(resultReader.loadNextBatch());
                VectorSchemaRoot batch = resultReader.getVectorSchemaRoot();
                Assertions.assertEquals(rowCount, batch.getRowCount());
                Assertions.assertEquals(3, batch.getSchema().getFields().size());
                // Verify age column
                IntVector ageResultVector = (IntVector) batch.getVector("age");
                for (int i = 0; i < rowCount; i++) {
                  Assertions.assertEquals(20 + i, ageResultVector.get(i));
                }
                IntVector idResultVector = (IntVector) batch.getVector("id");
                for (int i = 0; i < rowCount; i++) {
                  Assertions.assertEquals(i, idResultVector.get(i));
                }
              }
            }
          }
        }
      }
    } finally {
      allocator.close();
    }
  }

  private static LanceField findField(LanceSchema schema, String name) {
    for (LanceField field : schema.fields()) {
      if (field.getName().equals(name)) {
        return field;
      }
    }
    throw new IllegalArgumentException("No such field: " + name);
  }

  // Replace column id data type, add new column age and remove column name.
  @Test
  void testReplaceAsDiffColumns(@TempDir Path tempDir) throws Exception {
    String datasetPath = tempDir.resolve("testReplaceAsDiffColumns").toString();
    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    try {
      TestUtils.SimpleTestDataset testDataset =
          new TestUtils.SimpleTestDataset(allocator, datasetPath);

      int rowCount = 15;
      try (Dataset initialDataset = createAndAppendRows(testDataset, rowCount)) {
        // Add a new column age
        Field ageFieldArrow = Field.nullable("age", new ArrowType.Int(32, true));
        // Update id with different data type
        Field idFieldArrow = Field.notNullable("id", new ArrowType.Int(64, true));

        // Compute new lance schema:
        //  id   - type updated from int32 to int64
        //  age  - new added column
        //  name - column removed
        LanceSchema baseSchema = initialDataset.getLanceSchema();
        LanceField idLanceField =
            LanceField.builder(findField(baseSchema, "id"))
                .type(new ArrowType.Int(64, true))
                .encoding(LanceField.Encoding.PLAIN)
                .build();

        int ageFieldId = initialDataset.maxFieldId() + 1;
        LanceField ageLanceField =
            LanceField.builder()
                .name("age")
                .id(ageFieldId)
                .parentId(-1)
                .nullable(true)
                .type(new ArrowType.Int(32, true))
                .encoding(LanceField.Encoding.PLAIN)
                .build();
        LanceSchema evolvedLanceSchema =
            LanceSchema.builder().withFields(Arrays.asList(idLanceField, ageLanceField)).build();

        List<Field> fields = Arrays.asList(idFieldArrow, ageFieldArrow);
        try (VectorSchemaRoot ageRoot =
            VectorSchemaRoot.create(new Schema(fields, null), allocator)) {
          // Write new fragment with new id and age
          ageRoot.allocateNew();
          IntVector ageVector = (IntVector) ageRoot.getVector("age");
          BigIntVector idVector = (BigIntVector) ageRoot.getVector("id");

          for (int i = 0; i < rowCount; i++) {
            ageVector.setSafe(i, 20 + i);
            idVector.setSafe(i, i);
          }
          ageRoot.setRowCount(rowCount);

          DataFile ageDataFile =
              writeLanceDataFile(
                  allocator,
                  datasetPath,
                  ageRoot,
                  new int[] {idLanceField.getId(), ageFieldId},
                  new int[] {0, 1});

          FragmentMetadata fragmentMeta = initialDataset.getFragment(0).metadata();
          FragmentMetadata evolvedFragment =
              new FragmentMetadata(
                  fragmentMeta.getId(),
                  Collections.singletonList(ageDataFile),
                  fragmentMeta.getPhysicalRows(),
                  fragmentMeta.getDeletionFile(),
                  fragmentMeta.getRowIdMeta());

          Transaction mergeTransaction =
              initialDataset
                  .newTransactionBuilder()
                  .operation(
                      Merge.builder()
                          .fragments(Collections.singletonList(evolvedFragment))
                          .schema(evolvedLanceSchema)
                          .build())
                  .build();

          try (Dataset evolvedDataset = mergeTransaction.commit()) {
            Assertions.assertEquals(3, evolvedDataset.version());
            Assertions.assertEquals(rowCount, evolvedDataset.countRows());
            Assertions.assertEquals(evolvedLanceSchema, evolvedDataset.getLanceSchema());
            Assertions.assertEquals(2, evolvedDataset.getSchema().getFields().size());
            // Verify merged data
            try (LanceScanner scanner = evolvedDataset.newScan()) {
              try (ArrowReader resultReader = scanner.scanBatches()) {
                Assertions.assertTrue(resultReader.loadNextBatch());
                VectorSchemaRoot batch = resultReader.getVectorSchemaRoot();
                Assertions.assertEquals(rowCount, batch.getRowCount());
                Assertions.assertEquals(2, batch.getSchema().getFields().size());
                // Verify age column
                IntVector ageResultVector = (IntVector) batch.getVector("age");
                for (int i = 0; i < rowCount; i++) {
                  Assertions.assertEquals(20 + i, ageResultVector.get(i));
                }
                // Verify id column
                BigIntVector idResultVector = (BigIntVector) batch.getVector("id");
                for (int i = 0; i < rowCount; i++) {
                  Assertions.assertEquals(i, idResultVector.get(i));
                }
              }
            }
          }
        }
      }
    } finally {
      allocator.close();
    }
  }

  @Test
  void testMergeExistingColumn(@TempDir Path tempDir) throws Exception {
    String datasetPath = tempDir.resolve("testMergeExistingColumn").toString();
    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    try {
      // Test merging with existing column updates
      TestUtils.SimpleTestDataset testDataset =
          new TestUtils.SimpleTestDataset(allocator, datasetPath);

      int rowCount = 10;
      try (Dataset initialDataset = createAndAppendRows(testDataset, rowCount)) {
        LanceSchema baseSchema = initialDataset.getLanceSchema();
        LanceField idLanceField = findField(baseSchema, "id");
        int newNameId = initialDataset.maxFieldId() + 1;
        LanceField nameLanceField =
            LanceField.builder(findField(baseSchema, "name")).id(newNameId).build();
        LanceSchema evolvedSchema =
            LanceSchema.builder().withFields(Arrays.asList(idLanceField, nameLanceField)).build();

        // Create updated name column data
        Field nameFieldArrow = Field.nullable("name", new ArrowType.Utf8());
        Schema nameSchema = new Schema(Collections.singletonList(nameFieldArrow), null);

        try (VectorSchemaRoot updatedNameRoot = VectorSchemaRoot.create(nameSchema, allocator)) {
          updatedNameRoot.allocateNew();
          VarCharVector nameVector = (VarCharVector) updatedNameRoot.getVector("name");

          for (int i = 0; i < rowCount; i++) {
            String updatedName = "UpdatedName_" + i;
            nameVector.setSafe(i, updatedName.getBytes(StandardCharsets.UTF_8));
          }
          updatedNameRoot.setRowCount(rowCount);

          // Create DataFile for updated column
          DataFile updatedNameDataFile =
              writeLanceDataFile(
                  allocator,
                  datasetPath,
                  updatedNameRoot,
                  new int[] {nameLanceField.getId()},
                  new int[] {0});

          // Perform merge with updated column
          FragmentMetadata fragmentMeta = initialDataset.getFragment(0).metadata();
          List<DataFile> dataFiles = fragmentMeta.getFiles();
          dataFiles.add(updatedNameDataFile);
          FragmentMetadata evolvedFragment =
              new FragmentMetadata(
                  fragmentMeta.getId(),
                  dataFiles,
                  fragmentMeta.getPhysicalRows(),
                  fragmentMeta.getDeletionFile(),
                  fragmentMeta.getRowIdMeta());

          Transaction mergeTransaction =
              initialDataset
                  .newTransactionBuilder()
                  .operation(
                      Merge.builder()
                          .fragments(Collections.singletonList(evolvedFragment))
                          .schema(evolvedSchema)
                          .build())
                  .build();

          try (Dataset mergedDataset = mergeTransaction.commit()) {
            Assertions.assertEquals(3, mergedDataset.version());
            Assertions.assertEquals(rowCount, mergedDataset.countRows());
            Assertions.assertEquals(evolvedSchema, mergedDataset.getLanceSchema());

            // Verify updated data
            try (LanceScanner scanner = mergedDataset.newScan()) {
              try (ArrowReader resultReader = scanner.scanBatches()) {
                Assertions.assertTrue(resultReader.loadNextBatch());
                VectorSchemaRoot batch = resultReader.getVectorSchemaRoot();

                VarCharVector nameResultVector = (VarCharVector) batch.getVector("name");
                for (int i = 0; i < rowCount; i++) {
                  String expectedName = "UpdatedName_" + i;
                  String actualName = new String(nameResultVector.get(i), StandardCharsets.UTF_8);
                  Assertions.assertEquals(expectedName, actualName);
                }
              }
            }
          }
        }
      }
    } finally {
      allocator.close();
    }
  }
}
