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
import org.lance.file.LanceFileWriter;
import org.lance.fragment.DataFile;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.util.Collections;
import java.util.UUID;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OperationTestBase {

  public static final int TEST_FILE_FORMAT_MAJOR_VERSION = 2;
  public static final int TEST_FILE_FORMAT_MINOR_VERSION = 0;
  protected Dataset dataset;

  @BeforeAll
  void setup() {}

  @AfterAll
  void tearDown() {
    // Cleanup resources used by the tests
    if (dataset != null) {
      dataset.close();
    }
  }

  /** Helper method to append simple data to a dataset. */
  protected Dataset createAndAppendRows(TestUtils.SimpleTestDataset suite, int rowCount) {
    dataset = suite.createEmptyDataset();
    FragmentMetadata fragmentMeta = suite.createNewFragment(rowCount);

    Transaction appendTxn =
        dataset
            .newTransactionBuilder()
            .operation(Append.builder().fragments(Collections.singletonList(fragmentMeta)).build())
            .build();
    return appendTxn.commit();
  }

  /**
   * Helper method to create a DataFile from a VectorSchemaRoot.
   *
   * <p>This implementation uses LanceFileWriter to ensure compatibility with the Lance format. The
   * {@code fieldIds} parameter contains Lance field ids from the dataset schema, and the {@code
   * columnIndices} parameter contains the indices of the corresponding columns in the file.
   */
  protected DataFile writeLanceDataFile(
      BufferAllocator allocator,
      String basePath,
      VectorSchemaRoot root,
      int[] fieldIds,
      int[] columnIndices) {
    // Create a unique file path for the data file
    String fileName = UUID.randomUUID() + ".lance";
    String filePath = basePath + "/data/" + fileName;

    // Create parent directories if they don't exist
    File file = new File(filePath);

    // Use LanceFileWriter to write the data
    try (LanceFileWriter writer = LanceFileWriter.open(filePath, allocator, null)) {
      writer.write(root);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // Create a DataFile object with field ids matching the dataset schema.
    // The fields array contains Lance field ids in the schema.
    // The columnIndices array contains the indices of the columns in the file.
    // Use a stable file format version.
    return new DataFile(
        fileName,
        fieldIds, // Lance field ids in the schema
        columnIndices, // Column indices in the written file
        TEST_FILE_FORMAT_MAJOR_VERSION, // File major version
        TEST_FILE_FORMAT_MINOR_VERSION, // File minor version
        file.length(), // File size in bytes (now contains actual data)
        null);
  }
}
