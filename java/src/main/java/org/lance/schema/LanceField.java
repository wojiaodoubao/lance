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

import com.google.common.base.MoreObjects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

public class LanceField {
  /**
   * Logical encoding of the field's values. Mirrors lance_core::datatypes::field::Encoding on the
   * Rust side.
   */
  public enum Encoding {
    PLAIN,
    VAR_BINARY,
    DICTIONARY,
    RLE
  }

  /**
   * Dictionary descriptor for this field. Mirrors lance_core::datatypes::Dictionary, but uses an
   * IPC-encoded Arrow array for the values.
   */
  public static class LanceDictionary {
    private final long offset;
    private final long length;
    private final byte[] values;

    public LanceDictionary(long offset, long length, byte[] values) {
      this.offset = offset;
      this.length = length;
      this.values = values;
    }

    public long getOffset() {
      return offset;
    }

    public long getLength() {
      return length;
    }

    public byte[] getValues() {
      return values;
    }

    @Override
    public int hashCode() {
      return Objects.hash(offset, length, values);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LanceDictionary that = (LanceDictionary) o;
      if (offset != that.offset
          || length != that.length
          || (values == null && that.values != null)
          || (values != null && that.values == null)) {
        return false;
      }
      if (values != null) {
        return Objects.equals(decodeDictionaryValues(values), decodeDictionaryValues(that.values));
      }
      return true;
    }
  }

  private final int id;
  private final int parentId;
  private final String name;
  private final boolean nullable;
  private final ArrowType type;
  private final Map<String, String> metadata;
  private final List<LanceField> children;
  private final boolean isUnenforcedPrimaryKey;
  private final int unenforcedPrimaryKeyPosition;
  private final Encoding encoding;
  private final LanceDictionary dictionary;

  LanceField(
      int id,
      int parentId,
      String name,
      boolean nullable,
      ArrowType type,
      Map<String, String> metadata,
      List<LanceField> children,
      boolean isUnenforcedPrimaryKey,
      int unenforcedPrimaryKeyPosition,
      Encoding encoding,
      LanceDictionary dictionary) {
    this.id = id;
    this.parentId = parentId;
    this.name = name;
    this.nullable = nullable;
    this.type = type;
    this.metadata = metadata != null ? metadata : Collections.emptyMap();
    this.children = children != null ? children : Collections.emptyList();
    this.isUnenforcedPrimaryKey = isUnenforcedPrimaryKey;
    this.unenforcedPrimaryKeyPosition = unenforcedPrimaryKeyPosition;
    this.encoding = encoding;
    this.dictionary = dictionary;
  }

  public int getId() {
    return id;
  }

  public int getParentId() {
    return parentId;
  }

  public String getName() {
    return name;
  }

  public boolean isNullable() {
    return nullable;
  }

  public ArrowType getType() {
    return type;
  }

  public Optional<Encoding> getEncoding() {
    return Optional.ofNullable(encoding);
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public List<LanceField> getChildren() {
    return children;
  }

  public boolean hasDictionary() {
    return dictionary != null;
  }

  public Optional<LanceDictionary> getDictionary() {
    return Optional.ofNullable(dictionary);
  }

  public boolean isUnenforcedPrimaryKey() {
    return isUnenforcedPrimaryKey;
  }

  /**
   * Get the position of this field within a composite primary key.
   *
   * @return the 1-based position if explicitly set, or empty if using schema field id ordering
   */
  public OptionalInt getUnenforcedPrimaryKeyPosition() {
    if (unenforcedPrimaryKeyPosition > 0) {
      return OptionalInt.of(unenforcedPrimaryKeyPosition);
    }
    return OptionalInt.empty();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(LanceField other) {
    return new Builder(other);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .add("parentId", parentId)
        .add("name", name)
        .add("nullable", nullable)
        .add("type", type)
        .add("children", children)
        .add("isUnenforcedPrimaryKey", isUnenforcedPrimaryKey)
        .add("unenforcedPrimaryKeyPosition", unenforcedPrimaryKeyPosition)
        .add("encoding", encoding)
        .add("dictionary", dictionary)
        .add("metadata", metadata)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LanceField that = (LanceField) o;
    return Objects.equals(id, that.id)
        && Objects.equals(parentId, that.parentId)
        && Objects.equals(name, that.name)
        && nullable == that.nullable
        && Objects.equals(type, that.type)
        && Objects.equals(metadata, that.metadata)
        && Objects.equals(children, that.children)
        && encoding == that.encoding
        && isUnenforcedPrimaryKey == that.isUnenforcedPrimaryKey
        && unenforcedPrimaryKeyPosition == that.unenforcedPrimaryKeyPosition
        && Objects.equals(children, that.children)
        && Objects.equals(dictionary, that.dictionary);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name,
        nullable,
        type,
        metadata,
        children,
        isUnenforcedPrimaryKey,
        unenforcedPrimaryKeyPosition,
        encoding,
        dictionary);
  }

  /** Builder for LanceField. */
  public static class Builder {
    private int id;
    private int parentId;
    private String name;
    private boolean nullable;
    private ArrowType type;
    private Map<String, String> metadata;
    private List<LanceField> children;
    private boolean isUnenforcedPrimaryKey;
    private int unenforcedPrimaryKeyPosition;
    private Encoding encoding;
    private LanceDictionary dictionary;

    public Builder() {}

    public Builder(LanceField other) {
      this.id = other.id;
      this.parentId = other.parentId;
      this.name = other.name;
      this.nullable = other.nullable;
      this.type = other.type;
      this.metadata = other.metadata;
      this.children = other.children;
      this.isUnenforcedPrimaryKey = other.isUnenforcedPrimaryKey;
      this.unenforcedPrimaryKeyPosition = other.unenforcedPrimaryKeyPosition;
      this.encoding = other.encoding;
      this.dictionary = other.dictionary;
    }

    public Builder id(int id) {
      this.id = id;
      return this;
    }

    public Builder parentId(int parentId) {
      this.parentId = parentId;
      return this;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder nullable(boolean nullable) {
      this.nullable = nullable;
      return this;
    }

    public Builder type(ArrowType type) {
      this.type = type;
      return this;
    }

    public Builder encoding(Encoding encoding) {
      this.encoding = encoding;
      return this;
    }

    public Builder dictionary(LanceDictionary dictionary) {
      this.dictionary = dictionary;
      return this;
    }

    public Builder metadata(Map<String, String> metadata) {
      this.metadata = metadata;
      return this;
    }

    public Builder children(List<LanceField> children) {
      this.children = children;
      return this;
    }

    public Builder unenforcedPrimaryKey(boolean isUnenforcedPrimaryKey) {
      this.isUnenforcedPrimaryKey = isUnenforcedPrimaryKey;
      return this;
    }

    public Builder unenforcedPrimaryKeyPosition(int unenforcedPrimaryKeyPosition) {
      this.unenforcedPrimaryKeyPosition = unenforcedPrimaryKeyPosition;
      return this;
    }

    public LanceField build() {
      Map<String, String> resolvedMetadata =
          metadata == null ? Collections.emptyMap() : Collections.unmodifiableMap(metadata);
      List<LanceField> resolvedChildren =
          children == null ? Collections.emptyList() : Collections.unmodifiableList(children);

      return new LanceField(
          id,
          parentId,
          name,
          nullable,
          type,
          resolvedMetadata,
          resolvedChildren,
          isUnenforcedPrimaryKey,
          unenforcedPrimaryKeyPosition,
          encoding,
          dictionary);
    }
  }

  private static List<String> decodeDictionaryValues(byte[] bytes) {
    try {
      try (BufferAllocator allocator = new RootAllocator()) {
        try (ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {
          List<String> values = new ArrayList<>();
          while (reader.loadNextBatch()) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            VarCharVector vector = (VarCharVector) root.getVector(0);
            int rowCount = root.getRowCount();
            for (int i = 0; i < rowCount; i++) {
              if (vector.isNull(i)) {
                values.add(null);
              } else {
                byte[] data = vector.get(i);
                values.add(new String(data, StandardCharsets.UTF_8));
              }
            }
          }
          return values;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Error reading dictionary values", e);
    }
  }
}
