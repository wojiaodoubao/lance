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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class LanceSchema {
  private final List<LanceField> fields;
  private final Map<String, String> metadata;

  LanceSchema(List<LanceField> fields, Map<String, String> metadata) {
    this.fields = fields;
    this.metadata = metadata != null ? metadata : Collections.emptyMap();
  }

  public List<LanceField> fields() {
    return fields;
  }

  public Map<String, String> metadata() {
    return Collections.unmodifiableMap(metadata);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LanceSchema that = (LanceSchema) o;
    return Objects.equals(fields, that.fields) && Objects.equals(metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fields, metadata);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("fields", fields)
        .add("metadata", metadata)
        .toString();
  }

  /** Builder for LanceSchema. */
  public static class Builder {
    private List<LanceField> fields;
    private Map<String, String> metadata;

    public Builder() {}

    public Builder withFields(List<LanceField> fields) {
      this.fields = fields;
      return this;
    }

    public Builder withMetadata(Map<String, String> metadata) {
      this.metadata = metadata;
      return this;
    }

    public LanceSchema build() {
      List<LanceField> resolvedFields =
          fields == null ? Collections.emptyList() : Collections.unmodifiableList(fields);
      Map<String, String> resolvedMetadata =
          metadata == null ? Collections.emptyMap() : Collections.unmodifiableMap(metadata);
      return new LanceSchema(resolvedFields, resolvedMetadata);
    }
  }
}
