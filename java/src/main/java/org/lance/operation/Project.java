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

import org.lance.schema.LanceSchema;

import com.google.common.base.MoreObjects;

/**
 * Project to a new schema. This Operation only changes the schema, not the data. Note: 1. For
 * removing columns. The data will be removed after compaction. 2. Project will modify column
 * positions, not ids(a.k.a. field id)
 */
public class Project extends SchemaOperation {
  private Project(LanceSchema schema) {
    super(schema);
  }

  @Override
  public String name() {
    return "Project";
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("schema", schema()).toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private LanceSchema schema;

    public Builder() {}

    public Builder schema(LanceSchema schema) {
      this.schema = schema;
      return this;
    }

    public Project build() {
      return new Project(schema);
    }
  }
}
