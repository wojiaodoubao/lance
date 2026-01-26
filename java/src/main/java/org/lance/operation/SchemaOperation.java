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

import java.util.Objects;

/** Schema related base operation. */
public abstract class SchemaOperation implements Operation {
  private final LanceSchema schema;

  protected SchemaOperation(LanceSchema schema) {
    this.schema = schema;
  }

  public LanceSchema schema() {
    return schema;
  }

  public void release() {}

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SchemaOperation that = (SchemaOperation) o;
    return Objects.equals(schema, that.schema);
  }
}
