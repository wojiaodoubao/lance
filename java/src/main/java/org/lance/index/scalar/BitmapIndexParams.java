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
package org.lance.index.scalar;

import org.lance.util.JsonUtils;

import java.util.HashMap;
import java.util.Map;

/** Builder-style configuration for Bitmap scalar index parameters. */
public final class BitmapIndexParams {
  private static final String INDEX_TYPE = "bitmap";

  private BitmapIndexParams() {}

  /** Create a new builder for Bitmap index parameters. */
  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private Boolean enableLargeBitmaps;

    /**
     * Enable bitmap storage using Arrow LargeBinary to support huge bitmaps.
     *
     * <p>When enabled, Lance will write bitmap pages using Arrow LargeBinary so a single bitmap
     * can exceed the 2GiB Binary limit.
     *
     * <p>Compatibility note: bitmap indices written with LargeBinary cannot be read by older
     * Lance versions that only support Binary.
     */
    public Builder enableLargeBitmaps(boolean enableLargeBitmaps) {
      this.enableLargeBitmaps = enableLargeBitmaps;
      return this;
    }

    /** Build a {@link ScalarIndexParams} instance for a Bitmap index. */
    public ScalarIndexParams build() {
      Map<String, Object> params = new HashMap<>();
      if (Boolean.TRUE.equals(enableLargeBitmaps)) {
        params.put("enable_large_bitmaps", true);
      }

      if (params.isEmpty()) {
        return ScalarIndexParams.create(INDEX_TYPE);
      }

      String json = JsonUtils.toJson(params);
      return ScalarIndexParams.create(INDEX_TYPE, json);
    }
  }
}
