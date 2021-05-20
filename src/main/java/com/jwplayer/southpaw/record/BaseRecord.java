/*
 * Copyright 2018 Longtail Ad Solutions (DBA JW Player)
 *
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

package com.jwplayer.southpaw.record;

import com.jwplayer.southpaw.util.ByteArray;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * A record abstraction for standardizing and exposing some basic functionality.
 */
public abstract class BaseRecord {
  /**
   * Accessor for a particular field in this record
   *
   * @param fieldName - The name of the field to get
   * @return The value of the field, or null if it doesn't exist
   */
  public abstract Object get(String fieldName);

  /**
   * Gets the schema of the record. Note that the schema can evolve or change between different records of the
   * same type.
   *
   * @return The schema as a map of field name to Java class
   */
  public abstract Map<String, Class> getSchema();

  /**
   * Determines if this record is empty. In other words, the internal representation is null or contains no entries.
   *
   * @return True if the record is empty, else false
   */
  public abstract boolean isEmpty();

  /**
   * Get the record as a map of field names to Java objects.
   *
   * @return The contents of the record as a map
   */
  public abstract Map<String, ?> toMap();

  /**
   * Converts this record into a byte array. Typically used to get a nice PK from a key record.
   *
   * @return - This record as a ByteArray
   */
  public ByteArray toByteArray() {
    List<Map.Entry<String, Class>> fields = new ArrayList<>(getSchema().entrySet());
    switch (fields.size()) {
      case 0:
        return new ByteArray("");
      case 1:
        return ByteArray.toByteArray(get(fields.get(0).getKey()));
      default:
        fields.sort(Map.Entry.comparingByKey());
        List<ByteArray> pks = new ArrayList<>(fields.size());
        for (Map.Entry<String, Class> field : fields) {
          pks.add(ByteArray.toByteArray(get(field.getKey())));
        }

        return new ByteArray(ByteArray.toBytes(pks));
    }
  }

  /**
   * Gives a friendly string representation of the object. Particularly useful for debugging in Intellij.
   *
   * @return The string representation of this object.
   */
  public String toString() {
    return toMap().toString();
  }
}
