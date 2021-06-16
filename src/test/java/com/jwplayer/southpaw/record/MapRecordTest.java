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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;


import java.util.Map;
import org.junit.Before;
import org.junit.Test;


public class MapRecordTest {
  private BaseRecord record;

  @Before
  public void setup() {
    record = new MapRecord(null);
  }

  @Test
  public void testGetWithNullInternalRecord() {
    assertNull(record.get("SomeField"));
  }

  @Test
  public void testGetSchemaWithNullInternalRecord() {
    Map<String, Class> schema = record.getSchema();

    assertNotNull(schema);
    assertEquals(0, schema.size());
  }

  @Test
  public void testToMapWithNullInternalRecord() {
    Map<String, ?> map = record.toMap();

    assertNotNull(map);
    assertEquals(0, map.size());
  }
}
