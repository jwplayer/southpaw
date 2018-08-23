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
package com.jwplayer.southpaw.serde;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.record.MapRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;


public class JsonSerdeTest {
    private JsonSerde serde;

    @Before
    public void setup() {
        serde = new JsonSerde();
        serde.configure(new HashMap<>(0), false);
    }

    @After
    public void cleanup() {
        serde.close();
    }

    @Test
    public void testDeserializerEmptyJson() {
        byte[] bytes = "{ }".getBytes();
        BaseRecord record = serde.deserializer().deserialize(null, bytes);
        assertTrue(record.isEmpty());
    }

    @Test
    public void testDeserializerJson() {
        byte[] bytes = "{ \"A\": 1, \"B\": \"Cat\", \"C\": null }".getBytes();
        BaseRecord record = serde.deserializer().deserialize(null, bytes);
        assertFalse(record.isEmpty());
        assertEquals(1, record.get("A"));
        assertEquals("Cat", record.get("B"));
        assertNull(record.get("C"));
        assertNull(record.get("D"));
    }

    @Test(expected = ClassCastException.class)
    public void testDeserializerNakedValue() {
        byte[] bytes = "1".getBytes();
        serde.deserializer().deserialize(null, bytes);
    }

    @Test
    public void testDeserializerNull() {
        BaseRecord record = serde.deserializer().deserialize(null, null);
        assertTrue(record.isEmpty());
    }

    @Test
    public void testDeserializerNullJson() {
        byte[] bytes = "null".getBytes();
        BaseRecord record = serde.deserializer().deserialize(null, bytes);
        assertTrue(record.isEmpty());
    }

    @Test
    public void testSerializerEmptyRecord() {
        MapRecord record = new MapRecord(Collections.emptyMap());
        byte[] bytes = serde.serializer().serialize(null, record);
        String json = new String(bytes);
        assertEquals("{}", json);
    }

    @Test
    public void testSerializerNull() {
        byte[] bytes = serde.serializer().serialize(null, null);
        assertNull(bytes);
    }

    @Test
    public void testSerializerNullRecord() {
        MapRecord record = new MapRecord(null);
        byte[] bytes = serde.serializer().serialize(null, record);
        String json = new String(bytes);
        assertEquals("{}", json);
    }

    @Test
    public void testSerializerRecord() {
        Map<String, Object> map = ImmutableMap.of(
                "A", 1,
                "B", "Cat",
                "C", ImmutableList.of(1, 2, 3)
        );
        MapRecord record = new MapRecord(map);
        byte[] bytes = serde.serializer().serialize(null, record);
        String json = new String(bytes);
        assertEquals("{\"A\":1,\"B\":\"Cat\",\"C\":[1,2,3]}", json);
    }
}
