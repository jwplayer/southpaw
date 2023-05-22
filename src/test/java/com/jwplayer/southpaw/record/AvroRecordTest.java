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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.jwplayer.southpaw.util.ByteArray;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.common.utils.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;


public class AvroRecordTest {
    private BaseRecord nullRecord;
    private BaseRecord record;

    @Before
    public void setup() {
        nullRecord = new AvroRecord(null);
        Schema schema = SchemaBuilder
                .record("Record").namespace("com.jwplayer.avro")
                .fields()
                .name("string").type().stringType().noDefault()
                .name("int").type().intType().noDefault()
                .name("boolean").type().booleanType().noDefault()
                .name("bytes").type().bytesType().noDefault()
                .name("double").type().doubleType().noDefault()
                .name("float").type().floatType().noDefault()
                .name("long").type().longType().noDefault()
                .name("null").type().nullType().noDefault()
                .name("list").type().array().items().intType().noDefault()
                .name("map").type().map().values().intType().noDefault()
                .name("union1").type().unionOf().stringType().and().nullType().endUnion().noDefault()
                .name("union2").type().unionOf().stringType().and().nullType().endUnion().noDefault()
                .endRecord();
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("string", "TROGDOR BURNINATOR");
        genericRecord.put("int", 45);
        genericRecord.put("boolean", true);
        genericRecord.put("bytes", "ABCD".getBytes());
        genericRecord.put("double", 1.0D);
        genericRecord.put("float", 2.0F);
        genericRecord.put("long", (long) Math.pow(2.0D, 60.0D));
        genericRecord.put("null", null);
        genericRecord.put("list", ImmutableList.of(1, 2, 3));
        genericRecord.put("map", ImmutableMap.of("A", 1, "B", 2, "C", 3));
        genericRecord.put("union1", "I'm a string!");
        genericRecord.put("union2", null);
        record = new AvroRecord(genericRecord);
    }

    @Test
    public void testGetWithNullInternalRecord() {
        assertNull(nullRecord.get("SomeField"));
    }

    @Test
    public void testGetSchema() {
        Map<String, Class> schema = record.getSchema();
        assertEquals(12, schema.size());
        assertTrue(schema.containsKey("string"));
        assertEquals(String.class, schema.get("string"));
        assertTrue(schema.containsKey("int"));
        assertEquals(Integer.class, schema.get("int"));
        assertTrue(schema.containsKey("boolean"));
        assertEquals(Boolean.class, schema.get("boolean"));
        assertTrue(schema.containsKey("bytes"));
        assertEquals(Byte[].class, schema.get("bytes"));
        assertTrue(schema.containsKey("double"));
        assertEquals(Double.class, schema.get("double"));
        assertTrue(schema.containsKey("float"));
        assertEquals(Float.class, schema.get("float"));
        assertTrue(schema.containsKey("long"));
        assertEquals(Long.class, schema.get("long"));
        assertTrue(schema.containsKey("null"));
        assertEquals(Object.class, schema.get("null"));
        assertTrue(schema.containsKey("list"));
        assertEquals(List.class, schema.get("list"));
        assertTrue(schema.containsKey("map"));
        assertEquals(Map.class, schema.get("map"));
        assertTrue(schema.containsKey("union1"));
        assertEquals(Object.class, schema.get("union1"));
        assertTrue(schema.containsKey("union2"));
        assertEquals(Object.class, schema.get("union2"));
    }

    @Test
    public void testGetSchemaWithNullInternalRecord() {
        Map<String, Class> schema = nullRecord.getSchema();

        assertNotNull(schema);
        assertEquals(0, schema.size());
    }

    @Test
    public void testToByteArray() throws Exception {
        Schema schema = SchemaBuilder
                .record("PKRecord").namespace("com.jwplayer.avro")
                .fields()
                .name("string").type().stringType().noDefault()
                .name("int").type().intType().noDefault()
                .endRecord();
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("string", "TROGDOR BURNINATOR");
        genericRecord.put("int", 45);
        BaseRecord pkRecord = new AvroRecord(genericRecord);
        ByteArray pkByteArray = pkRecord.toByteArray();
        byte[] bytes = Hex.decodeHex("012d1254524f47444f52204255524e494e41544f52".toCharArray());
        assertEquals(new ByteArray(bytes), pkByteArray);
    }

    @Test
    public void testByteArrayWithPKOverlapBug() {
        // This tests to make sure that pks like ["12","3"] and ["1","23"] do not generate the same ByteArray
        Schema schema = SchemaBuilder
                .record("PKRecord").namespace("com.jwplayer.avro")
                .fields()
                .name("alpha").type().stringType().noDefault()
                .name("omega").type().stringType().noDefault()
                .endRecord();
        GenericRecord genericRecord1 = new GenericData.Record(schema);
        genericRecord1.put("alpha", "12");
        genericRecord1.put("omega", "3");
        GenericRecord genericRecord2 = new GenericData.Record(schema);
        genericRecord2.put("alpha", "1");
        genericRecord2.put("omega", "23");
        BaseRecord record1 = new AvroRecord(genericRecord1);
        BaseRecord record2 = new AvroRecord(genericRecord2);
        assertNotEquals(record1.toByteArray(), record2.toByteArray());
    }

    @Test
    public void testToMap() {
        Map<String, ?> map = record.toMap();
        assertEquals(12, map.size());
        assertTrue(map.containsKey("string"));
        assertEquals("TROGDOR BURNINATOR", map.get("string"));
        assertTrue(map.containsKey("int"));
        assertEquals(45, map.get("int"));
        assertTrue(map.containsKey("boolean"));
        assertEquals(true, map.get("boolean"));
        assertTrue(map.containsKey("bytes"));
        assertArrayEquals("ABCD".getBytes(), (byte[]) map.get("bytes"));
        assertTrue(map.containsKey("double"));
        assertEquals(1.0D, map.get("double"));
        assertTrue(map.containsKey("float"));
        assertEquals(2.0F, map.get("float"));
        assertTrue(map.containsKey("long"));
        assertEquals((long) Math.pow(2.0D, 60.0D), map.get("long"));
        assertTrue(map.containsKey("null"));
        assertNull(map.get("null"));
        assertTrue(map.containsKey("list"));
        assertEquals(ImmutableList.of(1, 2, 3), map.get("list"));
        assertTrue(map.containsKey("map"));
        assertEquals(ImmutableMap.of("A", 1, "B", 2, "C", 3), map.get("map"));
        assertTrue(map.containsKey("union1"));
        assertEquals("I'm a string!", map.get("union1"));
        assertTrue(map.containsKey("union2"));
        assertNull(map.get("union2"));
    }

    @Test
    public void testToMapWithNullInternalRecord() {
        Map<String, ?> map = nullRecord.toMap();

        assertNotNull(map);
        assertEquals(0, map.size());
    }

    @Test
    public void testToString() {
        String string = record.toString();
        assertNotNull(string);
    }

    @Test
    public void testToStringWithNullInternalRecord() {
        String string = nullRecord.toString();
        assertNotNull(string);
    }
}
