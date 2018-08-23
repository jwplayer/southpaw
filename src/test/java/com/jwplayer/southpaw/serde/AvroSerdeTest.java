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

import com.jwplayer.southpaw.record.AvroRecord;
import com.jwplayer.southpaw.record.BaseRecord;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;


public class AvroSerdeTest {
    private static final String AVRO_RECORD_HEX = "000000000106426f620a536d6974685a";
    private static final String BAD_RECORD_HEX = "000000000206426f620a536d6974685a";

    private AvroSerde serde;
    private Schema schema;

    @Before
    public void setup() throws Exception {
        // Setup the registry and serde
        MockSchemaRegistryClient registryClient = new MockSchemaRegistryClient();
        serde = new AvroSerde(registryClient);
        Map<String, Object> config = new HashMap<>();
        config.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "nowhere");
        serde.configure(config, false);

        // Create the schema and register it
        schema = SchemaBuilder
                .record("Person").namespace("com.jwplayer.avro")
                .fields()
                .name("first").type().stringType().noDefault()
                .name("last").type().stringType().noDefault()
                .name("age").type().intType().noDefault()
                .endRecord();
        registryClient.register("Person-value", schema);
    }

    @After
    public void cleanup() {
        serde.close();
    }

    @Test(expected = SerializationException.class)
    public void testDeserializerEmptyBytes() {
        byte[] bytes = new byte[0];
        serde.deserializer().deserialize("Person", bytes);
    }

    @Test(expected = SerializationException.class)
    public void testDeserializeInvalidRecord() throws Exception {
        byte[] bytes = Hex.decodeHex(BAD_RECORD_HEX.toCharArray());
        serde.deserializer().deserialize("Person", bytes);
    }

    @Test
    public void testDeserializerNull() {
        BaseRecord record = serde.deserializer().deserialize("Person", null);
        assertTrue(record.isEmpty());
    }

    @Test
    public void testDeserializeRecord() throws Exception {
        byte[] bytes = Hex.decodeHex(AVRO_RECORD_HEX.toCharArray());
        BaseRecord record = serde.deserializer().deserialize("Person", bytes);
        assertNotNull(record);
        assertEquals("Bob", record.get("first"));
        assertEquals("Smith", record.get("last"));
        assertEquals(45, record.get("age"));
    }

    @Test(expected = SerializationException.class)
    public void testSerializerEmptyRecord() {
        GenericRecord genericRecord = new GenericData.Record(schema);
        AvroRecord record = new AvroRecord(genericRecord);
        serde.serializer().serialize("Person", record);
    }

    @Test
    public void testSerializeNullRecord() {
        byte[] bytes = serde.serializer().serialize("Person", null);
        assertNull(bytes);
    }

    @Test
    public void testSerializeRecord() {
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("first", "Bob");
        genericRecord.put("last", "Smith");
        genericRecord.put("age", 45);
        AvroRecord record = new AvroRecord(genericRecord);
        byte[] bytes = serde.serializer().serialize("Person", record);
        assertEquals(AVRO_RECORD_HEX, Hex.encodeHexString(bytes));
    }
}
