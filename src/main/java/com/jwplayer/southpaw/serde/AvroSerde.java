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
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;


/**
 * Our AvroSerde that uses the GenericAvroSerde, but returns our generic AvroRecord
 */
public class AvroSerde implements BaseSerde<BaseRecord> {
    /**
     * The deserializer used by this serde
     */
    private AvroDeserializer deserializer;
    /**
     * The serializer used by this serde
     */
    private AvroSerializer serializer;

    /**
     * Why do so many open source projects love making trivial things difficult to extend through private access?
     */
    public static class KafkaAvroDeserializer extends io.confluent.kafka.serializers.KafkaAvroDeserializer {
        public KafkaAvroDeserializer() {
            super();
        }

        public KafkaAvroDeserializer(SchemaRegistryClient client) {
            super(client);
        }

        public GenericRecord deserialize(String topic, boolean isKey, byte[] bytes) {
            return (GenericRecord) this.deserialize(true, topic, isKey, bytes, null);
        }
    }

    /**
     * A simple wrapper for GenericAvroDeserializer that works on our AvroRecord instead of GenericRecord
     */
    public static class AvroDeserializer implements Deserializer<BaseRecord> {
        protected boolean isKey;
        KafkaAvroDeserializer internalDeserializer;

        public AvroDeserializer(KafkaAvroDeserializer internalDeserializer) {
            this.internalDeserializer = internalDeserializer;
        }

        @Override
        public void configure(Map<String, ?> config, boolean isKey) {
            this.isKey = isKey;
            internalDeserializer.configure(config, isKey);
        }

        @Override
        public BaseRecord deserialize(String topic, byte[] bytes) {
            return new AvroRecord(internalDeserializer.deserialize(topic, isKey, bytes));
        }

        @Override
        public void close() {
            internalDeserializer.close();
        }
    }

    /**
     * A simple wrapper for GenericAvroSerializer that works on our AvroRecord instead of GenericRecord
     */
    public static class AvroSerializer implements Serializer<BaseRecord> {
        Serializer<Object> internalSerializer;

        public AvroSerializer(Serializer<Object> internalSerializer) {
            this.internalSerializer = internalSerializer;
        }

        @Override
        public void configure(Map<String, ?> config, boolean isKey) {
            internalSerializer.configure(config, isKey);
        }

        @Override
        public byte[] serialize(String topic, BaseRecord record) {
            if(record == null) {
                return internalSerializer.serialize(topic, null);
            } else {
                return internalSerializer.serialize(topic, ((AvroRecord) record).getInternalRecord());
            }
        }

        @Override
        public void close() {
            internalSerializer.close();
        }
    }

    public AvroSerde() {
        this.deserializer = new AvroDeserializer(new KafkaAvroDeserializer());
        this.serializer = new AvroSerializer(new KafkaAvroSerializer());
    }

    public AvroSerde(SchemaRegistryClient client) {
        this.deserializer = new AvroDeserializer(new KafkaAvroDeserializer(client));
        this.serializer = new AvroSerializer(new KafkaAvroSerializer(client));
    }

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
        deserializer.configure(config, isKey);
        serializer.configure(config, isKey);
    }

    @Override
    public void close() {
        deserializer.close();
        serializer.close();
    }

    @Override
    public Serializer<BaseRecord> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<BaseRecord> deserializer() {
        return deserializer;
    }
}
