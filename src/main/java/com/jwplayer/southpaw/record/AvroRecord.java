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

import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Wrapper record for Avro's GenericRecord class
 */
public class AvroRecord extends BaseRecord {
    private GenericRecord internalRecord;

    public AvroRecord(GenericRecord internalRecord) {
        this.internalRecord = internalRecord;
    }

    @Override
    public Object get(String fieldName) {
        if(internalRecord == null) return null;
        return parseAvroValue(internalRecord.get(fieldName));
    }

    /**
     * Get the internal record stored by this record
     * @return The internal record
     */
    public GenericRecord getInternalRecord() {
        return internalRecord;
    }

    @Override
    public Map<String, Class> getSchema() {
        Map<String, Class> schema = new HashMap<>();
        if(internalRecord == null) return schema;
        Schema avroSchema = internalRecord.getSchema();

        for(Schema.Field field: avroSchema.getFields()) {
            schema.put(field.name(), parseFieldSchema(field.schema().getType()));
        }

        return schema;
    }

    @Override
    public boolean isEmpty() {
        return internalRecord == null;
    }

    /**
     * Parse the Java type for the given Avro type. Returns Object.class by default if it doesn't know a better type.
     * @param type - The avro type to parse
     * @return The Java type for the given Avro type
     */
    private static Class parseFieldSchema(Schema.Type type) {
        switch(type) {
            case ARRAY:
                return List.class;
            case MAP:
                return Map.class;
            case STRING:
                return String.class;
            case BYTES:
                return Byte[].class;
            case INT:
                return Integer.class;
            case LONG:
                return Long.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case BOOLEAN:
                return Boolean.class;
            default:
                return Object.class;
        }
    }

    /**
     * Parse Avro types, particularly Avro strings.
     * @param value - The Avro value to parse
     * @return The parsed value
     */
    private static Object parseAvroValue(Object value) {
        // Avro strings are stored using a special Avro type instead of using Java primitives
        if(value instanceof Utf8) {
            return value.toString();
        } else if(value instanceof Map<?, ?>) {
            Map<Object, Object> map = new HashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>)value).entrySet()) {
                map.put(parseAvroValue(entry.getKey()), parseAvroValue(entry.getValue()));
            }
            return map;
        } else {
            return value;
        }
    }

    @Override
    public Map<String, ?> toMap() {
        if(internalRecord != null) {
            Map<String, Object> map = new HashMap<>(internalRecord.getSchema().getFields().size());
            for (Map.Entry<String, Class> entry : getSchema().entrySet()) {
                map.put(entry.getKey(), get(entry.getKey()));
            }
            return map;
        } else {
            return new HashMap<>(0);
        }
    }
}
