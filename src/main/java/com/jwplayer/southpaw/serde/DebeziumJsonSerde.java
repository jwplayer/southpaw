package com.jwplayer.southpaw.serde;

import java.util.Collections;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.record.MapRecord;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;

/**
 * Combines key/value parsing.  This is really not desirable, but follows the pattern of the other Serde classes.
 * It would be better to have separate key and value
 *  - key can be a simple type or BaseRecord, and value can be BaseRecord.
 *
 * Alternatively this could be implemented using JsonNode and a new {@link BaseRecord} type.
 */
public class DebeziumJsonSerde implements BaseSerde<BaseRecord> {

    KafkaJsonDeserializer<Map<String, ?>> internalDeserializer = new KafkaJsonDeserializer<>();
    boolean isKey = false;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        internalDeserializer.configure(configs, isKey);
        this.isKey = isKey;
    }

    @Override
    public void close() {
        internalDeserializer.close();
    }

    @Override
    public Serializer<BaseRecord> serializer() {
        throw new NotImplementedException();
    }

    @Override
    public Deserializer<BaseRecord> deserializer() {
        return new Deserializer<BaseRecord>() {

            @Override
            public BaseRecord deserialize(String topic, byte[] data) {
                Map<String, ?> envelope = internalDeserializer.deserialize(topic, data);
                if (envelope == null) {
                    return null; //tombstone
                }
                if (isKey || envelope.get("source") == null) {
                    if (envelope instanceof Map) {
                        return new MapRecord(envelope);
                    }
                    //provide a wrapper for a non-object type
                    return new MapRecord(Collections.singletonMap("id", envelope));
                }
                Map<String, ?> after = (Map<String, ?>) envelope.get("after");
                MapRecord result = new MapRecord(after);
                //result.setMetadata(envelope);
                return result;
            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                throw new NotImplementedException();
            }

            @Override
            public void close() {
                throw new NotImplementedException();
            }
        };
    }

}
