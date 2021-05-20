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

import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.record.MapRecord;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;


public class JsonSerde implements BaseSerde<BaseRecord> {
  JsonDeserializer internalDeserializer;
  JsonSerializer internalSerializer;

  public static class JsonDeserializer implements Deserializer<BaseRecord> {
    private KafkaJsonDeserializer<Map<String, ?>> internalDeserializer;

    public JsonDeserializer() {
      internalDeserializer = new KafkaJsonDeserializer<>();
    }

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
      internalDeserializer.configure(config, isKey);
    }

    @Override
    public BaseRecord deserialize(String topic, byte[] bytes) {
      return new MapRecord(internalDeserializer.deserialize(topic, bytes));
    }

    @Override
    public void close() {
      internalDeserializer.close();
    }
  }

  public static class JsonSerializer implements Serializer<BaseRecord> {
    KafkaJsonSerializer<Map<String, ?>> internalSerializer;

    public JsonSerializer() {
      internalSerializer = new KafkaJsonSerializer<>();
    }

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
      internalSerializer.configure(config, isKey);
    }

    @Override
    public byte[] serialize(String topic, BaseRecord record) {
      if (record == null) {
        return internalSerializer.serialize(topic, null);
      } else {
        return internalSerializer.serialize(topic, record.toMap());
      }
    }

    @Override
    public void close() {
      internalSerializer.close();
    }
  }

  public JsonSerde() {
    internalDeserializer = new JsonDeserializer();
    internalSerializer = new JsonSerializer();
  }

  @Override
  public void configure(Map<String, ?> config, boolean isKey) {
    internalDeserializer.configure(config, isKey);
    internalSerializer.configure(config, isKey);
  }

  @Override
  public void close() {
    internalDeserializer.close();
    internalSerializer.close();
  }

  @Override
  public Serializer<BaseRecord> serializer() {
    return internalSerializer;
  }

  @Override
  public Deserializer<BaseRecord> deserializer() {
    return internalDeserializer;
  }
}
