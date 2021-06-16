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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;


/**
 * Special serde for converting from JSON to auto-created objects using Jackson. Does not use BaseRecord like the
 * other serdes. Used for writing the denormalized records to Kafka.
 *
 * @param <T> - The auto-created object type
 */
public class JacksonSerde<T> implements Serde<T> {
  /**
   * Config for the class (de)serialized by this serde
   */
  public static final String CLASS_CONFIG = "jackson.serde.class";

  protected Class<T> clazz;
  protected JacksonDeserializer internalDeserializer;
  protected JacksonSerializer internalSerializer;

  public class JacksonDeserializer implements Deserializer<T> {
    private final ObjectMapper mapper = new ObjectMapper();

    public JacksonDeserializer() {
    }

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(String topic, byte[] bytes) {
      try {
        if (bytes == null) {
          return null;
        }
        return mapper.readValue(new String(bytes), clazz);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public void close() {
    }
  }

  public class JacksonSerializer implements Serializer<T> {
    private final ObjectMapper mapper = new ObjectMapper();

    public JacksonSerializer() {
    }

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, T record) {
      try {
        if (record == null) {
          return null;
        }
        return mapper.writeValueAsBytes(record);
      } catch (JsonProcessingException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public void close() {
    }
  }

  public JacksonSerde() {
    internalDeserializer = new JacksonDeserializer();
    internalSerializer = new JacksonSerializer();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> config, boolean isKey) {
    String type = Preconditions.checkNotNull(config.get(CLASS_CONFIG)).toString();
    try {
      clazz = (Class<T>) Class.forName(type);
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }
    internalDeserializer.configure(config, isKey);
    internalSerializer.configure(config, isKey);
  }

  @Override
  public void close() {
    internalDeserializer.close();
    internalSerializer.close();
  }

  @Override
  public Serializer<T> serializer() {
    return internalSerializer;
  }

  @Override
  public Deserializer<T> deserializer() {
    return internalDeserializer;
  }
}
