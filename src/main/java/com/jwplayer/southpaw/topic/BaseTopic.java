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
package com.jwplayer.southpaw.topic;

import com.google.common.base.Preconditions;
import com.jwplayer.southpaw.filter.BaseFilter;
import com.jwplayer.southpaw.state.BaseState;
import com.jwplayer.southpaw.util.ByteArray;
import com.jwplayer.southpaw.metric.Metrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;

import java.util.Iterator;
import java.util.Map;


/**
 * Base class that defines the interface for the storage system (Kafka, or a mocked storage for testing).
 * Exposes necessary functionality and nothing else.
 * @param <K> - The type of the record key
 * @param <V> - the type of the record value
 */
public abstract class BaseTopic<K, V> {
    /**
     * Used as the suffix of the group name where topic data is stored.
     */
    public static final String DATA = "data";
    /**
     * Used as the suffix of the group name where offsets are stored.
     */
    public static final String OFFSETS = "offsets";
    public static final String FILTER_CLASS_CONFIG = "filter.class";
    public static final String FILTER_CLASS_DEFAULT = "com.jwplayer.southpaw.filter.DefaultFilter";
    public static final String FILTER_CLASS_DOC = "Filter class used to filter input records, treating them " +
            "like tombstones and not recording them in the local state.";
    public static final String KEY_SERDE_CLASS_CONFIG = "key.serde.class";
    public static final String KEY_SERDE_CLASS_DOC =
            "Config option for specifying the key serde class for an input record";
    public static final String TOPIC_CLASS_CONFIG = "topic.class";
    public static final String TOPIC_CLASS_DOC =
            "The topic class to use for Southpaw. The different topics can use different topic implementations.";
    /**
     * Config option for specifying the name of the topic
     */
    public static final String TOPIC_NAME_CONFIG = "topic.name";
    public static final String VALUE_SERDE_CLASS_CONFIG = "value.serde.class";
    public static final String VALUE_SERDE_CLASS_DOC =
            "Config option for specifying the value serde class for an input record";

    /**
     * Configuration for this topic
     */
    protected Map<String, Object> config;
    /**
     * Filter used to filter out consumed records, treating them like a tombstone
     */
    protected BaseFilter filter;
    /**
     * The serde for (de)serializing Kafka record keys
     */
    protected Serde<K> keySerde;
    /**
     * The short name for this topic, could be the entity stored in this topic and used in indices (e.g. user)
     */
    protected String shortName;
    /**
     * We store offsets in the state instead of Kafka.
     */
    protected BaseState state;
    /**
     * The full topic name (e.g. my.topic.user)
     */
    protected String topicName;
    /**
     * The serde for (de)serializing Kafka record values
     */
    protected Serde<V> valueSerde;
    /**
     * Reference to metrics class for metric reporting
     */
    protected Metrics metrics;

    /**
     * Commits the current offsets and data to the state. Use after reading messages using the readNext method,
     * but only after all processing of those messages is complete.
     */
    public abstract void commit();

    /**
     * Configures the topic object. Should be called after instantiation.
     * @param shortName - The short name for this topic, could be the entity stored in this topic and used in indices (e.g. user)
     * @param config - This topic configuration
     * @param state - The state where we store the offsets for this topic
     * @param keySerde - The serde for (de)serializing Kafka record keys
     * @param valueSerde - The serde for (de)serializing Kafka record values
     * @param filter - The filter used to filter out consumed records, treating them like a tombstone
     */
    public void configure(
        String shortName,
        Map<String, Object> config,
        BaseState state,
        Serde<K> keySerde,
        Serde<V> valueSerde,
        BaseFilter filter
    ) {
        this.configure(
            shortName,
            config,
            state,
            keySerde,
            valueSerde,
            filter,
            null
        );
    }

    /**
     * Configures the topic object. Should be called after instantiation.
     * @param shortName - The short name for this topic, could be the entity stored in this topic and used in indices (e.g. user)
     * @param config - This topic configuration
     * @param state - The state where we store the offsets for this topic
     * @param keySerde - The serde for (de)serializing Kafka record keys
     * @param valueSerde - The serde for (de)serializing Kafka record values
     * @param filter - The filter used to filter out consumed records, treating them like a tombstone
     * @param metrics - The Southpaw Metrics object
     */
    public void configure(
            String shortName,
            Map<String, Object> config,
            BaseState state,
            Serde<K> keySerde,
            Serde<V> valueSerde,
            BaseFilter filter,
            Metrics metrics
    ) {
        this.shortName = shortName;
        this.config = Preconditions.checkNotNull(config);
        this.topicName = config.getOrDefault(TOPIC_NAME_CONFIG, "").toString();
        this.state = state;
        this.state.createKeySpace(shortName + "-" + DATA);
        this.state.createKeySpace(shortName + "-" + OFFSETS);
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.filter = filter;
        this.metrics = metrics;
    }

    /**
     * Flushes any uncommitted writes
     */
    public abstract void flush();

    /**
     * Accessor for the current offset.
     * @return Current (long) offset.
     */
    public abstract Long getCurrentOffset();

    /**
     * Accessor for the key serde.
     * @return The key serde
     */
    public Serde<K> getKeySerde() {
        return keySerde;
    }

    /**
     * Gets the difference between the current offset and last offset of this topic
     * @return The lag of this topic
     */
    public abstract long getLag();

    /**
     * Accessor for the short name for this topic.
     * @return The short name for this topic.
     */
    public String getShortName() {
        return shortName;
    }

    /**
     * Accessor for the topic name.
     * @return The full topic name.
     */
    public String getTopicName() {
        return topicName;
    }

    /**
     * Accessor for the value serde
     * @return The value serde
     */
    public Serde<V> getValueSerde() {
        return valueSerde;
    }

    /**
     * Reads a single record value from the state based on that record's primary key.
     * @param primaryKey - The primary key of the record to read.
     * @return A single record value, or null if there is no record with that PK.
     */
    public abstract V readByPK(ByteArray primaryKey);

    /**
     * Reads records in topic based on the current offset.
     * @return The list of records read.
     */
    public abstract Iterator<ConsumerRecord<K, V>> readNext();

    /**
     * Resets the current offset to the beginning of the topic.
     */
    public abstract void resetCurrentOffset();

    /**
     * Gives a nicely formatted string representation of this object. Useful for the Intellij debugger.
     * @return Formatted string representation of this object
     */
    public String toString() {
        return String.format(
                "{shortName=%s,topicName=%s,currentOffset=%s,keySerde=%s,valueSerde=%s}",
                shortName,
                topicName,
                getCurrentOffset(),
                keySerde.getClass().getName(),
                valueSerde.getClass().getName()
        );
    }

    /**
     * Writes the serialized kv pair to Kafka.
     * @param key - The serialized key.
     * @param value - The serialized value.
     */
    public abstract void write(K key, V value);
}
