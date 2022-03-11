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

import com.jwplayer.southpaw.util.ByteArray;
import com.jwplayer.southpaw.filter.BaseFilter;
import com.jwplayer.southpaw.state.BaseState;
import com.jwplayer.southpaw.metric.Metrics;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;

import java.util.Iterator;


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
    public static final String FILTER_CLASS_DEFAULT = "com.jwplayer.southpaw.filter.BaseFilter";
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
     * Configuration object
     */
    protected TopicConfig<K, V> topicConfig;
    /**
     * The full topic name (e.g. my.topic.user)
     */
    protected String topicName;

    /**
     * Commits the current offsets and data to the state. Use after reading messages using the readNext method,
     * but only after all processing of those messages is complete.
     */
    public abstract void commit();

    /**
     * Configures the topic object. Should be called after instantiation.
     * @param config - This topic configuration
     */
    public void configure(TopicConfig<K, V> config) {
        // Store the configuration for this topic
        this.topicConfig = config;

        // Initialize topic
        this.topicName = this.topicConfig.southpawConfig.getOrDefault(TOPIC_NAME_CONFIG, "").toString();
        this.topicConfig.state.createKeySpace(this.getShortName() + "-" + DATA);
        this.topicConfig.state.createKeySpace(this.getShortName() + "-" + OFFSETS);
    }

    /**
     * Flushes any uncommitted writes
     */
    public abstract void flush();

    /**
     * Accessor for the current offset.
     * @return Current (long) offset.
     */
    public abstract Map<Integer, Long> getCurrentOffsets();

    /**
     * Accessor for the key serde.
     * @return The key serde
     */
    public Serde<K> getKeySerde() {
        return this.topicConfig.keySerde;
    }

    /**
     * Accessor for the value serde
     * @return The value serde
     */
    public Serde<V> getValueSerde() {
        return this.topicConfig.valueSerde;
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
        return this.topicConfig.shortName;
    }

    /**
     * Accessor for the topic name.
     * @return The full topic name.
     */
    public String getTopicName() {
        return topicName;
    }

    /**
     * Accessor for the topic filter.
     * @return The topic filter.
     */
    public BaseFilter getFilter() {
        return this.topicConfig.filter;
    }

    /**
     * Accessor for the topic metrics.
     * @return The topic metrics.
     */
    public Metrics getMetrics() {
        return this.topicConfig.metrics;
    }

    /**
     * Accessor for the topic state.
     * @return The topic state.
     */
    public BaseState getState() {
        return this.topicConfig.state;
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
     * Resets the current offsets to the beginning of the topic.
     */
    public abstract void resetCurrentOffsets();

    /**
     * Gives a nicely formatted string representation of this object. Useful for the Intellij debugger.
     * @return Formatted string representation of this object
     */
    public String toString() {
        return String.format(
                "{shortName=%s,topicName=%s,currentOffsets=%s,keySerde=%s,valueSerde=%s}",
                this.topicConfig.shortName,
                topicName,
                getCurrentOffsets(),
                this.topicConfig.keySerde.getClass().getName(),
                this.topicConfig.valueSerde.getClass().getName()
        );
    }

    /**
     * Writes the serialized kv pair to Kafka.
     * @param key - The serialized key.
     * @param value - The serialized value.
     */
    public abstract void write(K key, V value);
}
