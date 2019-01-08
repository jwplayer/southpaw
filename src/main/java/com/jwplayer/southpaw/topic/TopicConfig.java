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
import com.jwplayer.southpaw.metric.Metrics;
import org.apache.kafka.common.serialization.Serde;

import java.util.Map;


public final class TopicConfig<K, V> {

    public String shortName;
    public Map<String, Object> southpawConfig;
    public BaseState state;
    public Serde<K> keySerde;
    public Serde<V> valueSerde;
    public BaseFilter filter;
    public Metrics metrics;

    /**
     * Default constructor
     */
    public TopicConfig() {
        this.shortName = null;
        this.southpawConfig = null;
        this.state = null;
        this.keySerde = null;
        this.valueSerde = null;
        this.filter = null;
        this.metrics = null;
    }

    /**
     * Topic configuration constructor
     * 
     * @param shortName - The short name for this topic, could be the entity stored in this topic and used in indices (e.g. user)
     * @param southpawConfig - Southpaw configuration
     * @param state - The state where we store the offsets for this topic
     * @param keySerde - The serde for (de)serializing Kafka record keys
     * @param valueSerde - The serde for (de)serializing Kafka record values
     * @param filter - The filter used to filter out consumed records, treating them like a tombstone
     * @param metrics - The Southpaw Metrics object
     */
    public TopicConfig(
        String shortName,
        Map<String, Object> southpawConfig,
        BaseState state,
        Serde<K> keySerde,
        Serde<V> valueSerde,
        BaseFilter filter,
        Metrics metrics
    ) {
        this.setShortName(shortName);
        this.setSouthpawConfig(southpawConfig);
        this.setState(state);
        this.setKeySerde(keySerde);
        this.setValueSerde(valueSerde);
        this.setFilter(filter);
        this.setMetrics(metrics);
    }

    public TopicConfig<K, V> setShortName(String shortName) {
        this.shortName = shortName;
        return this;
    }

    public TopicConfig<K, V> setSouthpawConfig(Map<String, Object> config) {
        this.southpawConfig = Preconditions.checkNotNull(config);
        return this;
    }

    public TopicConfig<K, V> setState(BaseState state) {
        this.state = state;
        return this;
    }

    public TopicConfig<K, V> setKeySerde(Serde<K> keySerde) {
        this.keySerde = keySerde;
        return this;
    }

    public TopicConfig<K, V> setValueSerde(Serde<V> valueSerde) {
        this.valueSerde = valueSerde;
        return this;
    }

    public TopicConfig<K, V> setFilter(BaseFilter filter) {
        this.filter = filter;
        return this;
    }

    public TopicConfig<K, V> setMetrics(Metrics metrics) {
        this.metrics = metrics;
        return this;
    }
}