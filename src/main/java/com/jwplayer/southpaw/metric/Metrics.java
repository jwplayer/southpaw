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
package com.jwplayer.southpaw.metric;

import com.codahale.metrics.*;
import com.codahale.metrics.jmx.JmxReporter;
import com.jwplayer.southpaw.strategy.QueueingStrategy;

import java.util.*;

/**
 * Simple metrics class for Southpaw
 */
public class Metrics {
    public static final String PREFIX = "jw.southpaw";
    public static final String BACKUPS_CREATED = "backups.created";
    public static final String BACKUPS_DELETED = "backups.deleted";
    public static final String BACKUPS_RESTORED = "backups.restored";
    public static final String DENORMALIZED_RECORDS_CREATED = "denormalized.records.created";
    public static final String DENORMALIZED_RECORDS_DROPPED = "denormalized.records.dropped";
    public static final String DENORMALIZED_RECORDS_TO_CREATE = "denormalized.records.to.create";
    public static final String INDEX_ENTRIES_SIZE = "index.entries.size";
    public static final String INDEX_REVERSE_ENTRIES_SIZE = "index.reverse.entries.size";
    public static final String RECORDS_CONSUMED = "records.consumed";
    public static final String STATE_COMMITTED = "states.committed";
    public static final String STATES_DELETED = "states.deleted";
    public static final String TIME_SINCE_LAST_BACKUP = "time.since.last.backup";
    public static final String TOPIC_LAG = "topic.lag";

    /**
     * Registry where metrics are registered
     */
    protected static final MetricRegistry registry = new MetricRegistry();
    /**
     * Send the metrics to JMX.
     */
    protected final JmxReporter reporter = JmxReporter.forRegistry(registry).inDomain(PREFIX).build();
    /**
     * Timer for backups created
     */
    public final com.codahale.metrics.Timer backupsCreated = registry.timer(BACKUPS_CREATED);
    /**
     * Number of backups deleted
     */
    public final Meter backupsDeleted = registry.meter(BACKUPS_DELETED);
    /**
     * Timer for backups restored
     */
    public final com.codahale.metrics.Timer backupsRestored = registry.timer(BACKUPS_RESTORED);
    /**
     * The number of denormalized records created for all topics
     */
    public final Meter denormalizedRecordsCreated = registry.meter(DENORMALIZED_RECORDS_CREATED);
    /**
     * The number of denormalized records created by topic
     */
    public final Map<String, Meter> denormalizedRecordsCreatedByTopic = new HashMap<>();
    /**
     * The number of denormalized records created by topic and priority
     */
    public final Map<String, Map<QueueingStrategy.Priority, Meter>> denormalizedRecordsCreatedByTopicAndPriority = new HashMap<>();
    /**
     * The number of denormalized records dropped due to having a priority of "none" for all topics
     */
    public final Meter denormalizedRecordsDropped = registry.meter(DENORMALIZED_RECORDS_DROPPED);
    /**
     * The number of denormalized records dropped due to having a priority of "none" by topic
     */
    public final Map<String, Meter> denormalizedRecordsDroppedByTopic = new HashMap<>();
    /**
     * The number of denormalized records queued to create for all topics
     */
    public StaticGauge<Long> denormalizedRecordsToCreate =  new StaticGauge<>();
    /**
     * The number of denormalized records to create by topic
     */
    public final Map<String, StaticGauge<Long>> denormalizedRecordsToCreateByTopic = new HashMap<>();
    /**
     * The number of denormalized records to create by topic and priority queue
     */
    public final Map<String, Map<QueueingStrategy.Priority, StaticGauge<Long>>> denormalizedRecordsToCreateByTopicAndPriority = new HashMap<>();
    /**
     * Histogram of the size of entries for each index
     */
    public final Map<String, Histogram> indexEntriesSize = new HashMap<>();
    /**
     * Histogram of the size of entries for each reverse index
     */
    public final Map<String, Histogram> indexReverseEntriesSize = new HashMap<>();
    /**
     * Number of records consumed from all topics
     */
    public final Meter recordsConsumed = registry.meter(RECORDS_CONSUMED);
    /**
     * Number of records consumed by topic
     */
    public final Map<String, Meter> recordsConsumedByTopic = new HashMap<>();
    /**
     * The amount of time each state commit takes to run in milliseconds
     */
    public final com.codahale.metrics.Timer stateCommitted = registry.timer(STATE_COMMITTED);
    /**
     * The number of states deleted
     */
    public final Meter statesDeleted = registry.meter(STATES_DELETED);
    /**
     * The number of records yet to be consumed from all topics
     */
    public StaticGauge<Long> topicLag = new StaticGauge<>();
    /**
     * Time since the last backup
     */
    public StaticGauge<Long> timeSinceLastBackup;
    /**
     * The number of records yet to be consumed by topic
     */
    public final Map<String, StaticGauge<Long>> topicLagByTopic = new HashMap<>();

    /**
     * Constructor
     */
    @SuppressWarnings("unchecked")
    public Metrics() {
        reporter.start();
        if(!registry.getMetrics().containsKey(TIME_SINCE_LAST_BACKUP)) {
            timeSinceLastBackup = new StaticGauge<>();
            registry.register(TIME_SINCE_LAST_BACKUP, timeSinceLastBackup);
        } else {
            timeSinceLastBackup = (StaticGauge<Long>) registry.getMetrics().get(TIME_SINCE_LAST_BACKUP);
        }
        if(!registry.getMetrics().containsKey(TOPIC_LAG)) {
            registry.register(TOPIC_LAG, topicLag);
        } else {
            topicLag = (StaticGauge<Long>) registry.getMetrics().get(TOPIC_LAG);
        }
        if(!registry.getMetrics().containsKey(DENORMALIZED_RECORDS_TO_CREATE)) {
            registry.register(DENORMALIZED_RECORDS_TO_CREATE, denormalizedRecordsToCreate);
        } else {
            denormalizedRecordsToCreate = (StaticGauge<Long>) registry.getMetrics().get(DENORMALIZED_RECORDS_TO_CREATE);
        }
    }

    /**
     * Stops reporting on this metrics object
     */
    public void close() {
        reporter.close();
    }

    /**
     * Register an input topic for per topic metrics.
     * @param shortName - The topic short name to register the metric under
     */
    @SuppressWarnings("unchecked")
    public void registerInputTopic(String shortName) {
        String meterName = String.join(".", RECORDS_CONSUMED, shortName);
        if(!registry.getMetrics().containsKey(meterName)) {
            recordsConsumedByTopic.put(shortName, registry.meter(meterName));
        } else {
            recordsConsumedByTopic.put(shortName, (Meter) registry.getMetrics().get(meterName));
        }
        meterName = String.join(".", TOPIC_LAG, shortName);
        if(!registry.getMetrics().containsKey(meterName)) {
            StaticGauge<Long> gauge = new StaticGauge<>();
            registry.register(meterName, gauge);
            topicLagByTopic.put(shortName, gauge);
        } else {
            topicLagByTopic.put(shortName, (StaticGauge<Long>) registry.getMetrics().get(meterName));
        }
    }

    /**
     * Register an output topic for per topic metrics.
     * @param shortName - The topic short name to register the metric under
     */
    @SuppressWarnings("unchecked")
    public void registerOutputTopic(String shortName) {
        String meterName = String.join(".", DENORMALIZED_RECORDS_CREATED, shortName);
        if(!registry.getMetrics().containsKey(meterName)) {
            denormalizedRecordsCreatedByTopic.put(shortName, registry.meter(meterName));
        } else {
            denormalizedRecordsCreatedByTopic.put(shortName, (Meter) registry.getMetrics().get(meterName));
        }
        meterName = String.join(".", DENORMALIZED_RECORDS_DROPPED, shortName);
        if(!registry.getMetrics().containsKey(meterName)) {
            denormalizedRecordsDroppedByTopic.put(shortName, registry.meter(meterName));
        } else {
            denormalizedRecordsDroppedByTopic.put(shortName, (Meter) registry.getMetrics().get(meterName));
        }
        denormalizedRecordsCreatedByTopicAndPriority.put(shortName, new HashMap<>());
        for(QueueingStrategy.Priority priority: QueueingStrategy.Priority.values()) {
            if(priority == QueueingStrategy.Priority.NONE) continue;
            meterName = String.join(".", DENORMALIZED_RECORDS_CREATED, priority.name().toLowerCase(), shortName);
            if(!registry.getMetrics().containsKey(meterName)) {
                denormalizedRecordsCreatedByTopicAndPriority.get(shortName).put(priority, registry.meter(meterName));
            } else {
                denormalizedRecordsCreatedByTopicAndPriority.get(shortName).put(priority, (Meter) registry.getMetrics().get(meterName));
            }
        }
        meterName = String.join(".", DENORMALIZED_RECORDS_TO_CREATE, shortName);
        if(!registry.getMetrics().containsKey(meterName)) {
            denormalizedRecordsToCreateByTopic.put(shortName, registry.register(meterName, new StaticGauge<>()));
        } else {
            denormalizedRecordsToCreateByTopic.put(shortName, (StaticGauge<Long>) registry.getMetrics().get(meterName));
        }
        denormalizedRecordsToCreateByTopicAndPriority.put(shortName, new HashMap<>());
        for(QueueingStrategy.Priority priority: QueueingStrategy.Priority.values()) {
            if(priority == QueueingStrategy.Priority.NONE) continue;
            meterName = String.join(".", DENORMALIZED_RECORDS_TO_CREATE, priority.name().toLowerCase(), shortName);
            if(!registry.getMetrics().containsKey(meterName)) {
                denormalizedRecordsToCreateByTopicAndPriority.get(shortName).put(priority, new StaticGauge<>());
            } else {
                denormalizedRecordsToCreateByTopicAndPriority.get(shortName).put(priority, (StaticGauge<Long>) registry.getMetrics().get(meterName));
            }
        }
    }

    /**
     * Register an index's metrics
     * @param indexName - The name of the index to register
     */
    public void registerIndex(String indexName) {
        String meterName = String.join(".", INDEX_ENTRIES_SIZE, indexName);
        if(!registry.getMetrics().containsKey(meterName)) {
            indexEntriesSize.put(indexName, registry.histogram(meterName));
        } else {
            indexEntriesSize.put(indexName, (Histogram) registry.getMetrics().get(meterName));
        }
        meterName = String.join(".", INDEX_REVERSE_ENTRIES_SIZE, indexName);
        if(!registry.getMetrics().containsKey(meterName)) {
            indexReverseEntriesSize.put(indexName, registry.histogram(meterName));
        } else {
            indexReverseEntriesSize.put(indexName, (Histogram) registry.getMetrics().get(meterName));
        }
    }
}
