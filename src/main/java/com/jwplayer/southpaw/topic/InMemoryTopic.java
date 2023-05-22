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

import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.util.ByteArray;
import com.jwplayer.southpaw.filter.BaseFilter.FilterMode;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.*;


/**
 * This is an in memory representation of a topic. Useful for testing. It does not support persistence.
 * @param <K> - The type of the record key
 * @param <V> - the type of the record value
 */
public final class InMemoryTopic<K, V> extends BaseTopic<K, V> {
    /**
     * Internal iterator class
     */
    private static class IMTIterator<K, V> implements Iterator<ConsumerRecord<K, V>> {
        int currentPartition = 0;
        ConsumerRecord<K, V> stagedRecord = null;
        InMemoryTopic<K, V> topic;

        public IMTIterator(InMemoryTopic<K, V> topic) {
            this.topic = topic;
            stageRecord();
        }

        @Override
        public boolean hasNext() {
            if(stagedRecord == null) {
                stageRecord();
                return stagedRecord != null;
            } else {
                return true;
            }
        }

        @Override
        public ConsumerRecord<K, V> next() {
            if(stagedRecord == null) {
                throw new NoSuchElementException();
            } else {
                ConsumerRecord<K, V> nextRecord = stagedRecord;
                stageRecord();
                return nextRecord;
            }
        }

        public void stageRecord() {
            for(int partitionsChecked = 0; partitionsChecked < NUM_PARTITIONS; partitionsChecked++) {
                while(topic.getPartitionLag(currentPartition) > 0) {
                    long currentOffset = topic.currentOffsets.get(currentPartition);
                    long firstOffset = topic.firstOffsets.get(currentPartition);
                    topic.currentOffsets.put(currentPartition, currentOffset + 1);
                    ConsumerRecord<K, V> record
                        = topic.records.get(currentPartition).get((int) (currentOffset - firstOffset));
                    ByteArray parsedKey = topic.getParsedKey(record.key());
                    V oldValue = topic.readByPK(parsedKey);
                    FilterMode filterMode = FilterMode.UPDATE;

                    if (record.value() instanceof BaseRecord) {
                        filterMode = topic.getFilter().filter(
                                topic.getShortName(),
                                (BaseRecord) record.value(),
                                (BaseRecord) oldValue);
                    }
                    V parsedValue;

                    switch (filterMode) {
                        case DELETE:
                            parsedValue = null;
                            break;
                        case UPDATE:
                            parsedValue = record.value();
                            break;
                        case SKIP:
                        default:
                            continue;
                    }
                    stagedRecord = new ConsumerRecord<>(
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            parsedValue);
                    topic.recordsByPK.put(topic.getParsedKey(record.key()), stagedRecord);
                    currentPartition = (currentPartition + 1) % NUM_PARTITIONS;
                    return;
                }
                currentPartition = (currentPartition + 1) % NUM_PARTITIONS;
            }
            stagedRecord = null;
        }
    }

    public static final int NUM_PARTITIONS = 3;
    /**
     * The last read offsets by partition using the read next method.
     */
    private final Map<Integer, Long> currentOffsets = new HashMap<>();
    /**
     * The first offsets by partition for this topic. Used so not all topics start at offset 0 to prevent subtle, hard
     * to debug errors in testing.
     */
    private final Map<Integer, Long> firstOffsets;
    /**
     * The internal records
     */
    private final Map<Integer, List<ConsumerRecord<K, V>>> records = new HashMap<>();
    /**
     * The internal records stored by PK
     */
    private final Map<ByteArray, ConsumerRecord<K, V>> recordsByPK = new HashMap<>();

    public InMemoryTopic() {
        Random random = new Random();
        this.firstOffsets = new HashMap<>();
        for(int i = 0; i < NUM_PARTITIONS; i++) {
            long offset = Math.abs(random.nextInt(50));
            this.currentOffsets.put(i, offset);
            this.firstOffsets.put(i, offset);
            this.records.put(i, new ArrayList<>());
        }
    }

    @Override
    public void commit() {
        // noop
    }

    @Override
    public void flush() {
        // noop
    }

    @Override
    public Map<Integer, Long> getCurrentOffsets() {
        return this.currentOffsets;
    }

    @Override
    public long getLag() {
        long lag = 0;
        for(int i = 0; i < NUM_PARTITIONS; i++) {
            lag += getPartitionLag(i);
        }
        return lag;
    }

    private ByteArray getParsedKey(K key) {
        if(key instanceof BaseRecord) {
            return ((BaseRecord) key).toByteArray();
        } else {
            return ByteArray.toByteArray(key);
        }
    }

    private long getPartitionLag(int partition) {
        return records.get(partition).size() + firstOffsets.get(partition) - currentOffsets.get(partition);
    }

    @Override
    public V readByPK(ByteArray primaryKey) {
        if(primaryKey == null) {
            return null;
        } else {
            ConsumerRecord<K, V> record = recordsByPK.get(primaryKey);
            if(record == null) {
                return null;
            } else {
                return record.value();
            }
        }
    }

    @Override
    public Iterator<ConsumerRecord<K, V>> readNext() {
        return new IMTIterator<>(this);
    }

    @Override
    public void resetCurrentOffsets() {
        this.currentOffsets.putAll(firstOffsets);
    }

    @Override
    public void write(K key, V value) {
        ConsumerRecord<K, V> record;
        int partition = getParsedKey(key).hashCode() % NUM_PARTITIONS;
        long newOffset = records.get(partition).size() + firstOffsets.get(partition);
        record = new ConsumerRecord<>(topicName, partition, newOffset, key, value);
        records.get(partition).add(record);
    }
}
