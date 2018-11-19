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
     * The last read offset using the read next method.
     */
    protected long currentOffset;
    /**
     * The first offset for this topic. Used so not all topics start at offset 0 to prevent subtle, hard to debug
     * errors in testing.
     */
    public final long firstOffset;
    /**
     * The internal records
     */
    private List<ConsumerRecord<K, V>> records = new ArrayList<>();
    /**
     * The internal records stored by PK
     */
    private Map<ByteArray, ConsumerRecord<K, V>> recordsByPK = new HashMap<>();

    public InMemoryTopic() {
        Random random = new Random();
        this.firstOffset = Math.abs(random.nextInt(50));
        this.currentOffset = firstOffset - 1;
    }

    public InMemoryTopic(long firstOffset) {
        this.firstOffset = firstOffset;
        this.currentOffset = firstOffset - 1;
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
    public Long getCurrentOffset() {
        return currentOffset;
    }

    @Override
    public long getLag() {
        return records.size() - getCurrentOffset() + firstOffset;
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
        List<ConsumerRecord<K, V>> retVal = new ArrayList<>();
        while(records.size() + firstOffset > currentOffset + 1) {
            currentOffset++;
            retVal.add(records.get(((Long) (currentOffset - firstOffset)).intValue()));
        }
        return retVal.iterator();
    }

    @Override
    public void resetCurrentOffset() {
        currentOffset = firstOffset - 1;
    }

    @Override
    public void write(K key, V value) {
        ConsumerRecord<K, V> record;
        FilterMode filterMode = FilterMode.UPDATE;

        if (value instanceof BaseRecord) {
            filterMode = this.getFilter().filter(this.getShortName(), (BaseRecord) value, null);
        }

        switch (filterMode) {
            case DELETE:
                record = new ConsumerRecord<>(topicName, 0, records.size() + firstOffset, key, null);
                break;
            case UPDATE:
                record = new ConsumerRecord<>(topicName, 0, records.size() + firstOffset, key, value);
                break;
            case SKIP:
            default:
                record = null;
        }

        if (record != null) {
            records.add(record);
            if(key instanceof BaseRecord) {
                recordsByPK.put(((BaseRecord) key).toByteArray(), record);
            } else {
                recordsByPK.put(ByteArray.toByteArray(key), record);
            }
        }
    }
}
