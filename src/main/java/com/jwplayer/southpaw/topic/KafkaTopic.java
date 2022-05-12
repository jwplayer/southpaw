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

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.jwplayer.southpaw.filter.BaseFilter.FilterMode;
import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.util.ByteArray;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Kafka implementation of the topic abstraction.
 * @param <K> - The type of the record key
 * @param <V> - the type of the record value
 */
public class KafkaTopic<K, V> extends BaseTopic<K, V> {
    public static final long END_OFFSET_REFRESH_MS_DEFAULT = 60000;
    public static final String POLL_TIMEOUT_CONFIG = "poll.timeout";
    public static final long POLL_TIMEOUT_DEFAULT = 1000;

    /**
     * Le Logger
     */
    private static final Logger logger = LoggerFactory.getLogger(KafkaTopic.class);

    /**
     * This allows us to capture the record and update our state when next() is called.
     */
    private static class KafkaTopicIterator<K, V> implements Iterator<ConsumerRecord<K, V>> {
        
        protected Iterator<ConsumerRecord<byte[], byte[]>> iter;
        protected KafkaTopic<K, V> topic;

        /*
         * Information about the next valid record:
         * 
         * The next record needs to be staged as we'll have consumed it in hasNext() by calling next()
         * nextRecord can be either (1) NULL or (2) not NULL:
         * 
         *  (1) When NULL this indicates we don't have a staged record available, 
         *     there might be another record available in the Kafka topic.
         *  (2) When not NULL, we have a staged record that has passed filtering already
         *      nextRecord is the what should be returned from next()
        */
        private ConsumerRecord<byte[], byte[]> nextRecord;
        private FilterMode nextRecordFilterMode;
        private ByteArray nextRecordPrimaryKey;

        /**
         * Constructor
         * @param iter - The iterator to wrap
         * @param topic - The topic whose offsets we'll update
         */
        private KafkaTopicIterator(Iterator<ConsumerRecord<byte[], byte[]>> iter, KafkaTopic<K, V> topic) {
            this.iter = iter;
            this.topic = topic;
            this.resetStagedRecord();
        }

        /**
         * Internal helper to obtain and stage the next non-skipped record
         * 
         * @return ConsumerRecord - The next non-skipped record
         */
        private ConsumerRecord<byte[], byte[]> getAndStageNextRecord() {

            // If there exists a pre-staged record, our work is done
            if (this.nextRecord != null) {
                return this.nextRecord;
            }

            ConsumerRecord<byte[], byte[]> record = null;
            BaseRecord oldRec = null;
            FilterMode filterMode = FilterMode.SKIP;
            ByteArray primaryKey = null;
            K key;
            V value, currState;

            // Obtain a record and stage it
            while(iter.hasNext() && filterMode == FilterMode.SKIP) {
                record = iter.next();
                // The current offset is one ahead of the last read one. 
                // This copies what Kafka would return as the current offset.
                topic.setCurrentOffset(record.partition(), record.offset() + 1L);

                key = topic.getKeySerde().deserializer().deserialize(record.topic(), record.key());
                value = topic.getValueSerde().deserializer().deserialize(record.topic(), record.value());

                if (key instanceof BaseRecord) {
                    primaryKey = ((BaseRecord) key).toByteArray();
                } else {
                    primaryKey = new ByteArray(record.key());
                }

                currState = topic.readByPK(primaryKey);
                if (currState instanceof BaseRecord) {
                    oldRec = (BaseRecord) currState;
                }

                // Non-BaseRecord value types will not be filtered
                if (value instanceof BaseRecord) {
                    filterMode = topic.getFilter().filter(topic.topicConfig.shortName, (BaseRecord) value, oldRec);
                } else {
                    filterMode = FilterMode.UPDATE;
                }

                // If the record is classified as to be skipped
                // increment the appropriate metrics to indicate we have finished consuming
                // an input topic record.
                // In the case that the record is not flagged as skip, we'll rely on the caller
                // to increment appropriate metrics when it has finished consuming the record.
                if (filterMode == FilterMode.SKIP && this.topic.getMetrics() != null) {
                    this.topic.getMetrics().recordsConsumed.mark(1);
                    this.topic.getMetrics().recordsConsumedByTopic.get(this.topic.getShortName()).mark(1);
                }
            }

            // by this point, we've either reached the end of the topic with no message found
            // or, we have a valid record available
            if (record != null && filterMode != FilterMode.SKIP) {
                // if the last record's filter mode was not skip, we have a valid record
                // update internal state to reflect
                this.nextRecord = record;
                this.nextRecordFilterMode = filterMode;
                this.nextRecordPrimaryKey = primaryKey;
            }
            
            return this.nextRecord;
        }

        /**
         * Reset the staging variables referring to the next available record
         * This function is intended to be called once the staged next 
         * record is consumed (returned by the next function).
         */
        private void resetStagedRecord() {
            this.nextRecord = null;
            this.nextRecordFilterMode = null;
            this.nextRecordPrimaryKey = null;
        }

        @Override
        public boolean hasNext() {
            return getAndStageNextRecord() != null;
        }

        /**
         * Get the next record, or NULL if remaining records are skipped.
         */
        @Override
        public ConsumerRecord<K, V> next() {

            // obtain the next valid (non-skipped) record
            ConsumerRecord<byte[], byte[]> record = getAndStageNextRecord();
            if (record == null) {
                throw new NoSuchElementException();
            }

            K key = topic.getKeySerde().deserializer().deserialize(record.topic(), record.key());
            V value = topic.getValueSerde().deserializer().deserialize(record.topic(), record.value());

            // update state
            switch (this.nextRecordFilterMode) {
                case SKIP:
                    // By design, this should never be the case
                    throw new IllegalStateException("Staged record has unexpected filter mode of SKIP");
                case DELETE:
                    topic.getState().delete(topic.getShortName() + "-" + DATA, this.nextRecordPrimaryKey.getBytes());
                    value = null;
                    break;
                case UPDATE:
                default:
                    topic.getState().put(topic.getShortName() + "-" + DATA, this.nextRecordPrimaryKey.getBytes(), record.value());
                    break;
            }

            // mark the record as consumed from the staging area and return
            this.resetStagedRecord();
            return new ConsumerRecord<>(
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                record.timestampType(),
                0L,
                record.serializedKeySize(),
                record.serializedValueSize(),
                key,
                value,
                record.headers()
            );
        }
    }

    /**
     * Callback to be used when issuing Kafka producer writes. This callback will be used to keep track of async write
     * successes and failures. On a record write failure the {@link #callbackException} variable will store the failed
     * records exception. Async exceptions should be checked for by calling {@link #checkCallbackExceptions()}
     */
    private class KafkaProducerCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null && callbackException == null) {
                callbackException = e;
            }
            inflightRecords.decrementAndGet();
        }
    }

    /**
     * An exception returned back from an async Kafka producer callback
     */
    private volatile Exception callbackException;
    /**
     * The Kafka consumer this abstraction wraps around
     */
    protected KafkaConsumer<byte[], byte[]> consumer;
    /**
     * The last read offset by partition using the read next method.
     */
    private Map<Integer, Long> currentOffsets = new HashMap<>();
    /**
     * The end offset for this topic. Cached for performance reasons
     */
    private Map<Integer, Long> endOffsets;
    /**
     * Stop watch used to determine when to refresh the end offset
     */
    private StopWatch endOffsetWatch;
    /**
     * A count of all currently in flight async writes to Kafka
     */
    private AtomicLong inflightRecords = new AtomicLong();
    /**
     * Number of partitions in the topic
     */
    private Integer numPartitions;
    /**
     * The timeout for each poll call to Kafka
     */
    private long pollTimeout;
    /**
     * Producer for writing data back to the topic
     */
    private KafkaProducer<K, V> producer = null;
    /**
     * The callback for Kafka producer writes
     */
    private final Callback producerCallback = new KafkaProducerCallback();

    @Override
    public void commit() {
        commitData();
        for(Map.Entry<Integer, Long> offset: currentOffsets.entrySet()) {
            if(offset.getValue() != null) {
                this.getState().put(
                    offsetKeyspaceName,
                    Ints.toByteArray(offset.getKey()),
                    Longs.toByteArray(offset.getValue()));
            }
        }
        this.getState().flush(offsetKeyspaceName);
    }

    protected void commitData() {
        this.getState().flush(dataKeyspaceName);
    }

    @Override
    public void configure(TopicConfig<K, V> topicConfig) {
        super.configure(topicConfig);

        Map<String, Object> spConfig = topicConfig.southpawConfig;

        // Make a consumer
        if(!ObjectUtils.equals(spConfig.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), false)) {
            logger.warn("Southpaw does not use Kafka's offset management. Enabling auto commit does nothing, except maybe incur some overhead.");
        }
        if(!ObjectUtils.equals(spConfig.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest")) {
            logger.warn("Since Southpaw handles its own offsets, the auto offset reset config is ignored. If there are no existing offsets, we will always start at the beginning.");
        }
        consumer = new KafkaConsumer<>(spConfig, Serdes.ByteArray().deserializer(), Serdes.ByteArray().deserializer());
        // Subscribe is lazy and requires a poll() call, which we don't want to require, so we do this instead
        Set<TopicPartition> partitionsToAssign = consumer.partitionsFor(topicName).stream()
            .map(partitionInfo -> new TopicPartition(topicName, partitionInfo.partition()))
            .collect(Collectors.toSet());
        numPartitions = partitionsToAssign.size();
        consumer.assign(partitionsToAssign);
        for(TopicPartition partition: partitionsToAssign) {
            byte[] bytes = this.getState().get(offsetKeyspaceName, Ints.toByteArray(partition.partition()));
            if (bytes == null) {
                consumer.seekToBeginning(Collections.singleton(partition));
                logger.info(String.format("No offsets found for topic %s and partition %s, seeking to beginning.", this.getShortName(), partition.partition()));
            } else {
                Long offset = Longs.fromByteArray(bytes);
                currentOffsets.put(partition.partition(), offset);
                consumer.seek(new TopicPartition(topicName, partition.partition()), offset);
                logger.info(String.format("Topic %s and partition %s starting with offset %s.", this.getShortName(), partition.partition(), offset));
            }
        }
        endOffsetWatch = new StopWatch();
        endOffsetWatch.start();
        pollTimeout = ((Number) spConfig.getOrDefault(POLL_TIMEOUT_CONFIG, POLL_TIMEOUT_DEFAULT)).longValue();

        // Check producer config
        if(!ObjectUtils.equals(spConfig.get(ProducerConfig.ACKS_CONFIG), "all")) {
            logger.warn("It is recommended to set ACKS to 'all' otherwise data loss can occur");
        }
    }

    @Override
    public void flush() {
        if(producer != null) {
            producer.flush();
        }

        long count = inflightRecords.get();
        if(count != 0) {
            throw new RuntimeException("Could not successfully flush " + count + " records");
        }

        checkCallbackExceptions();
    }

    @Override
    public Map<Integer, Long> getCurrentOffsets() {
        return Collections.unmodifiableMap(currentOffsets);
    }

    @Override
    public long getLag() {
        // Periodically cache the end offset
        if (endOffsets == null || endOffsetWatch.getTime() > END_OFFSET_REFRESH_MS_DEFAULT) {
            Set<TopicPartition> partitionsToCheck = IntStream.range(0, numPartitions)
                .mapToObj(partition -> new TopicPartition(topicName, partition))
                .collect(Collectors.toSet());
            Map<TopicPartition, Long> offsets = consumer.endOffsets(partitionsToCheck);
            endOffsets = offsets.entrySet().stream().collect(
                Collectors.toMap(entry -> entry.getKey().partition(), Map.Entry::getValue));
            endOffsetWatch.reset();
            endOffsetWatch.start();
        }
        long lag = 0;
        for(Map.Entry<Integer, Long> entry: endOffsets.entrySet()) {
            // Because the end offset is only updated periodically, it's possible to see negative lag. Send 0 instead.
            long currentOffset = currentOffsets.get(entry.getKey()) == null ? 0: currentOffsets.get(entry.getKey());
            long endOffset = entry.getValue() == null ? 0: entry.getValue();
            long partitionLag = endOffset - currentOffset < 0 ? 0: endOffset - currentOffset;
            lag += partitionLag;
        }
        return lag;
    }

    @Override
    public V readByPK(ByteArray primaryKey) {
        byte[] bytes;
        if(primaryKey == null) {
            return null;
        } else {
            bytes = this.getState().get(dataKeyspaceName, primaryKey.getBytes());
        }
        return this.getValueSerde().deserializer().deserialize(topicName, bytes);
    }

    @Override
    public Iterator<ConsumerRecord<K, V>> readNext() {
        return new KafkaTopicIterator<>(consumer.poll(pollTimeout).iterator(), this);
    }

    @Override
    public void resetCurrentOffsets() {
        logger.info(String.format("Resetting offsets for topic %s, seeking to beginning.", this.getShortName()));
        for(PartitionInfo info: consumer.partitionsFor(this.topicName)) {
            this.getState().delete(offsetKeyspaceName, Ints.toByteArray(info.partition()));
            consumer.seekToBeginning(ImmutableList.of(new TopicPartition(topicName, 0)));
        }
        currentOffsets = new HashMap<>();
    }

    /**
     * Method so the iterator returned by readNext() can set the current offset of this topic.
     * @param partition - The partition to set the offset for
     * @param offset - The new current offset
     */
    private void setCurrentOffset(int partition, long offset) {
        currentOffsets.put(partition, offset);
    }

    @Override
    public void write(K key, V value) {
        checkCallbackExceptions();

        if(producer == null) producer = new KafkaProducer<>(topicConfig.southpawConfig,
                this.getKeySerde().serializer(), this.getValueSerde().serializer());

        inflightRecords.incrementAndGet();

        producer.send(new ProducerRecord<>(topicName, key, value), producerCallback);
    }

    private void checkCallbackExceptions() throws RuntimeException {
        Exception ex = callbackException;
        if (callbackException != null) {
            callbackException = null;

            throw new RuntimeException("Failed to write record to " + this.getShortName() + " topic: " + ex.getMessage(), ex);
        }
    }
}
