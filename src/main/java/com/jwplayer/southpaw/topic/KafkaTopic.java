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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.jwplayer.southpaw.filter.BaseFilter.FilterMode;
import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.util.ByteArray;


/**
 * Kafka implementation of the topic abstraction.
 * @param <K> - The type of the record key
 * @param <V> - the type of the record value
 */
public class KafkaTopic<K, V> extends BaseTopic<K, V> {
    public static final long END_OFFSET_REFRESH_MS_DEFAULT = 60000;

    public static final String PERSISTENT = "persistent";
    public static final boolean PERSISTENT_DEFAULT = true;
    public static final String TABLE_NAME = "table.name";
    /**
     * Le Logger
     */
    private static final Logger logger = LoggerFactory.getLogger(KafkaTopic.class);

    /**
     * This allows us to capture the record and update our state when next() is called.
     */
    private static class KafkaTopicIterator<K, V> implements ConsumerRecordIterator<K, V> {

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
        private V nextValue;
        private int approximateCount;

        /**
         * Constructor
         * @param iter - The iterator to wrap
         * @param topic - The topic whose offsets we'll update
         */
        private KafkaTopicIterator(ConsumerRecords<byte[], byte[]> consumerRecords, KafkaTopic<K, V> topic) {
            this.iter = consumerRecords.iterator();
            this.topic = topic;
            this.approximateCount = consumerRecords.count();
        }

        @Override
        public int getApproximateCount() {
            return approximateCount;
        }

        @Override
        public V peekValue() {
            return nextValue;
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
            FilterMode filterMode = FilterMode.SKIP;
            ByteArray primaryKey = null;
            K key;
            V value = null;

            // Obtain a record and stage it
            while(iter.hasNext() && filterMode == FilterMode.SKIP) {
                record = iter.next();
                topic.setCurrentOffset(record.offset());

                key = topic.getKeySerde().deserializer().deserialize(record.topic(), record.key());
                value = topic.getValueSerde().deserializer().deserialize(record.topic(), record.value());

                if (key instanceof BaseRecord) {
                    primaryKey = ((BaseRecord) key).toByteArray();
                } else {
                    primaryKey = new ByteArray(record.key());
                }

                // Non-BaseRecord value types will not be filtered
                if (value instanceof BaseRecord) {
                    final ByteArray pk = primaryKey;
                    filterMode = topic.getFilter().filter(topic.topicConfig.shortName, (BaseRecord) value, ()->{
                        BaseRecord oldRec = null;
                        V currState = topic.readByPK(pk);
                        if (currState instanceof BaseRecord) {
                            oldRec = (BaseRecord) currState;
                        }
                        return oldRec;
                    });
                } else if (value != null){ //skip tombstones
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
                this.nextValue = value;
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
            this.nextValue = null;
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
            V value = nextValue;
            // update state
            switch (this.nextRecordFilterMode) {
                case SKIP:
                    // By design, this should never be the case
                    throw new IllegalStateException("Staged record has unexpected filter mode of SKIP");
                case DELETE:
                    if (topic.persistent) {
                        topic.getState().delete(topic.getShortName() + "-" + DATA, this.nextRecordPrimaryKey.getBytes());
                    }
                    value = null;
                    break;
                case UPDATE:
                default:
                    //TODO: for debezium this could be the serialization of the value, not the whole envelope if we don't need txn processing
                    if (topic.persistent) {
                        topic.getState().put(topic.getShortName() + "-" + DATA, this.nextRecordPrimaryKey.getBytes(), record.value());
                    }
                    break;
            }

            // mark the record as consumed from the staging area and return
            //The current offset is one ahead of the last read one.
            //This copies what Kafka would return as the current offset.
            topic.setCurrentOffset(record.offset() + 1L);
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
    private KafkaConsumer<byte[], byte[]> consumer;
    /**
     * The last read offset using the read next method.
     */
    private Long currentOffset;
    /**
     * The end offset for this topic. Cached for performance reasons
     */
    private Long endOffset;
    /**
     * Stop watch used to determine when to refresh the end offset
     */
    private StopWatch endOffsetWatch;
    /**
     * A count of all currently in flight async writes to Kafka
     */
    private AtomicLong inflightRecords = new AtomicLong();
    /**
     * The timeout for each poll call to Kafka
     */
    private long pollTimeout = 0;
    /**
     * Producer for writing data back to the topic
     */
    private KafkaProducer<K, V> producer = null;
    /**
     * The callback for Kafka producer writes
     */
    private final Callback producerCallback = new KafkaProducerCallback();
    /**
     * If the values for this topic should be saved
     */
    private boolean persistent;
    private TopicPartition topicPartition;
    /**
     * The table name associated with this topic if transactional
     */
    private String tableName;

    @Override
    public void commit() {
        commitData();
        if(currentOffset != null) {
            this.getState().put(this.getShortName() + "-" + OFFSETS, Ints.toByteArray(0), Longs.toByteArray(currentOffset));
        }
        this.getState().flush(this.getShortName() + "-" + OFFSETS);
    }

    protected void commitData() {
        this.getState().flush(this.getShortName() + "-" + DATA);
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
        if(consumer.partitionsFor(topicName).size() > 1) {
            throw new RuntimeException(String.format("Topic '%s' has more than one partition. Southpaw currently only supports topics with a single partition.", topicName));
        }
        topicPartition = new TopicPartition(topicName, 0);
        // Subscribe is lazy and requires a poll() call, which we don't want to require, so we do this instead
        consumer.assign(Collections.singleton(topicPartition));
        byte[] bytes = this.getState().get(this.getShortName() + "-" + OFFSETS, Ints.toByteArray(0));
        if(bytes == null) {
            consumer.seekToBeginning(Collections.singleton(topicPartition));
            logger.info(String.format("No offsets found for topic %s, seeking to beginning.", this.getShortName()));
        } else {
            currentOffset = Longs.fromByteArray(bytes);
            consumer.seek(topicPartition, currentOffset);
            logger.info(String.format("Topic %s starting with offset %s.", this.getShortName(), currentOffset));
        }
        endOffsetWatch = new StopWatch();
        endOffsetWatch.start();
        //we don't want to have the main processing thread wait on individual topics
        //pollTimeout = ((Number) spConfig.getOrDefault(POLL_TIMEOUT_CONFIG, POLL_TIMEOUT_DEFAULT)).longValue();

        // Check producer config
        if(!ObjectUtils.equals(spConfig.get(ProducerConfig.ACKS_CONFIG), "all")) {
            logger.warn("It is recommended to set ACKS to 'all' otherwise data loss can occur");
        }

        this.persistent = (Boolean)spConfig.getOrDefault(PERSISTENT, PERSISTENT_DEFAULT);
        this.tableName = (String)spConfig.getOrDefault(TABLE_NAME, null);
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
    public Long getCurrentOffset() {
        return currentOffset;
    }

    @Override
    public long getLag() {
        // Periodically cache the end offset
        if(endOffset == null || endOffsetWatch.getTime() > END_OFFSET_REFRESH_MS_DEFAULT) {
            Map<TopicPartition, Long> offsets = consumer.endOffsets(Collections.singletonList(topicPartition));
            endOffset = offsets.get(topicPartition);
            endOffsetWatch.reset();
            endOffsetWatch.start();
        }
        // Because the end offset is only updated periodically, it's possible to see negative lag. Send 0 instead.
        long lag = endOffset - (getCurrentOffset() == null ? 0 : getCurrentOffset());
        return lag < 0 ? 0 : lag;
    }

    @Override
    public V readByPK(ByteArray primaryKey) {
        byte[] bytes;
        if(primaryKey == null) {
            return null;
        } else {
            bytes = this.getState().get(this.getShortName() + "-" + DATA, primaryKey.getBytes());
        }
        return this.getValueSerde().deserializer().deserialize(topicName, bytes);
    }

    @Override
    public ConsumerRecordIterator<K, V> readNext() {
        return new KafkaTopicIterator<>(consumer.poll(pollTimeout), this);
    }

    @Override
    public void resetCurrentOffset() {
        logger.info(String.format("Resetting offsets for topic %s, seeking to beginning.", this.getShortName()));
        this.getState().delete(this.getShortName() + "-" + OFFSETS, Ints.toByteArray(0));
        consumer.seekToBeginning(ImmutableList.of(topicPartition));
        currentOffset = null;
    }

    /**
     * Method so the iterator returned by readNext() can set the current offset of this topic.
     * @param offset - The new current offset
     */
    private void setCurrentOffset(long offset) {
        currentOffset = offset;
    }

    @Override
    public void write(K key, V value) {
        checkCallbackExceptions();

        if(producer == null) producer = new KafkaProducer<>(topicConfig.southpawConfig,
                this.getKeySerde().serializer(), this.getValueSerde().serializer());

        inflightRecords.incrementAndGet();

        producer.send(new ProducerRecord<>(topicName, 0, key, value), producerCallback);
    }

    private void checkCallbackExceptions() throws RuntimeException {
        Exception ex = callbackException;
        if (callbackException != null) {
            callbackException = null;

            throw new RuntimeException("Failed to write record to " + this.getShortName() + " topic: " + ex.getMessage(), ex);
        }
    }

    public void setPollTimeout(long pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }
}
