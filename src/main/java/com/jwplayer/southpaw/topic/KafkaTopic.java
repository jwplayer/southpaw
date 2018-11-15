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
import com.jwplayer.southpaw.filter.BaseFilter;
import com.jwplayer.southpaw.filter.BaseFilter.FilterMode;
import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.state.BaseState;
import com.jwplayer.southpaw.util.ByteArray;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


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
    private static final Logger logger = Logger.getLogger(KafkaTopic.class);

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
                topic.setCurrentOffset(record.offset() + 1L);

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
                    filterMode = topic.filter.filter(topic.getShortName(), (BaseRecord) value, oldRec);
                } else {
                    filterMode = FilterMode.UPDATE;
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
                    topic.state.delete(topic.shortName + "-" + DATA, this.nextRecordPrimaryKey.getBytes());
                    break;
                case UPDATE:
                default:
                    topic.state.put(topic.shortName + "-" + DATA, this.nextRecordPrimaryKey.getBytes(), record.value());
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
     * The timeout for each poll call to Kafka
     */
    private long pollTimeout;
    /**
     * Producer for writing data back to the topic
     */
    private KafkaProducer<K, V> producer = null;
    /**
     * List container future objects from the producer. Allows us to batch writes and check that records are
     * properly sent to Kafka when flush() is called.
     */
    private List<Future<RecordMetadata>> producerFutures = new ArrayList<>();

    @Override
    public void commit() {
        commitData();
        if(currentOffset != null) {
            state.put(shortName + "-" + OFFSETS, Ints.toByteArray(0), Longs.toByteArray(currentOffset));
        }
        state.flush(shortName + "-" + OFFSETS);
    }

    protected void commitData() {
        state.flush(shortName + "-" + DATA);
    }

    @Override
    public void configure(
            String shortName,
            Map<String, Object> config,
            BaseState state,
            Serde<K> keySerde,
            Serde<V> valueSerde,
            BaseFilter filter
    ) {
        super.configure(shortName, config, state, keySerde, valueSerde, filter);
        // Make a consumer
        if(!ObjectUtils.equals(config.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), false)) {
            logger.warn("Southpaw does not use Kafka's offset management. Enabling auto commit does nothing, except maybe incur some overhead.");
        }
        if(!ObjectUtils.equals(config.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest")) {
            logger.warn("Since Southpaw handles its own offsets, the auto offset reset config is ignored. If there are no existing offsets, we will always start at the beginning.");
        }
        consumer = new KafkaConsumer<>(config, Serdes.ByteArray().deserializer(), Serdes.ByteArray().deserializer());
        if(consumer.partitionsFor(topicName).size() > 1) {
            throw new RuntimeException(String.format("Topic '%s' has more than one partition. Southpaw currently only supports topics with a single partition.", topicName));
        }
        // Subscribe is lazy and requires a poll() call, which we don't want to require, so we do this instead
        consumer.assign(Collections.singleton(new TopicPartition(topicName, 0)));
        byte[] bytes = state.get(shortName + "-" + OFFSETS, Ints.toByteArray(0));
        if(bytes == null) {
            consumer.seekToBeginning(Collections.singleton(new TopicPartition(topicName, 0)));
            logger.info(String.format("No offsets found for topic %s, seeking to beginning.", shortName));
        } else {
            currentOffset = Longs.fromByteArray(bytes);
            consumer.seek(new TopicPartition(topicName, 0), currentOffset);
            logger.info(String.format("Topic %s starting with offset %s.", shortName, currentOffset));
        }
        endOffsetWatch = new StopWatch();
        endOffsetWatch.start();
        pollTimeout = ((Number) config.getOrDefault(POLL_TIMEOUT_CONFIG, POLL_TIMEOUT_DEFAULT)).longValue();

        // Check producer config
        if(!ObjectUtils.equals(config.get(ProducerConfig.ACKS_CONFIG), "all")) {
            logger.warn("It is recommended to set ACKS to 'all' otherwise data loss can occur");
        }
    }

    @Override
    public void flush() {
        boolean isSuccessful = true;
        if(producer != null) {
            producer.flush();
        }
        for(Future<RecordMetadata> future: producerFutures) {
            try {
                future.get();
            } catch(ExecutionException | InterruptedException ex) {
                logger.error(ex);
                isSuccessful = false;
            }
        }
        if(!isSuccessful) {
            throw new RuntimeException("Could not successfully flush all remaining record writes to the topic");
        }
        producerFutures.clear();
    }

    @Override
    public Long getCurrentOffset() {
        return currentOffset;
    }

    @Override
    public long getLag() {
        // Periodically cache the end offset
        if(endOffset == null || endOffsetWatch.getTime() > END_OFFSET_REFRESH_MS_DEFAULT) {
            Map<TopicPartition, Long> offsets = consumer.endOffsets(Collections.singletonList(new TopicPartition(topicName, 0)));
            endOffset = offsets.get(new TopicPartition(topicName, 0));
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
            bytes = state.get(shortName + "-" + DATA, primaryKey.getBytes());
        }
        return valueSerde.deserializer().deserialize(topicName, bytes);
    }

    @Override
    public Iterator<ConsumerRecord<K, V>> readNext() {
        return new KafkaTopicIterator<>(consumer.poll(pollTimeout).iterator(), this);
    }

    @Override
    public void resetCurrentOffset() {
        logger.info(String.format("Resetting offsets for topic %s, seeking to beginning.", shortName));
        state.delete(shortName + "-" + OFFSETS, Ints.toByteArray(0));
        consumer.seekToBeginning(ImmutableList.of(new TopicPartition(topicName, 0)));
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
        if(producer == null) producer = new KafkaProducer<>(config, keySerde.serializer(), valueSerde.serializer());
        producerFutures.add(producer.send(new ProducerRecord<>(topicName, 0, key, value)));
    }
}
