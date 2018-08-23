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

        /**
         * Constructor
         * @param iter - The iterator to wrap
         * @param topic - The topic whose offsets we'll update
         */
        private KafkaTopicIterator(Iterator<ConsumerRecord<byte[], byte[]>> iter, KafkaTopic<K, V> topic) {
            this.iter = iter;
            this.topic = topic;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public ConsumerRecord<K, V> next() {
            ConsumerRecord<byte[], byte[]> record = iter.next();
            ConsumerRecord<K, V> retVal =
                    new ConsumerRecord<>(
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.timestamp(),
                            record.timestampType(),
                            0L,
                            record.serializedKeySize(),
                            record.serializedValueSize(),
                            topic.getKeySerde().deserializer().deserialize(record.topic(), record.key()),
                            topic.getValueSerde().deserializer().deserialize(record.topic(), record.value()),
                            record.headers()
                    );
            ByteArray primaryKey;
            if(retVal.key() instanceof BaseRecord) {
                primaryKey = ((BaseRecord) retVal.key()).toByteArray();
            } else {
                primaryKey = new ByteArray(record.key());
            }
            if(record.value() == null) {
                topic.state.delete(topic.shortName + "-" + DATA, primaryKey.getBytes());
            } else {
                topic.state.put(topic.shortName + "-" + DATA, primaryKey.getBytes(), record.value());
            }
            // The current offset is one ahead of the last read one. This copies what Kafka would return as the
            // current offset.
            topic.setCurrentOffset(record.offset() + 1L);
            return retVal;
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
            Serde<V> valueSerde
    ) {
        super.configure(shortName, config, state, keySerde, valueSerde);
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
