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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.jwplayer.southpaw.MockState;
import com.jwplayer.southpaw.filter.BaseFilter;
import com.jwplayer.southpaw.state.BaseState;
import com.jwplayer.southpaw.util.ByteArray;
import com.jwplayer.southpaw.util.KafkaTestServer;


public class KafkaTopicTest {
    private static final String TEST_TOPIC = "test-topic";

    private KafkaTestServer kafkaServer;
    private BaseState state;
    private KafkaTopic<String, String> topic;

    public KafkaTopic<String, String> createTopic(String topicName) {
        kafkaServer.createTopic(topicName, 1);
        KafkaTopic<String, String> topic = new KafkaTopic<>();
        topic.setPollTimeout(100);
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer.getConnectionString());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        config.put(KafkaTopic.TOPIC_NAME_CONFIG, topicName);
        topic.configure(new TopicConfig<String, String>()
            .setShortName("test")
            .setSouthpawConfig(config)
            .setState(state)
            .setKeySerde(Serdes.String())
            .setValueSerde(Serdes.String())
            .setFilter(new BaseFilter()));

        topic.write("A", "1");
        topic.write("B", "2");
        topic.write("C", "3");
        topic.flush();
        return topic;
    }

    @Before
    public void setup() {
        state = new MockState();
        state.open();
        kafkaServer = new KafkaTestServer();
        topic = createTopic(TEST_TOPIC);
    }

    @After
    public void cleanup() {
        kafkaServer.shutdown();
        state.delete();
    }

    @Test
    public void testGetCurrentOffsetAfterRead() {
        topic.resetCurrentOffset();
        Iterator<ConsumerRecord<String, String>> records = topic.readNext();
        long count = 0;
        while(records.hasNext()) {
            records.next();
            count++;
        }
        topic.commit();
        assertEquals(count, (long) topic.getCurrentOffset());
    }

    @Test
    public void testGetCurrentOffsetBeforeRead() {
        KafkaTopic<String, String> topic = createTopic("test-topic-before-read");
        assertNull(topic.getCurrentOffset());
    }

    @Test
    public void testGetLag() {
        KafkaTopic<String, String> topic = createTopic("test-topic-get-lag");
        assertEquals(3L, topic.getLag());
        Iterator<ConsumerRecord<String, String>> iter = topic.readNext();
        while(iter.hasNext()) iter.next();
        assertEquals(0L, topic.getLag());
    }

    @Test
    public void testGetTopicName() {
        KafkaTopic<String, String> topic = createTopic("test-topic");
        assertEquals("test-topic", topic.getTopicName());
    }

    @Test
    public void testReadByPK() {
        // Read the records so they get cached in the state
        Iterator<ConsumerRecord<String, String>> iter = topic.readNext();
        while(iter.hasNext()) {
            iter.next();
        }
        // Read by primary key
        String value = topic.readByPK(new ByteArray("B"));
        assertNotNull(value);
        assertEquals("2", value);

        // Read a 'bad' primary key
        value = topic.readByPK(new ByteArray("D"));
        assertNull(value);
    }

    @Test
    public void testReadNext() {
        topic.resetCurrentOffset();
        Iterator<ConsumerRecord<String, String>> records = topic.readNext();
        assertTrue(records.hasNext());
        ConsumerRecord<String, String> record = records.next();
        assertNotNull(record);
        assertEquals("A", record.key());
        assertEquals("1", record.value());
        assertTrue(records.hasNext());
        record = records.next();
        assertNotNull(record);
        assertEquals("B", record.key());
        assertEquals("2", record.value());
        assertTrue(records.hasNext());
        record = records.next();
        assertNotNull(record);
        assertEquals("C", record.key());
        assertEquals("3", record.value());
    }

    @Test
    public void testToString() {
        KafkaTopic<String, String> topic = createTopic("test-topic");
        assertNotNull(topic.toString());
    }
}
