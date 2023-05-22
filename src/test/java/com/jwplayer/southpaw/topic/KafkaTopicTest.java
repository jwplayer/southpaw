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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.jwplayer.southpaw.state.InMemoryState;
import com.jwplayer.southpaw.filter.BaseFilter;
import com.jwplayer.southpaw.state.BaseState;
import com.jwplayer.southpaw.util.ByteArray;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.*;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.utility.DockerImageName;

import java.util.*;

import static org.junit.Assert.*;


public class KafkaTopicTest {


    @ClassRule
    public static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.1.1"));
    private static BaseState state;
    private static AdminClient adminClient;
    public KafkaTopic<String, String> createTopic(String topicName) {
        return createTopic(topicName, 3);
    }

    public KafkaTopic<String, String> createTopic(String topicName, int partitions) {
        createKafkaTopic(topicName, partitions);
        KafkaTopic<String, String> topic = new KafkaTopic<>();
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        config.put(KafkaTopic.TOPIC_NAME_CONFIG, topicName);
        topic.configure(new TopicConfig<String, String>()
            .setShortName(topicName)
            .setSouthpawConfig(config)
            .setState(state)
            .setKeySerde(Serdes.String())
            .setValueSerde(Serdes.String())
            .setFilter(new BaseFilter()));

        topic.write("A", "7");
        topic.write("B", "8");
        topic.write("C", "9");
        topic.write("D", "10");
        topic.write("E", "11");
        topic.write("F", "12");
        topic.write("A", "1");
        topic.write("B", "2");
        topic.write("C", "3");
        topic.write("D", "4");
        topic.write("E", "5");
        topic.write("F", "6");
        topic.flush();
        return topic;
    }

    @BeforeClass
    public static void setup() {
        state = new InMemoryState();
        state.open();

        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        adminClient = AdminClient.create(properties);
    }

    @AfterClass
    public static void cleanup() {
        state.delete();
        adminClient.close();
    }

    @Test
    public void testCommit() {
        String dataKeyspace = "testCommit-data";
        String offsetsKeyspace = "testCommit-offsets";
        KafkaTopic<String, String> topic = createTopic("testCommit");
        Iterator<ConsumerRecord<String, String>> records = topic.readNext();
        while(records.hasNext()) {
            records.next();
        }
        topic.commit();
        // Test data in state
        assertArrayEquals("1".getBytes(), state.get(dataKeyspace, "A".getBytes()));
        assertArrayEquals("2".getBytes(), state.get(dataKeyspace, "B".getBytes()));
        assertArrayEquals("3".getBytes(), state.get(dataKeyspace, "C".getBytes()));
        assertArrayEquals("4".getBytes(), state.get(dataKeyspace, "D".getBytes()));
        assertArrayEquals("5".getBytes(), state.get(dataKeyspace, "E".getBytes()));
        assertArrayEquals("6".getBytes(), state.get(dataKeyspace, "F".getBytes()));
        // Test offsets in state
        assertArrayEquals(Longs.toByteArray(2L), state.get(offsetsKeyspace, Ints.toByteArray(0)));
        assertArrayEquals(Longs.toByteArray(8L), state.get(offsetsKeyspace, Ints.toByteArray(1)));
        assertArrayEquals(Longs.toByteArray(2L), state.get(offsetsKeyspace, Ints.toByteArray(2)));
    }

    @Test
    public void testGetCurrentOffsetAfterRead() {
        KafkaTopic<String, String> topic = createTopic("testGetCurrentOffsetAfterRead");
        topic.resetCurrentOffsets();
        Iterator<ConsumerRecord<String, String>> records = topic.readNext();
        while(records.hasNext()) {
            records.next();
        }
        topic.commit();
        Map<Integer, Long> expectedOffsets = new HashMap<>();
        expectedOffsets.put(0, 2L);
        expectedOffsets.put(1, 8L);
        expectedOffsets.put(2, 2L);
        assertEquals(expectedOffsets, topic.getCurrentOffsets());
    }

    @Test
    public void testGetCurrentOffsetBeforeRead() {
        KafkaTopic<String, String> topic = createTopic("testGetCurrentOffsetBeforeRead");
        assertEquals(Collections.emptyMap(), topic.getCurrentOffsets());
    }

    @Test
    public void testGetLag() {
        KafkaTopic<String, String> topic = createTopic("testGetLag");
        assertEquals(12L, topic.getLag());
        Iterator<ConsumerRecord<String, String>> iter = topic.readNext();
        while(iter.hasNext()) iter.next();
        assertEquals(0L, topic.getLag());
    }

    @Test
    public void testGetPreexistingOffsetsMultiPartition() {
        String keyspace = "testGetPreexistingOffsetsMultiPartition-offsets";
        String topicName = "testGetPreexistingOffsetsMultiPartition";
        state.createKeySpace(keyspace);
        state.put(keyspace, Ints.toByteArray(0), Longs.toByteArray(3));
        state.put(keyspace, Ints.toByteArray(1), Longs.toByteArray(20));
        KafkaTopic<String, String> topic = createTopic(topicName, 3);
        Map<Integer, Long> expectedOffsets = new HashMap<>();
        expectedOffsets.put(0, 3L);
        expectedOffsets.put(1, 20L);
        assertEquals(expectedOffsets, topic.getCurrentOffsets());
        assertEquals(3L, topic.consumer.position(new TopicPartition(topicName, 0)));
        assertEquals(20L, topic.consumer.position(new TopicPartition(topicName, 1)));
    }

    @Test
    public void testGetPreexistingOffsetsSinglePartition() {
        String keyspace = "testGetPreexistingOffsetsSinglePartition-offsets";
        String topicName = "testGetPreexistingOffsetsSinglePartition";
        state.createKeySpace(keyspace);
        state.put(keyspace, Ints.toByteArray(0), Longs.toByteArray(2));
        KafkaTopic<String, String> topic = createTopic(topicName, 1);
        Map<Integer, Long> expectedOffsets = new HashMap<>();
        expectedOffsets.put(0, 2L);
        assertEquals(expectedOffsets, topic.getCurrentOffsets());
        assertEquals(2L, topic.consumer.position(new TopicPartition(topicName, 0)));
    }

    @Test
    public void testGetTopicName() {
        KafkaTopic<String, String> topic = createTopic("testGetTopicName");
        assertEquals("testGetTopicName", topic.getTopicName());
    }
    
    @Test
    public void testReadByPK() {
        KafkaTopic<String, String> topic = createTopic("testReadByPK");
        // Read the records so they get cached in the state
        Iterator<ConsumerRecord<String, String>> iter = topic.readNext();
        while(iter.hasNext()) {
            iter.next();
        }
        // Read by primary key
        String value = topic.readByPK(new ByteArray("D"));
        assertNotNull(value);
        assertEquals("4", value);

        // Read a 'bad' primary key
        value = topic.readByPK(new ByteArray("Z"));
        assertNull(value);
    }

    @Test
    public void testReadNext() {
        KafkaTopic<String, String> topic = createTopic("testReadNext");
        topic.resetCurrentOffsets();
        Iterator<ConsumerRecord<String, String>> records = topic.readNext();
        assertTrue(records.hasNext());
        Map<String, String> actual = new HashMap<>();
        while(records.hasNext()) {
            ConsumerRecord<String, String> record = records.next();
            actual.put(record.key(), record.value());
        }
        Map<String, String> expected = new HashMap<>();
        expected.put("A", "1");
        expected.put("B", "2");
        expected.put("C", "3");
        expected.put("D", "4");
        expected.put("E", "5");
        expected.put("F", "6");
        assertEquals(expected, actual);
    }

    @Test
    public void testToString() {
        KafkaTopic<String, String> topic = createTopic("testToString");
        assertNotNull(topic.toString());
    }

    private void createKafkaTopic(String topicName, int partitions) {
        NewTopic newTopic = new NewTopic(topicName, partitions,  (short) 1);
        try {
            final CreateTopicsResult result = adminClient.createTopics(ImmutableList.of(newTopic));
            result.all().get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create topic: " + topicName, e);
        }

    }
}
