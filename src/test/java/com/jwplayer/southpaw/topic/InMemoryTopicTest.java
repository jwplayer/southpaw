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

import com.jwplayer.southpaw.state.RocksDBState;
import com.jwplayer.southpaw.state.RocksDBStateTest;
import com.jwplayer.southpaw.util.ByteArray;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.*;


public class InMemoryTopicTest {
    private final String[] keys = {"A", "B", "C"};
    private final String[] values = {"Badger", "Mushroom", "Snake"};
    private RocksDBState state;

    @Before
    public void setup() {
        Map<String, Object> config = RocksDBStateTest.createConfig("file:///tmp/RocksDB/InMemoryTopicTest");
        state = new RocksDBState();
        state.configure(config);
    }

    @After
    public void cleanup() {
        state.delete();
    }

    public InMemoryTopic<String, String> createTopic() {
        InMemoryTopic<String, String> topic = new InMemoryTopic<>();
        Map<String, Object> config = new HashMap<>();
        topic.configure("TestTopic", config, state, Serdes.String(), Serdes.String());
        for(int i = 0; i < keys.length; i++) {
            topic.write(keys[i], values[i]);
        }
        return topic;
    }

    @Test
    public void testReadNext() {
        InMemoryTopic<String, String> topic = createTopic();
        Iterator<ConsumerRecord<String, String>> records = topic.readNext();
        int i = 0;
        while(records.hasNext()) {
            ConsumerRecord<String, String> record = records.next();
            validateRecord(record, keys[i], values[i], (long) i + topic.firstOffset);
            i++;
        }
        assertEquals(3, i);
    }

    @Test
    public void testReadByPK() {
        BaseTopic<String, String> topic = createTopic();
        String value = topic.readByPK(new ByteArray("B"));

        assertEquals("Mushroom", value);
    }

    public void validateRecord(ConsumerRecord<String, String> record, String key, String value, Long offset) {
        assertEquals(key, record.key());
        assertEquals(value, record.value());
        assertEquals(offset, (Long) record.offset());
    }
}
