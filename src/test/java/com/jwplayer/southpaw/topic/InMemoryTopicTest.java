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

import com.google.common.collect.Lists;
import com.jwplayer.southpaw.state.InMemoryState;
import com.jwplayer.southpaw.filter.BaseFilter;
import com.jwplayer.southpaw.state.BaseState;
import com.jwplayer.southpaw.util.ByteArray;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.*;


public class InMemoryTopicTest {
    private final List<String> keys = Lists.newArrayList("A", "B", "C", "A");
    private final List<String> values = Lists.newArrayList("Badger", "Mushroom", "Snake", "Surprise!");
    private BaseState state;

    @Before
    public void setup() {
        state = new InMemoryState();
        state.open();
    }

    @After
    public void cleanup() {
        state.delete();
    }

    public InMemoryTopic<String, String> createTopic() {
        InMemoryTopic<String, String> topic = new InMemoryTopic<>();
        Map<String, Object> config = new HashMap<>();
        topic.configure(new TopicConfig<String, String>()
            .setShortName("TestTopic")
            .setSouthpawConfig(config)
            .setState(state)
            .setKeySerde(Serdes.String())
            .setValueSerde(Serdes.String())
            .setFilter(new BaseFilter()));

        for(int i = 0; i < keys.size(); i++) {
            topic.write(keys.get(i), values.get(i));
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
            assertEquals(keys.get(i), record.key());
            assertEquals(values.get(i), record.value());
            i++;
        }
        assertEquals(0, topic.getLag());
        assertEquals(keys.size(), i);
    }

    @Test
    public void testReadByPK() {
        BaseTopic<String, String> topic = createTopic();
        Iterator<ConsumerRecord<String, String>> records = topic.readNext();
        while(records.hasNext()) {
            records.next();
        }

        assertEquals("Surprise!", topic.readByPK(new ByteArray("A")));
        assertEquals("Mushroom", topic.readByPK(new ByteArray("B")));
        assertEquals("Snake", topic.readByPK(new ByteArray("C")));
    }
}
