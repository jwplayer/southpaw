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
package com.jwplayer.southpaw.index;

import com.jwplayer.southpaw.MockState;
import com.jwplayer.southpaw.filter.BaseFilter;
import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.serde.JsonSerde;
import com.jwplayer.southpaw.state.BaseState;
import com.jwplayer.southpaw.topic.BaseTopic;
import com.jwplayer.southpaw.topic.InMemoryTopic;
import com.jwplayer.southpaw.topic.TopicConfig;
import com.jwplayer.southpaw.util.ByteArray;
import com.jwplayer.southpaw.util.FileHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.*;

import java.net.URI;
import java.util.*;

import static org.junit.Assert.*;


public class MultiIndexTest {
    private MultiIndex<BaseRecord, BaseRecord> index;
    private BaseState state;

    @Before
    public void setup() {
        state = new MockState();
        index = createEmptyIndex(state);
    }

    @After
    public void cleanup() {
        state.delete();
    }

    private MultiIndex<BaseRecord, BaseRecord> createEmptyIndex(BaseState state) {
        Map<String, Object> config = new HashMap<>();
        config.put(MultiIndex.INDEX_LRU_CACHE_SIZE, 2);
        config.put(MultiIndex.INDEX_WRITE_BATCH_SIZE, 5);
        JsonSerde keySerde = new JsonSerde();
        keySerde.configure(config, true);
        JsonSerde valueSerde = new JsonSerde();
        valueSerde.configure(config, true);
        state.open(config);
        BaseTopic<BaseRecord, BaseRecord> indexedTopic = new InMemoryTopic<>(0);
        indexedTopic.configure(new TopicConfig<BaseRecord, BaseRecord>()
            .setShortName("IndexedTopic")
            .setSouthpawConfig(config)
            .setState(state)
            .setKeySerde(keySerde)
            .setValueSerde(valueSerde)
            .setFilter(new BaseFilter()));
        MultiIndex<BaseRecord, BaseRecord> index = new MultiIndex<>();
        index.configure("TestIndex", config, state, indexedTopic);
        return index;
    }

    private MultiIndex<BaseRecord, BaseRecord> createMultiIndex() throws Exception {
        String TOPIC_DATA_PATH = "test-resources/topic/slim_entity.json";
        populateTopic(index.getIndexedTopic(), new URI(TOPIC_DATA_PATH));
        Iterator<ConsumerRecord<BaseRecord, BaseRecord>> records = index.getIndexedTopic().readNext();
        while(records.hasNext()) {
            ConsumerRecord<BaseRecord, BaseRecord> record = records.next();
            index.add(ByteArray.toByteArray(record.value().get("JoinKey")), record.key().toByteArray());
        }
        index.getIndexedTopic().resetCurrentOffset();
        return index;
    }

    private void populateTopic(BaseTopic<BaseRecord, BaseRecord> topic, URI uri)
            throws Exception {
        String[] json = FileHelper.loadFileAsString(uri).split("\n");
        for(int i = 0; i < json.length / 2; i++) {
            topic.write(
                    topic.getKeySerde().deserializer().deserialize(null, json[2 * i].getBytes()),
                    topic.getValueSerde().deserializer().deserialize(null, json[2 * i + 1].getBytes())
            );
        }
    }

    @Test
    public void testEmptyIndexAdd() {
        index.add(new ByteArray("A"), new ByteArray(0L));
        Set<ByteArray> primaryKeys = index.getIndexEntry(new ByteArray("A"));

        assertNotNull(primaryKeys);
        assertEquals(1, primaryKeys.size());
        assertTrue(primaryKeys.contains(new ByteArray(0L)));
        Set<ByteArray> foreignKeys = index.getForeignKeys(new ByteArray(0L));
        assertNotNull(foreignKeys);
        assertEquals(1, foreignKeys.size());
        assertTrue(foreignKeys.contains(new ByteArray("A")));
    }

    @Test
    public void testEmptyIndexGetOffsets() {
        Set<ByteArray> primaryKeys = index.getIndexEntry(new ByteArray("A"));

        assertNull(primaryKeys);
    }

    @Test
    public void testEmptyIndexReadRecords() {
        Iterator<AbstractMap.SimpleEntry<ByteArray, BaseRecord>> records = index.readRecords(new ByteArray("A"));

        assertNotNull(records);
        assertFalse(records.hasNext());
    }

    @Test
    public void testEmptyIndexRemove() {
        Set<ByteArray> primaryKeys = index.remove(new ByteArray("A"));

        assertNull(primaryKeys);
    }

    @Test
    public void testEmptyIndexRemoveOffset() {
        boolean isRemoved = index.remove(new ByteArray("A"), new ByteArray(0L));

        assertFalse(isRemoved);
        assertNull(index.getForeignKeys(new ByteArray(0L)));
    }

    @Test
    public void testEmptyIndexToString() {
        String string = index.toString();
        assertNotNull(string);
    }

    @Test
    public void testLoad() {
        index.add(new ByteArray("A"), new ByteArray(0L));
        index.add(new ByteArray("B"), new ByteArray(1L));
        index.add(new ByteArray("B"), new ByteArray(2L));
        index.add(new ByteArray("B"), new ByteArray(3L));
        index.add(new ByteArray("C"), new ByteArray(4L));
        index.add(new ByteArray("C"), new ByteArray(5L));
        index.flush();

        Set<ByteArray> primaryKeys = index.getIndexEntry(new ByteArray("A"));
        assertNotNull(primaryKeys);
        assertEquals(1, primaryKeys.size());
        assertTrue(primaryKeys.contains(new ByteArray(0L)));
        Set<ByteArray> keys = index.getForeignKeys(new ByteArray(0L));
        assertNotNull(keys);
        assertEquals(1, keys.size());
        assertEquals(new ByteArray("A"), keys.toArray(new ByteArray[1])[0]);
        primaryKeys = index.getIndexEntry(new ByteArray("B"));
        assertNotNull(primaryKeys);
        assertEquals(3, primaryKeys.size());
        assertTrue(primaryKeys.contains(new ByteArray(1L)));
        keys = index.getForeignKeys(new ByteArray(1L));
        assertNotNull(keys);
        assertEquals(1, keys.size());
        assertEquals(new ByteArray("B"), keys.toArray(new ByteArray[1])[0]);
        assertTrue(primaryKeys.contains(new ByteArray(2L)));
        keys = index.getForeignKeys(new ByteArray(2L));
        assertNotNull(keys);
        assertEquals(1, keys.size());
        assertEquals(new ByteArray("B"), keys.toArray(new ByteArray[1])[0]);
        assertTrue(primaryKeys.contains(new ByteArray(3L)));
        keys = index.getForeignKeys(new ByteArray(3L));
        assertNotNull(keys);
        assertEquals(1, keys.size());
        assertEquals(new ByteArray("B"), keys.toArray(new ByteArray[1])[0]);
        primaryKeys = index.getIndexEntry(new ByteArray("C"));
        assertNotNull(primaryKeys);
        assertEquals(2, primaryKeys.size());
        assertTrue(primaryKeys.contains(new ByteArray(4L)));
        keys = index.getForeignKeys(new ByteArray(4L));
        assertNotNull(keys);
        assertEquals(1, keys.size());
        assertEquals(new ByteArray("C"), keys.toArray(new ByteArray[1])[0]);
        assertTrue(primaryKeys.contains(new ByteArray(5L)));
        keys = index.getForeignKeys(new ByteArray(5L));
        assertNotNull(keys);
        assertEquals(1, keys.size());
        assertEquals(new ByteArray("C"), keys.toArray(new ByteArray[1])[0]);
    }

    @Test
    public void testMultiIndexAdd() throws Exception {
        MultiIndex<BaseRecord, BaseRecord> index = createMultiIndex();
        index.add(new ByteArray("B"), new ByteArray(9L));
        Set<ByteArray> primaryKeys = index.getIndexEntry(new ByteArray("B"));

        assertNotNull(primaryKeys);
        assertEquals(2, primaryKeys.size());
        assertTrue(primaryKeys.contains(new ByteArray(9L)));
        Set<ByteArray> keys = index.getForeignKeys(new ByteArray(9L));
        assertNotNull(keys);
        assertEquals(1, keys.size());
        assertEquals(new ByteArray("B"), keys.toArray(new ByteArray[1])[0]);
    }

    @Test
    public void testMultiIndexGetIndexEntry() throws Exception {
        MultiIndex<BaseRecord, BaseRecord> index = createMultiIndex();
        Set<ByteArray> primaryKeys = index.getIndexEntry(new ByteArray("A"));

        assertNotNull(primaryKeys);
        assertEquals(3, primaryKeys.size());
        assertTrue(primaryKeys.contains(new ByteArray(1)));
        Set<ByteArray> foreignKeys = index.getForeignKeys(new ByteArray(1));
        assertNotNull(foreignKeys);
        assertEquals(2, foreignKeys.size());
        assertEquals(new ByteArray("A"), foreignKeys.toArray(new ByteArray[2])[0]);
        assertTrue(foreignKeys.contains(new ByteArray("A")));
        foreignKeys = index.getForeignKeys(new ByteArray(2));
        assertNotNull(foreignKeys);
        assertEquals(2, foreignKeys.size());
        assertEquals(new ByteArray("A"), foreignKeys.toArray(new ByteArray[2])[0]);
        assertTrue(foreignKeys.contains(new ByteArray("A")));
        foreignKeys = index.getForeignKeys(new ByteArray(3));
        assertNotNull(foreignKeys);
        assertEquals(1, foreignKeys.size());
        assertEquals(new ByteArray("A"), foreignKeys.toArray(new ByteArray[1])[0]);
    }

    @Test
    public void testMultiIndexReadRecords() throws Exception {
        MultiIndex<BaseRecord, BaseRecord> index = createMultiIndex();
        Iterator<AbstractMap.SimpleEntry<ByteArray, BaseRecord>> iter = index.readRecords(new ByteArray("C"));
        Map<ByteArray, BaseRecord> records = new HashMap<>();

        while(iter.hasNext()) {
            AbstractMap.SimpleEntry<ByteArray, BaseRecord> pair = iter.next();
            records.put(pair.getKey(), pair.getValue());
        }

        assertEquals(3, records.size());
        assertTrue(records.containsKey(new ByteArray(1)));
        assertEquals("Jamie", records.get(new ByteArray(1)).get("Field3"));
        assertTrue(records.containsKey(new ByteArray(2)));
        assertEquals("Tywin", records.get(new ByteArray(2)).get("Field3"));
        assertTrue(records.containsKey(new ByteArray(5)));
        assertEquals("Cersei", records.get(new ByteArray(5)).get("Field3"));
    }

    @Test
    public void testMultiIndexRemove() throws Exception {
        MultiIndex<BaseRecord, BaseRecord> index = createMultiIndex();
        Set<ByteArray> primaryKeys = index.remove(new ByteArray("B"));

        assertNotNull(primaryKeys);
        assertEquals(1, primaryKeys.size());
        assertTrue(primaryKeys.contains(new ByteArray(4)));
        assertNull(index.getForeignKeys(new ByteArray(4)));
    }

    @Test
    public void testMultiIndexRemove2() throws Exception {
        MultiIndex<BaseRecord, BaseRecord> index = createMultiIndex();

        assertFalse(index.remove(new ByteArray("A"), new ByteArray(9)));
        assertTrue(index.remove(new ByteArray("A"), new ByteArray(1)));
        Set<ByteArray> primaryKeys = index.getIndexEntry(new ByteArray("A"));
        assertNotNull(primaryKeys);
        assertEquals(2L, primaryKeys.size());
        Set<ByteArray> foreignKeys = index.getForeignKeys(new ByteArray(1));
        assertNotNull(foreignKeys);
        assertEquals(1, foreignKeys.size());
        assertEquals(new ByteArray("C"), foreignKeys.toArray(new ByteArray[1])[0]);
        assertNull(index.getForeignKeys(new ByteArray(9)));
    }

    @Test
    public void testMultiIndexToString() throws Exception {
        MultiIndex<BaseRecord, BaseRecord> index = createMultiIndex();
        String string = index.toString();
        assertNotNull(string);
    }
}
