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
package com.jwplayer.southpaw;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jwplayer.southpaw.index.BaseIndex;
import com.jwplayer.southpaw.json.DenormalizedRecord;
import com.jwplayer.southpaw.json.Record;
import com.jwplayer.southpaw.json.Relation;
import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.record.MapRecord;
import com.jwplayer.southpaw.state.RocksDBState;
import com.jwplayer.southpaw.topic.BaseTopic;
import com.jwplayer.southpaw.util.ByteArray;
import com.jwplayer.southpaw.util.ByteArraySet;
import com.jwplayer.southpaw.util.FileHelper;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.yaml.snakeyaml.Yaml;

import java.net.URI;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class SouthpawTest {
    private static final String BROKEN_RELATIONS_PATH = "test-resources/broken_relations.sample.json";
    private static final String CONFIG_PATH = "test-resources/config.sample.yaml";
    private static final String RELATIONS_PATH = "test-resources/relations3.sample.json";
    private static final String TOPIC_DATA_PATH = "test-resources/topic/";

    private URI brokenRelationsUri;
    private Map<String, Object> config;
    private MockSouthpaw southpaw;
    private URI relationsUri;

    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        brokenRelationsUri = new URI(BROKEN_RELATIONS_PATH);
        Yaml yaml = new Yaml();
        config = yaml.load(FileHelper.getInputStream(new URI(CONFIG_PATH)));
        // Override the db and backup URI's with the managed temp folder
        config.put(RocksDBState.URI_CONFIG, dbFolder.getRoot().toURI().toString() + "/db");
        config.put(RocksDBState.BACKUP_URI_CONFIG, dbFolder.getRoot().toURI().toString() + "/backup");

        relationsUri = new URI(RELATIONS_PATH);
        southpaw = new MockSouthpaw(config, Collections.singletonList(relationsUri));
        Southpaw.deleteBackups(config);
    }

    @After
    public void cleanup() {
        southpaw.close();
        Southpaw.deleteBackups(config);
        Southpaw.deleteState(config);
    }

    @Test
    public void testCreateInternalRecord() {
        Map<String, Comparable> map = new HashMap<>();
        map.put("A", 1);
        map.put("B", false);
        map.put("C", "Badger");
        BaseRecord mapRecord = new MapRecord(map);
        Record internalRecord = southpaw.createInternalRecord(mapRecord);
        Map<String, Object> internalMap = internalRecord.getAdditionalProperties();

        for(Map.Entry<String, Comparable> entry: map.entrySet()) {
            assertTrue(internalMap.containsKey(entry.getKey()));
            assertEquals(entry.getValue(), internalMap.get(entry.getKey()));
        }
    }

    @Test
    public void testCreateKafkaConfig() {
        Map<String, Object> kafkaConfig = southpaw.createTopicConfig("Overrides");

        assertEquals(kafkaConfig.get("bootstrap.servers"), "kafka:29092");
        assertEquals(kafkaConfig.get("client.id"), "overrides");
        assertEquals(kafkaConfig.get("group.id"), "southpaw");
        assertEquals(kafkaConfig.get("key.serde.class"), "com.jwplayer.southpaw.serde.AvroSerde");
        assertEquals(kafkaConfig.get("schema.registry.url"), "http://schema-registry:8081");
        assertEquals(kafkaConfig.get("value.serde.class"), "com.jwplayer.southpaw.serde.AvroSerde");
        assertEquals(kafkaConfig.get("topic.name"), "overrides");
    }

    @Test
    public void testFkIndices() {
        Map<String, BaseIndex<BaseRecord, BaseRecord, ByteArraySet>> indices = southpaw.getFkIndices();

        assertEquals(10, indices.size());
        // Join Key Indices
        assertTrue(indices.containsKey("JK|media|id"));
        assertTrue(indices.containsKey("JK|playlist_custom_params|playlist_id"));
        assertTrue(indices.containsKey("JK|playlist_media|playlist_id"));
        assertTrue(indices.containsKey("JK|playlist_tag|playlist_id"));
        assertTrue(indices.containsKey("JK|user|user_id"));
        assertTrue(indices.containsKey("JK|user_tag|id"));

        // Parent Key Indices
        assertTrue(indices.containsKey("PaK|playlist|playlist_media|media_id"));
        assertTrue(indices.containsKey("PaK|playlist|playlist|id"));
        assertTrue(indices.containsKey("PaK|playlist|playlist|user_id"));
        assertTrue(indices.containsKey("PaK|playlist|playlist_tag|user_tag_id"));
    }

    @Test
    public void testGetRelationChild() throws Exception {
        Relation relation = MockSouthpaw.loadRelations(Collections.singletonList(relationsUri))[0];
        AbstractMap.SimpleEntry<Relation, Relation> foundRelation = southpaw.getRelation(relation, "media");

        // Parent
        assertEquals("playlist_media", foundRelation.getKey().getEntity());
        assertEquals("playlist_id", foundRelation.getKey().getJoinKey());
        assertEquals("id", foundRelation.getKey().getParentKey());
        assertEquals(1, foundRelation.getKey().getChildren().size());
        // Child
        assertEquals("media", foundRelation.getValue().getEntity());
        assertEquals("id", foundRelation.getValue().getJoinKey());
        assertEquals("media_id", foundRelation.getValue().getParentKey());
        assertEquals(0, foundRelation.getValue().getChildren().size());
    }

    @Test
    public void testGetRelationMissing() throws Exception {
        Relation relation = MockSouthpaw.loadRelations(Collections.singletonList(relationsUri))[0];
        AbstractMap.SimpleEntry<Relation, Relation> foundRelation = southpaw.getRelation(relation, "your mom");

        assertNull(foundRelation);
    }

    @Test
    public void testGetRelationRoot() throws Exception {
        Relation relation = MockSouthpaw.loadRelations(Collections.singletonList(relationsUri))[0];
        AbstractMap.SimpleEntry<Relation, Relation> foundRelation = southpaw.getRelation(relation, relation.getEntity());

        assertNull(foundRelation.getKey());
        assertEquals(relation, foundRelation.getValue());
    }

    @Test
    public void testNormalizedTopics() {
        Map<String, BaseTopic<BaseRecord, BaseRecord>> topics = southpaw.getNormalizedTopics();

        assertEquals(7, topics.size());
        assertTrue(topics.containsKey("media"));
        assertTrue(topics.containsKey("playlist"));
        assertTrue(topics.containsKey("playlist_custom_params"));
        assertTrue(topics.containsKey("playlist_media"));
        assertTrue(topics.containsKey("playlist_tag"));
        assertTrue(topics.containsKey("user"));
        assertTrue(topics.containsKey("user_tag"));
    }

    @Test(expected = NullPointerException.class)
    public void testBrokenRelations() throws Exception {
        new MockSouthpaw(config, Collections.singletonList(brokenRelationsUri));
    }

    @Test
    public void testLoadRelations() throws Exception {
        Relation[] relations = MockSouthpaw.loadRelations(Collections.singletonList(relationsUri));

        assertEquals(1, relations.length);
        assertEquals("DenormalizedPlaylist", relations[0].getDenormalizedName());
        assertEquals("playlist", relations[0].getEntity());
        assertEquals(4, relations[0].getChildren().size());
    }

    @Test
    public void testCreateDenormalizedRecordsConsistency() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Map<ByteArray, DenormalizedRecord> expectedResults = new HashMap<>(12);
        expectedResults.put(ByteArray.toByteArray(2234), mapper.readValue("{\"Record\":{\"user_id\":1234,\"id\":2234,\"title\":\"big buck bunny\",\"status\":\"ready\"},\"Children\":{\"user\":[{\"Record\":{\"user_id\":1234,\"usage_type\":\"monthly\",\"user_name\":\"Suzy\",\"email\":\"Suzy+something@jwplayer.com\"},\"Children\":{}}],\"playlist_media\":[{\"Record\":{\"pos\":0,\"playlist_id\":4235,\"media_id\":2234,\"id\":6234},\"Children\":{\"playlist\":[{\"Record\":{\"user_id\":1234,\"active\":1,\"id\":4235,\"type\":\"feed\",\"title\":\"Titled Similar Playlist\"},\"Children\":{}}]}}]}}", DenormalizedRecord.class));
        expectedResults.put(ByteArray.toByteArray(2235), mapper.readValue("{\"Record\":{\"user_id\":1234,\"id\":2235,\"title\":\"something.mov\",\"status\":\"ready\"},\"Children\":{\"user\":[{\"Record\":{\"user_id\":1234,\"usage_type\":\"monthly\",\"user_name\":\"Suzy\",\"email\":\"Suzy+something@jwplayer.com\"},\"Children\":{}}],\"playlist_media\":[{\"Record\":{\"pos\":1,\"playlist_id\":4235,\"media_id\":2235,\"id\":6235},\"Children\":{\"playlist\":[{\"Record\":{\"user_id\":1234,\"active\":1,\"id\":4235,\"type\":\"feed\",\"title\":\"Titled Similar Playlist\"},\"Children\":{}}]}}]}}", DenormalizedRecord.class));
        expectedResults.put(ByteArray.toByteArray(2236), mapper.readValue("{\"Record\":{\"user_id\":1234,\"id\":2236,\"title\":\"something_else.mp4\",\"status\":\"ready\"},\"Children\":{\"user\":[{\"Record\":{\"user_id\":1234,\"usage_type\":\"monthly\",\"user_name\":\"Suzy\",\"email\":\"Suzy+something@jwplayer.com\"},\"Children\":{}}],\"playlist_media\":[{\"Record\":{\"pos\":2,\"playlist_id\":4235,\"media_id\":2236,\"id\":6236},\"Children\":{\"playlist\":[{\"Record\":{\"user_id\":1234,\"active\":1,\"id\":4235,\"type\":\"feed\",\"title\":\"Titled Similar Playlist\"},\"Children\":{}}]}}]}}", DenormalizedRecord.class));
        expectedResults.put(ByteArray.toByteArray(2237), mapper.readValue("{\"Record\":{\"user_id\":1235,\"id\":2237,\"title\":\"Title Change\",\"status\":\"ready\"},\"Children\":{\"user\":[{\"Record\":{\"user_id\":1235,\"usage_type\":\"unlimited\",\"user_name\":\"TROGDOR\",\"email\":\"TROGDOR@jwplayer.com\"},\"Children\":{}}],\"playlist_media\":[{\"Record\":{\"pos\":0,\"playlist_id\":4236,\"media_id\":2237,\"id\":6237},\"Children\":{\"playlist\":[{\"Record\":{\"user_id\":1235,\"active\":1,\"id\":4236,\"type\":\"trending\",\"title\":\"Test Trending Playlist\"},\"Children\":{}}]}},{\"Record\":{\"pos\":2,\"playlist_id\":4236,\"media_id\":2237,\"id\":6239},\"Children\":{\"playlist\":[{\"Record\":{\"user_id\":1235,\"active\":1,\"id\":4236,\"type\":\"trending\",\"title\":\"Test Trending Playlist\"},\"Children\":{}}]}}]}}", DenormalizedRecord.class));
        expectedResults.put(ByteArray.toByteArray(2238), null);

        int maxRecords = 0;
        Map<String, BaseTopic<BaseRecord, BaseRecord>> normalizedTopics = southpaw.getNormalizedTopics();
        Map<String, String[]> records = new HashMap<>();
        List<Object[]> retVal = new ArrayList<>();
        for(Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>> entry: normalizedTopics.entrySet()) {
            records.put(entry.getKey(), FileHelper.loadFileAsString(new URI(TOPIC_DATA_PATH + entry.getKey() + ".json")).split("\n"));
            maxRecords = Math.max(records.get(entry.getKey()).length, maxRecords);
        }

        for(int i = 0; i < maxRecords / 2; i++) {
            for(Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>> entry: normalizedTopics.entrySet()) {
                String[] json = records.get(entry.getKey());
                if(json.length >= i * 2 + 2) {
                    entry.getValue().write(
                            entry.getValue().getKeySerde().deserializer().deserialize(null, json[2 * i].getBytes()),
                            entry.getValue().getValueSerde().deserializer().deserialize(null, json[2 * i + 1].getBytes())
                    );
                }
            }
        }

        southpaw.run(1);
        southpaw.commit();
        southpaw.close();

        southpaw = spy(new MockSouthpaw(config, Collections.singletonList(relationsUri)));
        doCallRealMethod()
                .doThrow(RuntimeException.class)
                .when(southpaw)
                .createDenormalizedRecord(any(), any(), any(), any());

        maxRecords = 0;
        normalizedTopics = southpaw.getNormalizedTopics();
        records = new HashMap<>();
        for(Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>> entry: normalizedTopics.entrySet()) {
            records.put(entry.getKey(), FileHelper.loadFileAsString(new URI(TOPIC_DATA_PATH + entry.getKey() + ".json")).split("\n"));
            maxRecords = Math.max(records.get(entry.getKey()).length, maxRecords);
        }

        for(int i = 0; i < maxRecords / 2; i++) {
            for(Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>> entry: normalizedTopics.entrySet()) {
                String[] json = records.get(entry.getKey());
                if(json.length >= i * 2 + 2) {
                    entry.getValue().write(
                            entry.getValue().getKeySerde().deserializer().deserialize(null, json[2 * i].getBytes()),
                            entry.getValue().getValueSerde().deserializer().deserialize(null, json[2 * i + 1].getBytes())
                    );
                }
            }
        }

        BaseTopic<BaseRecord, BaseRecord> playlistTopic = normalizedTopics.get("playlist");

        String newRecordKey = "{\"id\":4235}";
        String newRecord = "{\"id\":4235,\"active\":1,\"user_id\":1236,\"type\":\"feed\",\"title\":\"another day, another record\"}";
        playlistTopic.write(
                playlistTopic.getKeySerde().deserializer().deserialize(null, newRecordKey.getBytes()),
                playlistTopic.getValueSerde().deserializer().deserialize(null, newRecord.getBytes())
        );

        RuntimeException expected = null;
        try {
            southpaw.run(1);
        } catch (RuntimeException ex) {
            expected = ex;
        }

        assertNotNull(expected);

        southpaw.close();

        southpaw = new MockSouthpaw(config, Collections.singletonList(relationsUri));

        maxRecords = 0;
        normalizedTopics = southpaw.getNormalizedTopics();
        records = new HashMap<>();
        for(Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>> entry: normalizedTopics.entrySet()) {
            records.put(entry.getKey(), FileHelper.loadFileAsString(new URI(TOPIC_DATA_PATH + entry.getKey() + ".json")).split("\n"));
            maxRecords = Math.max(records.get(entry.getKey()).length, maxRecords);
        }

        for(int i = 0; i < maxRecords / 2; i++) {
            for(Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>> entry: normalizedTopics.entrySet()) {
                String[] json = records.get(entry.getKey());
                if(json.length >= i * 2 + 2) {
                    entry.getValue().write(
                            entry.getValue().getKeySerde().deserializer().deserialize(null, json[2 * i].getBytes()),
                            entry.getValue().getValueSerde().deserializer().deserialize(null, json[2 * i + 1].getBytes())
                    );
                }
            }
        }

        playlistTopic = normalizedTopics.get("playlist");

        playlistTopic.write(
                playlistTopic.getKeySerde().deserializer().deserialize(null, newRecordKey.getBytes()),
                playlistTopic.getValueSerde().deserializer().deserialize(null, newRecord.getBytes())
        );

        southpaw.run(1);

        Map<String, Map<ByteArray, DenormalizedRecord>> denormalizedRecords = new HashMap<>();
        for(Map.Entry<String, BaseTopic<byte[], DenormalizedRecord>> entry: southpaw.outputTopics.entrySet()) {
            if(!denormalizedRecords.containsKey(entry.getKey())) {
                denormalizedRecords.put(entry.getKey(), new HashMap<>());
            }
            entry.getValue().resetCurrentOffset();
            Iterator<ConsumerRecord<byte[], DenormalizedRecord>> iter = entry.getValue().readNext();
            while(iter.hasNext()) {
                ConsumerRecord<byte[], DenormalizedRecord> record = iter.next();
                denormalizedRecords.get(entry.getKey()).put(new ByteArray(record.key()), record.value());
            }
        }

        for(Map.Entry<String, Map<ByteArray, DenormalizedRecord>> entry: denormalizedRecords.entrySet()) {
            for(Map.Entry<ByteArray, DenormalizedRecord> innerEntry: entry.getValue().entrySet()) {
                retVal.add(new Object[] {
                        String.format("Denormalized Entity: %s / Primary Key: %s", entry.getKey(), Hex.encodeHexString(innerEntry.getKey().getBytes())),
                        innerEntry.getValue(),
                        expectedResults.get(innerEntry.getKey())
                });
            }
        }

        System.out.println(denormalizedRecords.values());

        assertEquals(5, retVal.size());
    }
}
