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

import com.jwplayer.southpaw.json.DenormalizedRecord;
import com.jwplayer.southpaw.json.Relation;
import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.state.RocksDBState;
import com.jwplayer.southpaw.topic.BaseTopic;
import com.jwplayer.southpaw.util.ByteArray;
import com.jwplayer.southpaw.util.FileHelper;
import com.jwplayer.southpaw.util.Pair;
import com.jwplayer.southpaw.util.RelationHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.yaml.snakeyaml.Yaml;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;


public class SouthpawEndToEndTest {
    private Map<String, Object> config;
    Set<String> denormalizedNames;
    private Southpaw southpaw;

    @Rule
    public final TemporaryFolder dbFolder = new TemporaryFolder();

    @Rule
    public final TemporaryFolder backupFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        Yaml yaml = new Yaml();
        config = yaml.load(FileHelper.getInputStream(new URI(TestHelper.CONFIG_PATH)));
        config.put(RocksDBState.URI_CONFIG, dbFolder.getRoot().toURI().toString());
        config.put(RocksDBState.BACKUP_URI_CONFIG, backupFolder.getRoot().toURI().toString());
        southpaw = new Southpaw(
                config,
                Arrays.asList(
                        new URI(TestHelper.RELATION_PATHS.PLAYLIST),
                        new URI(TestHelper.RELATION_PATHS.PLAYER),
                        new URI(TestHelper.RELATION_PATHS.MEDIA)));
        Southpaw.deleteBackups(config);
        denormalizedNames = Arrays.stream(southpaw.relations)
                .map(Relation::getDenormalizedName).collect(Collectors.toSet());
    }

    @After
    public void cleanup() {
        southpaw.close();
        Southpaw.deleteBackups(config);
        Southpaw.deleteState(config);
    }

    public Map<String, Map<ByteArray, DenormalizedRecord>> getExpectedResults() throws Exception {
        Map<String, Map<ByteArray, DenormalizedRecord>> expectedResults = new HashMap<>();
        List<Pair<ByteArray, DenormalizedRecord>> playlistResults
                = TestHelper.readDenormalizedData(TestHelper.TOPIC_DATA_PATHS.DENORMALIZED_PLAYLIST);
        expectedResults.put("DenormalizedPlaylist",
                playlistResults.stream().collect(HashMap::new, (m, p) -> m.put(p.getA(), p.getB()), HashMap::putAll));
        List<Pair<ByteArray, DenormalizedRecord>> mediaResults
                = TestHelper.readDenormalizedData(TestHelper.TOPIC_DATA_PATHS.DENORMALIZED_MEDIA);
        expectedResults.put("DenormalizedMedia",
                mediaResults.stream().collect(HashMap::new, (m, p) -> m.put(p.getA(), p.getB()), HashMap::putAll));
        List<Pair<ByteArray, DenormalizedRecord>> playerResults
                = TestHelper.readDenormalizedData(TestHelper.TOPIC_DATA_PATHS.DENORMALIZED_PLAYER);
        expectedResults.put("DenormalizedPlayer",
                playerResults.stream().collect(HashMap::new, (m, p) -> m.put(p.getA(), p.getB()), HashMap::putAll));
        return expectedResults;
    }

    public Map<String, Iterator<Pair<BaseRecord, BaseRecord>>> getTopicsData() throws Exception {
        Map<String, String> topicDataPaths = new HashMap<>();
        topicDataPaths.put("media", TestHelper.TOPIC_DATA_PATHS.MEDIA);
        topicDataPaths.put("player", TestHelper.TOPIC_DATA_PATHS.PLAYER);
        topicDataPaths.put("playlist", TestHelper.TOPIC_DATA_PATHS.PLAYLIST);
        topicDataPaths.put("playlist_custom_params", TestHelper.TOPIC_DATA_PATHS.PLAYLIST_CUSTOM_PARAMS);
        topicDataPaths.put("playlist_media", TestHelper.TOPIC_DATA_PATHS.PLAYLIST_MEDIA);
        topicDataPaths.put("playlist_tag", TestHelper.TOPIC_DATA_PATHS.PLAYLIST_TAG);
        topicDataPaths.put("user", TestHelper.TOPIC_DATA_PATHS.USER);
        topicDataPaths.put("user_tag", TestHelper.TOPIC_DATA_PATHS.USER_TAG);
        Map<String, Iterator<Pair<BaseRecord, BaseRecord>>> topicsData = new HashMap<>();
        for(Map.Entry<String, String> entry: topicDataPaths.entrySet()) {
            List<Pair<BaseRecord, BaseRecord>> topicData = TestHelper.readRecordData(entry.getValue());
            topicsData.put(entry.getKey(), topicData.iterator());
        }
        return topicsData;
    }

    public Map<String, Map<ByteArray, DenormalizedRecord>> readDenormalizedRecords() {
        Map<String, Map<ByteArray, DenormalizedRecord>> denormalizedRecords = new HashMap<>();
        for(String denormalizedName: denormalizedNames) {
            BaseTopic<byte[], DenormalizedRecord> topic = southpaw.topics.getOutputTopic(denormalizedName);
            denormalizedRecords.put(denormalizedName, new HashMap<>());
            topic.resetCurrentOffsets();
            Iterator<ConsumerRecord<byte[], DenormalizedRecord>> iter = topic.readNext();
            while(iter.hasNext()) {
                ConsumerRecord<byte[], DenormalizedRecord> record = iter.next();
                denormalizedRecords.get(denormalizedName).put(new ByteArray(record.key()), record.value());
            }
        }
        return denormalizedRecords;
    }

    public void runSouthpaw(Map<String, Iterator<Pair<BaseRecord, BaseRecord>>> topicsData) {
        Set<String> entities = RelationHelper.getEntities(southpaw.relations);
        while(topicsData.values().stream().anyMatch(Iterator::hasNext)) {
            for(String entity: entities) {
                BaseTopic<BaseRecord, BaseRecord> topic = southpaw.topics.getInputTopic(entity);
                Iterator<Pair<BaseRecord, BaseRecord>> topicData = topicsData.get(entity);
                if(topicData.hasNext()) {
                    Pair<BaseRecord, BaseRecord> datum = topicData.next();
                    topic.write(datum.getA(), datum.getB());
                }
            }
            southpaw.run(1L);
        }
    }

    @Test
    public void testRecord() throws Exception {
        Map<String, Map<ByteArray, DenormalizedRecord>> expectedResults = getExpectedResults();
        Map<String, Iterator<Pair<BaseRecord, BaseRecord>>> topicsData = getTopicsData();
        runSouthpaw(topicsData);
        Map<String, Map<ByteArray, DenormalizedRecord>> denormalizedRecords = readDenormalizedRecords();

        for(String denormalizedName: denormalizedNames) {
            Map<ByteArray, DenormalizedRecord> actualResults = denormalizedRecords.get(denormalizedName);
            Map<ByteArray, DenormalizedRecord> expected = expectedResults.get(denormalizedName);
            assertEquals(
                    String.format("Denormalized Name: %s Expected: %s Actual: %s", denormalizedName, expected, actualResults),
                    expected.size(), actualResults.size());
            for(Map.Entry<ByteArray, DenormalizedRecord> actualResult: actualResults.entrySet()) {
                assertEquals(
                        String.format("Denormalized Name: %s Primary Key: %s", denormalizedName, actualResult.getKey()),
                        expected.get(actualResult.getKey()), actualResult.getValue());
            }
        }
        assertTrue(southpaw.indices.verifyIndices());
    }
}
