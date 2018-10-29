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
import com.jwplayer.southpaw.json.DenormalizedRecord;
import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.topic.BaseTopic;
import com.jwplayer.southpaw.util.ByteArray;
import com.jwplayer.southpaw.util.FileHelper;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.yaml.snakeyaml.Yaml;

import java.net.URI;
import java.util.*;

import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class SouthpawEndToEndTest {
    private static final String CONFIG_PATH = "test-resources/config.sample.yaml";
    private static final String RELATIONS_PATH = "test-resources/relations.sample.json";
    private static final String RELATIONS_PATH2 = "test-resources/relations2.sample.json";
    private static final String RELATIONS_PATH3 = "test-resources/relations3.sample.json";
    private static final String TOPIC_DATA_PATH = "test-resources/topic/";

    private String testName;
    private DenormalizedRecord actualRecord;
    private DenormalizedRecord expectedRecord;

    public SouthpawEndToEndTest(
            String testName,
            DenormalizedRecord actualRecord,
            DenormalizedRecord expectedRecord
    ) {
        this.testName = testName;
        this.actualRecord = actualRecord;
        this.expectedRecord = expectedRecord;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> getTestCases() throws Exception {
        // Build the expected results
        ObjectMapper mapper = new ObjectMapper();
        Map<ByteArray, DenormalizedRecord> expectedResults = new HashMap<>(12);
        // Expected Feed monthly
        expectedResults.put(ByteArray.toByteArray(4235), mapper.readValue("{\"Record\":{\"user_id\":1234,\"active\":1,\"id\":4235,\"type\":\"feed\",\"title\":\"Titled Similar Playlist\"},\"Children\":{\"user\":[{\"Record\":{\"user_id\":1234,\"usage_type\":\"monthly\",\"user_name\":\"Suzy\",\"email\":\"Suzy+something@jwplayer.com\"},\"Children\":{}}],\"playlist_custom_params\":[{\"Record\":{\"playlist_id\":4235,\"name\":\"param1\",\"id\":5234,\"value\":\"val1\"},\"Children\":{}}],\"playlist_tag\":[{\"Record\":{\"playlist_id\":4235,\"user_tag_id\":7234,\"tag_type\":\"nada\"},\"Children\":{\"user_tag\":[{\"Record\":{\"user_id\":1234,\"tag_name\":\"b\",\"id\":7234},\"Children\":{}}]}},{\"Record\":{\"playlist_id\":4235,\"user_tag_id\":7235,\"tag_type\":\"inc\"},\"Children\":{\"user_tag\":[{\"Record\":{\"user_id\":1234,\"tag_name\":\"b\",\"id\":7235},\"Children\":{}}]}},{\"Record\":{\"playlist_id\":4235,\"user_tag_id\":7236,\"tag_type\":\"inc\"},\"Children\":{\"user_tag\":[{\"Record\":{\"user_id\":1234,\"tag_name\":\"b\",\"id\":7236},\"Children\":{}}]}}],\"playlist_media\":[{\"Record\":{\"pos\":0,\"playlist_id\":4235,\"media_id\":2234,\"id\":6234},\"Children\":{\"media\":[{\"Record\":{\"user_id\":1234,\"id\":2234,\"title\":\"big buck bunny\",\"status\":\"ready\"},\"Children\":{}}]}},{\"Record\":{\"pos\":1,\"playlist_id\":4235,\"media_id\":2235,\"id\":6235},\"Children\":{\"media\":[{\"Record\":{\"user_id\":1234,\"id\":2235,\"title\":\"something.mov\",\"status\":\"ready\"},\"Children\":{}}]}},{\"Record\":{\"pos\":2,\"playlist_id\":4235,\"media_id\":2236,\"id\":6236},\"Children\":{\"media\":[{\"Record\":{\"user_id\":1234,\"id\":2236,\"title\":\"something_else.mp4\",\"status\":\"ready\"},\"Children\":{}}]}}]}}", DenormalizedRecord.class));
        expectedResults.put(ByteArray.toByteArray(4236), mapper.readValue("{\"Record\":{\"user_id\":1235,\"active\":1,\"id\":4236,\"type\":\"trending\",\"title\":\"Test Trending Playlist\"},\"Children\":{\"user\":[{\"Record\":{\"user_id\":1235,\"usage_type\":\"unlimited\",\"user_name\":\"TROGDOR\",\"email\":\"TROGDOR@jwplayer.com\"},\"Children\":{}}],\"playlist_custom_params\":[],\"playlist_tag\":[{\"Record\":{\"playlist_id\":4236,\"user_tag_id\":7236,\"tag_type\":\"inc\"},\"Children\":{\"user_tag\":[{\"Record\":{\"user_id\":1234,\"tag_name\":\"b\",\"id\":7236},\"Children\":{}}]}},{\"Record\":{\"playlist_id\":4236,\"user_tag_id\":7237,\"tag_type\":\"nada\"},\"Children\":{\"user_tag\":[{\"Record\":{\"user_id\":1235,\"tag_name\":\"b\",\"id\":7237},\"Children\":{}}]}}],\"playlist_media\":[{\"Record\":{\"pos\":0,\"playlist_id\":4236,\"media_id\":2237,\"id\":6237},\"Children\":{\"media\":[{\"Record\":{\"user_id\":1235,\"id\":2237,\"title\":\"Title Change\",\"status\":\"ready\"},\"Children\":{}}]}},{\"Record\":{\"pos\":1,\"playlist_id\":4236,\"media_id\":2238,\"id\":6238},\"Children\":{\"media\":[]}},{\"Record\":{\"pos\":2,\"playlist_id\":4236,\"media_id\":2237,\"id\":6239},\"Children\":{\"media\":[{\"Record\":{\"user_id\":1235,\"id\":2237,\"title\":\"Title Change\",\"status\":\"ready\"},\"Children\":{}}]}}]}}", DenormalizedRecord.class));
        expectedResults.put(ByteArray.toByteArray(4237), mapper.readValue("{\"Record\":{\"user_id\":1235,\"active\":1,\"id\":4237,\"type\":\"dynamic\",\"title\":\"Pinned Dynamic\"},\"Children\":{\"user\":[{\"Record\":{\"user_id\":1235,\"usage_type\":\"unlimited\",\"user_name\":\"TROGDOR\",\"email\":\"TROGDOR@jwplayer.com\"},\"Children\":{}}],\"playlist_custom_params\":[],\"playlist_tag\":[{\"Record\":{\"playlist_id\":4237,\"user_tag_id\":7235,\"tag_type\":\"nada\"},\"Children\":{\"user_tag\":[{\"Record\":{\"user_id\":1234,\"tag_name\":\"b\",\"id\":7235},\"Children\":{}}]}}],\"playlist_media\":[]}}", DenormalizedRecord.class));
        expectedResults.put(ByteArray.toByteArray(4234), null);
        expectedResults.put(ByteArray.toByteArray(4238), mapper.readValue("{\"Record\":{\"user_id\":1234,\"active\":1,\"id\":4238,\"type\":\"feed\",\"title\":\"look mom, I'm a title\"},\"Children\":{\"user\":[{\"Record\":{\"user_id\":1234,\"usage_type\":\"monthly\",\"user_name\":\"Suzy\",\"email\":\"Suzy+something@jwplayer.com\"},\"Children\":{}}],\"playlist_custom_params\":[],\"playlist_tag\":[{\"Record\":{\"playlist_id\":4238,\"user_tag_id\":7238,\"tag_type\":\"nada\"},\"Children\":{\"user_tag\":[{\"Record\":{\"user_id\":1235,\"tag_name\":\"b\",\"id\":7238},\"Children\":{}}]}}],\"playlist_media\":[]}}", DenormalizedRecord.class));
        // Expected Media results
        expectedResults.put(ByteArray.toByteArray(2234), mapper.readValue("{\"Record\":{\"user_id\":1234,\"id\":2234,\"title\":\"big buck bunny\",\"status\":\"ready\"},\"Children\":{\"user\":[{\"Record\":{\"user_id\":1234,\"usage_type\":\"monthly\",\"user_name\":\"Suzy\",\"email\":\"Suzy+something@jwplayer.com\"},\"Children\":{}}],\"playlist_media\":[{\"Record\":{\"pos\":0,\"playlist_id\":4235,\"media_id\":2234,\"id\":6234},\"Children\":{\"playlist\":[{\"Record\":{\"user_id\":1234,\"active\":1,\"id\":4235,\"type\":\"feed\",\"title\":\"Titled Similar Playlist\"},\"Children\":{}}]}}]}}", DenormalizedRecord.class));
        expectedResults.put(ByteArray.toByteArray(2235), mapper.readValue("{\"Record\":{\"user_id\":1234,\"id\":2235,\"title\":\"something.mov\",\"status\":\"ready\"},\"Children\":{\"user\":[{\"Record\":{\"user_id\":1234,\"usage_type\":\"monthly\",\"user_name\":\"Suzy\",\"email\":\"Suzy+something@jwplayer.com\"},\"Children\":{}}],\"playlist_media\":[{\"Record\":{\"pos\":1,\"playlist_id\":4235,\"media_id\":2235,\"id\":6235},\"Children\":{\"playlist\":[{\"Record\":{\"user_id\":1234,\"active\":1,\"id\":4235,\"type\":\"feed\",\"title\":\"Titled Similar Playlist\"},\"Children\":{}}]}}]}}", DenormalizedRecord.class));
        expectedResults.put(ByteArray.toByteArray(2236), mapper.readValue("{\"Record\":{\"user_id\":1234,\"id\":2236,\"title\":\"something_else.mp4\",\"status\":\"ready\"},\"Children\":{\"user\":[{\"Record\":{\"user_id\":1234,\"usage_type\":\"monthly\",\"user_name\":\"Suzy\",\"email\":\"Suzy+something@jwplayer.com\"},\"Children\":{}}],\"playlist_media\":[{\"Record\":{\"pos\":2,\"playlist_id\":4235,\"media_id\":2236,\"id\":6236},\"Children\":{\"playlist\":[{\"Record\":{\"user_id\":1234,\"active\":1,\"id\":4235,\"type\":\"feed\",\"title\":\"Titled Similar Playlist\"},\"Children\":{}}]}}]}}", DenormalizedRecord.class));
        expectedResults.put(ByteArray.toByteArray(2237), mapper.readValue("{\"Record\":{\"user_id\":1235,\"id\":2237,\"title\":\"Title Change\",\"status\":\"ready\"},\"Children\":{\"user\":[{\"Record\":{\"user_id\":1235,\"usage_type\":\"unlimited\",\"user_name\":\"TROGDOR\",\"email\":\"TROGDOR@jwplayer.com\"},\"Children\":{}}],\"playlist_media\":[{\"Record\":{\"pos\":0,\"playlist_id\":4236,\"media_id\":2237,\"id\":6237},\"Children\":{\"playlist\":[{\"Record\":{\"user_id\":1235,\"active\":1,\"id\":4236,\"type\":\"trending\",\"title\":\"Test Trending Playlist\"},\"Children\":{}}]}},{\"Record\":{\"pos\":2,\"playlist_id\":4236,\"media_id\":2237,\"id\":6239},\"Children\":{\"playlist\":[{\"Record\":{\"user_id\":1235,\"active\":1,\"id\":4236,\"type\":\"trending\",\"title\":\"Test Trending Playlist\"},\"Children\":{}}]}}]}}", DenormalizedRecord.class));
        expectedResults.put(ByteArray.toByteArray(2238), null);
        // Expected Player results
        expectedResults.put(ByteArray.toByteArray(3234), mapper.readValue("{\"Record\":{\"user_id\":1235,\"name\":\"16:9 example player\",\"id\":3234},\"Children\":{\"user\":[{\"Record\":{\"user_id\":1235,\"usage_type\":\"unlimited\",\"user_name\":\"TROGDOR\",\"email\":\"TROGDOR@jwplayer.com\"},\"Children\":{}}]}}", DenormalizedRecord.class));
        expectedResults.put(ByteArray.toByteArray(3235), mapper.readValue("{\"Record\":{\"user_id\":1234,\"name\":\"THIS IS A PLAYER\",\"id\":3235},\"Children\":{\"user\":[{\"Record\":{\"user_id\":1234,\"usage_type\":\"monthly\",\"user_name\":\"Suzy\",\"email\":\"Suzy+something@jwplayer.com\"},\"Children\":{}}]}}", DenormalizedRecord.class));

        // Generate the actual results
        int maxRecords = 0;
        Yaml yaml = new Yaml();
        Map<String, Object> config = yaml.load(FileHelper.getInputStream(new URI(CONFIG_PATH)));
        MockSouthpaw southpaw = new MockSouthpaw(
                config,
                Arrays.asList(new URI(RELATIONS_PATH), new URI(RELATIONS_PATH2), new URI(RELATIONS_PATH3))
        );
        southpaw.deleteBackups();
        Map<String, BaseTopic<BaseRecord, BaseRecord>> normalizedTopics = southpaw.getNormalizedTopics();
        Map<String, String[]> records = new HashMap<>();
        for(Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>> entry: normalizedTopics.entrySet()) {
            records.put(entry.getKey(), FileHelper.loadFileAsString(new URI(TOPIC_DATA_PATH + entry.getKey() + ".json")).split("\n"));
            maxRecords = Math.max(records.get(entry.getKey()).length, maxRecords);
        }
        Map<String, Map<ByteArray, DenormalizedRecord>> denormalizedRecords = new HashMap<>();
        List<Object[]> retVal = new ArrayList<>();

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
            southpaw.run(1);
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
        southpaw.deleteBackups();
        southpaw.deleteState();

        assertEquals(12, retVal.size());
        return retVal;
    }

    @Test
    public void testRecord() {
        assertEquals(expectedRecord, actualRecord);
    }
}
