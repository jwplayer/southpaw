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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jwplayer.southpaw.json.DenormalizedRecord;
import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.topic.BaseTopic;
import com.jwplayer.southpaw.util.ByteArray;
import com.jwplayer.southpaw.util.FileHelper;
import com.jwplayer.southpaw.util.KafkaTestServer;

import scala.Tuple2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static org.junit.Assert.*;


public class SouthpawByteArraySetEndToEndTest {
    private static final String CONFIG_PATH = "test-resources/ByteArraySetTest/config.yaml.tmpl";
    private static final String RELATIONS_PATH = "test-resources/ByteArraySetTest/relation.json";
    private static final String ROCKSDB_BASE_URI = "file:///tmp/RocksDB/ByteArraySetTest/";
    private static final String TOPIC_DATA_PATH = "test-resources/ByteArraySetTest/topic/";

    private KafkaTestServer kafkaServer;

    @Before
    public void setup() throws IOException, URISyntaxException {
        getRocksDBFolder().delete();
        getRocksDBFolder().mkdirs();
        kafkaServer = new KafkaTestServer();
    }

    @After
    public void cleanup() throws URISyntaxException {
        getRocksDBFolder().delete();
        kafkaServer.shutdown();
    }

    @Test
    public void testConsume() throws Exception {
        // Builds expected records
        Map<ByteArray, List<DenormalizedRecord>> expectedRecords = new HashMap<ByteArray, List<DenormalizedRecord>>();
        Map<ByteArray, Integer> mapping = new HashMap<ByteArray, Integer>();
        List<Integer> mediaIds1233 = new ArrayList<Integer>(Arrays.asList(2017, 2042, 2043, 2073, 2124, 2143));
        for(int mediaId = 2001; mediaId <= 2150 ; mediaId++) {
            String mediaIdStr = String.valueOf(mediaId);
            List<DenormalizedRecord> expectedRecordsMediaId;
            if (mediaIds1233.contains(mediaId)) {
                expectedRecordsMediaId = new ArrayList<DenormalizedRecord>(Arrays.asList(
                        getInitialRecord(mediaIdStr),
                        getNoUserCustomParamsRecord(mediaIdStr, "1233")
                ));
            } else {
                expectedRecordsMediaId = new ArrayList<DenormalizedRecord>(Arrays.asList(
                        getInitialRecord(mediaIdStr),
                        // getUserCustomParamsRecord(mediaIdStr, "1234", "3234", "es"),
                        // getUserCustomParamsRecord(mediaIdStr, "1234", "3234", "es", "3234", "fr"),
                        getUserCustomParamsRecord(mediaIdStr, "1234", "3234", "es", "3234", "fr", "3234", "th")
                ));
            }
            expectedRecords.put(ByteArray.toByteArray(mediaId), expectedRecordsMediaId);
            mapping.put(ByteArray.toByteArray(mediaId), mediaId);
        }

        // Sets up environment
        Map<String, Object> config = getConfig();
        MockSouthpaw southpaw = new MockSouthpaw(
                config,
                Arrays.asList(new URI(RELATIONS_PATH))
        );
        Southpaw.deleteBackups(config);
        Map<String, BaseTopic<BaseRecord, BaseRecord>> normalizedTopics = southpaw.getNormalizedTopics();

        // Triggers first Southpaw run
        Map<String, String[]> records = new HashMap<String, String[]>();
        for(Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>> entry: normalizedTopics.entrySet()) {
            records.put(entry.getKey(), FileHelper.loadFileAsString(new URI(TOPIC_DATA_PATH + entry.getKey() + ".json")).split("\n"));
        }
        for(Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>> entry: normalizedTopics.entrySet()) {
            String[] json = records.get(entry.getKey());
            for(String val: json) {
                entry.getValue().write(
                        entry.getValue().getKeySerde().deserializer().deserialize(null, val.getBytes()),
                        entry.getValue().getValueSerde().deserializer().deserialize(null, val.getBytes())
                );
            }
        }
        southpaw.run(1);

        // Triggers second Southpaw run
        String[] extra_vals = FileHelper.loadFileAsString(new URI(TOPIC_DATA_PATH + "user_custom_params_extra.json")).split("\n");
        for(String val: extra_vals) {
        	normalizedTopics.get("user_custom_params").write(
        			normalizedTopics.get("user_custom_params").getKeySerde().deserializer().deserialize(null, val.getBytes()),
        			normalizedTopics.get("user_custom_params").getValueSerde().deserializer().deserialize(null, val.getBytes())
            );
        }
        southpaw.run(1);

        // Fetches actual records
        Map<String, Map<ByteArray, List<DenormalizedRecord>>> denormalizedRecords = new HashMap<String, Map<ByteArray, List<DenormalizedRecord>>>();
        for(Map.Entry<String, BaseTopic<byte[], DenormalizedRecord>> entry: southpaw.outputTopics.entrySet()) {
            if (!denormalizedRecords.containsKey(entry.getKey())) {
                denormalizedRecords.put(entry.getKey(), new HashMap<ByteArray, List<DenormalizedRecord>>());
            }
            entry.getValue().resetCurrentOffset();
            Iterator<ConsumerRecord<byte[], DenormalizedRecord>> iter = entry.getValue().readNext();
            while(iter.hasNext()) {
                ConsumerRecord<byte[], DenormalizedRecord> record = iter.next();
                ByteArray recordKey = new ByteArray(record.key());
                if (!denormalizedRecords.get(entry.getKey()).containsKey(recordKey)) {
                    denormalizedRecords.get(entry.getKey()).put(recordKey, new ArrayList<DenormalizedRecord>());
                }
                denormalizedRecords.get(entry.getKey()).get(recordKey).add(record.value());
            }
        }

        // Verifies records
        for(Map.Entry<String, Map<ByteArray, List<DenormalizedRecord>>> entry: denormalizedRecords.entrySet()) {
            for(Map.Entry<ByteArray, List<DenormalizedRecord>> innerEntry: entry.getValue().entrySet()) {
                for (DenormalizedRecord record: innerEntry.getValue()) {
                    ByteArray key = ByteArray.toByteArray((int) record.getRecord().getAdditionalProperties().get("id"));
                    if (expectedRecords.get(key) == null) {
                        continue;
                    } else if (expectedRecords.get(key).contains(record)) {
                        expectedRecords.get(key).remove(record);
                    }
                }
            }
        }
        List<Integer> failingMediaIds = new ArrayList<Integer>();
        for(Map.Entry<ByteArray, List<DenormalizedRecord>> entry: expectedRecords.entrySet()) {
            if (entry.getValue().size() > 0) {
                failingMediaIds.add(mapping.get(entry.getKey()));
            }
        }
        assertEquals(new ArrayList<Integer>(), failingMediaIds);

        // Tears down environment
        southpaw.close();
        Southpaw.deleteBackups(config);
        Southpaw.deleteState(config);
    }

    private static File getRocksDBFolder() throws URISyntaxException {
        return new File(new URI(ROCKSDB_BASE_URI));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getConfig() throws IOException, URISyntaxException {
        Yaml yaml = new Yaml();
        Map<String, Object> config = yaml.load(FileHelper.getInputStream(new URI(CONFIG_PATH)));

        // Overrides bootstrap servers to point at the local Kafka cluster
        Map<String, Object> sectionConfig = config;
        for (String section: new ArrayList<String>(Arrays.asList("topics", "default"))) {
            if (!sectionConfig.containsKey(section)) {
                sectionConfig.put(section, new HashMap<String, Object>());
            }
            sectionConfig = (Map<String, Object>) sectionConfig.get(section);
        }
        sectionConfig.put("bootstrap.servers", kafkaServer.getConnectionString());

        return config;
    }

    private static DenormalizedRecord getInitialRecord(String id) throws JsonParseException, JsonMappingException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(
                String.format("{\"Record\":{\"id\":%s},\"Children\":{}}",
                        id),
                DenormalizedRecord.class);
    }

    private static DenormalizedRecord getNoUserCustomParamsRecord(String id, String user_id) throws JsonParseException, JsonMappingException, IOException {
        return getUserCustomParamsRecord(id, user_id, new ArrayList<Tuple2<String, String>>());
    }

    private static DenormalizedRecord getUserCustomParamsRecord(String id, String user_id, String index_language_id, String index_language_value) throws JsonParseException, JsonMappingException, IOException {
        List<Tuple2<String, String>> language_pairs = new ArrayList<Tuple2<String, String>>(Arrays.asList(
                new Tuple2<String, String>(index_language_id, index_language_value)));
        return getUserCustomParamsRecord(id, user_id, language_pairs);
    }

    private static DenormalizedRecord getUserCustomParamsRecord(String id, String user_id, String index_language_id_1, String index_language_value_1,
            String index_language_id_2, String index_language_value_2) throws JsonParseException, JsonMappingException, IOException {
        List<Tuple2<String, String>> language_pairs = new ArrayList<Tuple2<String, String>>(Arrays.asList(
                new Tuple2<String, String>(index_language_id_1, index_language_value_1),
                new Tuple2<String, String>(index_language_id_2, index_language_value_2)));
        return getUserCustomParamsRecord(id, user_id, language_pairs);
    }

    private static DenormalizedRecord getUserCustomParamsRecord(String id, String user_id, String index_language_id_1, String index_language_value_1,
            String index_language_id_2, String index_language_value_2,
            String index_language_id_3, String index_language_value_3) throws JsonParseException, JsonMappingException, IOException {
        List<Tuple2<String, String>> language_pairs = new ArrayList<Tuple2<String, String>>(Arrays.asList(
                new Tuple2<String, String>(index_language_id_1, index_language_value_1),
                new Tuple2<String, String>(index_language_id_2, index_language_value_2),
                new Tuple2<String, String>(index_language_id_3, index_language_value_3)));
        return getUserCustomParamsRecord(id, user_id, language_pairs);
    }

    private static DenormalizedRecord getUserCustomParamsRecord(String id, String user_id, List<Tuple2<String, String>> language_pairs) throws JsonParseException, JsonMappingException, IOException {
        String str = String.format("{\"Record\":{\"user_id\":%s,\"id\":%s,\"title\":\"Title %s\",\"status\":\"ready\"},\"Children\":{\"user_custom_params\":[",
                user_id,
                id,
                id);
        while (language_pairs.size() > 0) {
            Tuple2<String, String> language_pair = language_pairs.get(0);
            language_pairs.remove(0);
            String str_pair_template = "{\"Record\":{\"id\":%s,\"user_id\":%s,\"index_language\":\"%s\"},\"Children\":{}}";
            if (language_pairs.size() > 0) {
                str_pair_template += ",";
            }
            str += String.format(str_pair_template,
                    language_pair._1,
                    user_id,
                    language_pair._2);
        }
        str += "]}}";
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue( str, DenormalizedRecord.class);
    }
}
