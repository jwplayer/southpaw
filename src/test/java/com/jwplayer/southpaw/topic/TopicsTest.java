package com.jwplayer.southpaw.topic;

import com.jwplayer.southpaw.MockState;
import com.jwplayer.southpaw.metric.Metrics;
import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.util.FileHelper;
import com.jwplayer.southpaw.util.RelationHelper;
import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.net.URI;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;

public class TopicsTest {
    private static final String CONFIG_PATH = "test-resources/config.sample.yaml";
    private static final String RELATIONS_PATH = "test-resources/relations.sample.json";

    private Topics topics;

    @Before
    public void setup() throws Exception {
        Yaml yaml = new Yaml();
        Map<String, Object> config = yaml.load(FileHelper.getInputStream(new URI(CONFIG_PATH)));
        URI relationsUri = new URI(RELATIONS_PATH);
        topics = new Topics(
                config,
                new Metrics(),
                new MockState(),
                RelationHelper.loadRelations(Collections.singletonList(relationsUri)));
    }

    @Test
    public void testCreateTopicConfig() {
        Map<String, Object> kafkaConfig = topics.createTopicConfig("Overrides");

        assertEquals(kafkaConfig.get("bootstrap.servers"), "kafka:29092");
        assertEquals(kafkaConfig.get("client.id"), "overrides");
        assertEquals(kafkaConfig.get("group.id"), "southpaw");
        assertEquals(kafkaConfig.get("key.serde.class"), "com.jwplayer.southpaw.serde.AvroSerde");
        assertEquals(kafkaConfig.get("schema.registry.url"), "http://schema-registry:8081");
        assertEquals(kafkaConfig.get("value.serde.class"), "com.jwplayer.southpaw.serde.AvroSerde");
        assertEquals(kafkaConfig.get("topic.name"), "overrides");
    }

    @Test
    public void testInputTopics() {
        Map<String, BaseTopic<BaseRecord, BaseRecord>> inputTopics = topics.inputTopics;

        assertEquals(7, inputTopics.size());
        assertTrue(inputTopics.containsKey("media"));
        assertTrue(inputTopics.containsKey("playlist"));
        assertTrue(inputTopics.containsKey("playlist_custom_params"));
        assertTrue(inputTopics.containsKey("playlist_media"));
        assertTrue(inputTopics.containsKey("playlist_tag"));
        assertTrue(inputTopics.containsKey("user"));
        assertTrue(inputTopics.containsKey("user_tag"));
    }
}