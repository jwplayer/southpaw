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
package com.jwplayer.southpaw.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jwplayer.southpaw.json.Relation;
import com.jwplayer.southpaw.util.FileHelper;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;


public class JacksonSerdeTest {
    protected static final String RELATIONS = "[{\"DenormalizedName\":\"DenormalizedPlaylist\",\"Entity\":\"playlist\",\"Children\":[{\"Entity\":\"user\",\"JoinKey\":\"user_id\",\"ParentKey\":\"user_id\",\"Children\":[]},{\"Entity\":\"playlist_tag\",\"JoinKey\":\"playlist_id\",\"ParentKey\":\"id\",\"Children\":[{\"Entity\":\"user_tag\",\"JoinKey\":\"id\",\"ParentKey\":\"user_tag_id\",\"Children\":[]}]},{\"Entity\":\"playlist_custom_params\",\"JoinKey\":\"playlist_id\",\"ParentKey\":\"id\",\"Children\":[]},{\"Entity\":\"playlist_media\",\"JoinKey\":\"playlist_id\",\"ParentKey\":\"id\",\"Children\":[{\"Entity\":\"media\",\"JoinKey\":\"id\",\"ParentKey\":\"media_id\",\"Children\":[]}]}]}]";
    protected static final String RELATIONS_PATH = "test-resources/relations.sample.json";

    protected ObjectMapper mapper;
    protected JacksonSerde<Relation[]> serde;

    @Before
    public void setUp() {
        mapper = new ObjectMapper();
        serde = new JacksonSerde<>();
        Map<String, Object> config = new HashMap<>();
        config.put(JacksonSerde.CLASS_CONFIG, "[Lcom.jwplayer.southpaw.json.Relation;");
        serde.configure(config, false);
    }

    @Test
    public void testClose() {
        serde.close();
    }

    @Test
    public void testDeserializer() throws Exception {
        Relation[] expected = mapper.readValue(FileHelper.loadFileAsString(new URI(RELATIONS_PATH)), Relation[].class);
        Relation[] actual = serde.deserializer().deserialize(null, RELATIONS.getBytes());
        assertEquals(1, actual.length);
        assertEquals(expected[0], actual[0]);
    }

    @Test
    public void testDeserializerNull() {
        Relation[] actual = serde.deserializer().deserialize(null, null);
        assertNull(actual);
    }

    @Test
    public void testSerializer() throws Exception {
        Relation[] relations = mapper.readValue(FileHelper.loadFileAsString(new URI(RELATIONS_PATH)), Relation[].class);
        byte[] bytes = serde.serializer().serialize(null, relations);
        assertEquals(
                RELATIONS,
                new String(bytes)
        );
    }

    @Test
    public void testSerializerNull() {
        byte[] bytes = serde.serializer().serialize(null, null);
        assertNull(bytes);
    }
}
