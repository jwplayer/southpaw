package com.jwplayer.southpaw.index;

import com.jwplayer.southpaw.MockState;
import com.jwplayer.southpaw.json.Relation;
import com.jwplayer.southpaw.state.BaseState;
import com.jwplayer.southpaw.util.FileHelper;
import com.jwplayer.southpaw.util.RelationHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.yaml.snakeyaml.Yaml;

import java.net.URI;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;

public class IndicesTest {
    private static final String CONFIG_PATH = "test-resources/config.sample.yaml";
    private static final String RELATIONS_PATH = "test-resources/relations.sample.json";

    private Indices indices;
    private BaseState state;

    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();

    @Rule
    public TemporaryFolder backupFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception{
        Yaml yaml = new Yaml();
        Map<String, Object> config = yaml.load(FileHelper.getInputStream(new URI(CONFIG_PATH)));
        state = createState();
        Relation[] relations = RelationHelper.loadRelations(Collections.singletonList(new URI(RELATIONS_PATH)));
        indices = new Indices(config, state, relations);
    }

    @After
    public void cleanup() {
        state.close();
        state.delete();
    }

    private BaseState createState() {
        BaseState state = new MockState();
        state.open();
        return state;
    }

    @Test
    public void testIndices() {
        // Join Indices
        assertEquals(6, indices.joinIndices.size());
        assertTrue(indices.joinIndices.containsKey("JK|media|id"));
        assertTrue(indices.joinIndices.containsKey("JK|playlist_custom_params|playlist_id"));
        assertTrue(indices.joinIndices.containsKey("JK|playlist_media|playlist_id"));
        assertTrue(indices.joinIndices.containsKey("JK|playlist_tag|playlist_id"));
        assertTrue(indices.joinIndices.containsKey("JK|user|user_id"));
        assertTrue(indices.joinIndices.containsKey("JK|user_tag|id"));

        // Parent Indices
        assertEquals(4, indices.parentIndices.size());
        assertTrue(indices.parentIndices.containsKey("PaK|playlist|playlist_media|media_id"));
        assertTrue(indices.parentIndices.containsKey("PaK|playlist|playlist|id"));
        assertTrue(indices.parentIndices.containsKey("PaK|playlist|playlist|user_id"));
        assertTrue(indices.parentIndices.containsKey("PaK|playlist|playlist_tag|user_tag_id"));
    }
}
