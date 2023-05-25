package com.jwplayer.southpaw.index;

import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.record.MapRecord;
import com.jwplayer.southpaw.state.InMemoryState;
import com.jwplayer.southpaw.TestHelper;
import com.jwplayer.southpaw.json.Relation;
import com.jwplayer.southpaw.metric.Metrics;
import com.jwplayer.southpaw.state.BaseState;
import com.jwplayer.southpaw.util.ByteArray;
import com.jwplayer.southpaw.util.FileHelper;
import com.jwplayer.southpaw.util.RelationHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.net.URI;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class IndicesTest {
    private Indices indices;
    private BaseState state;
    private Relation[] relations;

    @Before
    public void setup() throws Exception{
        Yaml yaml = new Yaml();
        Map<String, Object> config = yaml.load(FileHelper.getInputStream(new URI(TestHelper.CONFIG_PATH)));
        state = new InMemoryState();
        state.open();
        relations = RelationHelper.loadRelations(Collections.singletonList(new URI(TestHelper.RELATION_PATHS.PLAYLIST)));
        indices = new Indices(config, new Metrics(), state, relations);
        indices.joinIndices.replaceAll((k, v) -> spy(v));
        indices.parentIndices.replaceAll((k, v) -> spy(v));
    }

    @After
    public void cleanup() {
        state.close();
        state.delete();
    }

    public void populateIndices(Relation root, Relation parent, Relation child) throws Exception {
        if(parent != null) {
            String joinIndexName = Indices.createJoinIndexName(child);
            TestHelper.populateIndex(indices.joinIndices.get(joinIndexName));
            String parentIndexName = Indices.createParentIndexName(root, parent, child);
            TestHelper.populateIndex(indices.parentIndices.get(parentIndexName));
        }

        if(child.getChildren() != null) {
            for(Relation grandchild: child.getChildren()) {
                populateIndices(root, child, grandchild);
            }
        }
    }

    @Test
    public void testCreateIndices() {
        // Join Indices
        assertEquals(6, indices.joinIndices.size());
        assertTrue(indices.joinIndices.containsKey(TestHelper.PLAYLIST_JOIN_INDICES.MEDIA));
        assertTrue(indices.joinIndices.containsKey(TestHelper.PLAYLIST_JOIN_INDICES.PLAYLIST_CUSTOM_PARAMS));
        assertTrue(indices.joinIndices.containsKey(TestHelper.PLAYLIST_JOIN_INDICES.PLAYLIST_MEDIA));
        assertTrue(indices.joinIndices.containsKey(TestHelper.PLAYLIST_JOIN_INDICES.PLAYLIST_TAG));
        assertTrue(indices.joinIndices.containsKey(TestHelper.PLAYLIST_JOIN_INDICES.USER));
        assertTrue(indices.joinIndices.containsKey(TestHelper.PLAYLIST_JOIN_INDICES.USER_TAG));

        // Parent Indices
        assertEquals(4, indices.parentIndices.size());
        assertTrue(indices.parentIndices.containsKey(TestHelper.PLAYLIST_PARENT_INDICES.PLAYLIST_MEDIA));
        assertTrue(indices.parentIndices.containsKey(TestHelper.PLAYLIST_PARENT_INDICES.PLAYLIST));
        assertTrue(indices.parentIndices.containsKey(TestHelper.PLAYLIST_PARENT_INDICES.PLAYLIST_USER));
        assertTrue(indices.parentIndices.containsKey(TestHelper.PLAYLIST_PARENT_INDICES.PLAYLIST_TAG));
    }

    @Test
    public void testCreateJoinIndexName() {
        String indexName = Indices.createJoinIndexName(relations[0].getChildren().get(0));
        assertEquals(TestHelper.PLAYLIST_JOIN_INDICES.USER, indexName);
    }

    @Test
    public void testCreateParentIndexName() {
        String indexName = Indices.createParentIndexName(relations[0], relations[0], relations[0].getChildren().get(0));
        assertEquals(TestHelper.PLAYLIST_PARENT_INDICES.PLAYLIST_USER, indexName);
    }

    @Test
    public void testFlush() {
        indices.flush();
        indices.joinIndices.forEach((name, index) -> verify(index, times(1)).flush());
        indices.parentIndices.forEach((name, index) -> verify(index, times(1)).flush());
    }

    @Test
    public void testGetRelationJKs() {
        ByteArray primaryKey = ByteArray.toByteArray(5);
        indices.getRelationJKs(relations[0].getChildren().get(0), primaryKey);
        verify(indices.joinIndices.get(TestHelper.PLAYLIST_JOIN_INDICES.USER), times(1)).getForeignKeys(primaryKey);
    }

    @Test
    public void testGetRelationPKs() {
        ByteArray joinKey = ByteArray.toByteArray(5);
        indices.getRelationPKs(relations[0].getChildren().get(0), joinKey);
        verify(indices.joinIndices.get(TestHelper.PLAYLIST_JOIN_INDICES.USER), times(1)).getIndexEntry(joinKey);
    }

    @Test
    public void testGetRootPKs() {
        ByteArray joinKey = ByteArray.toByteArray(5);
        indices.getRootPKs(relations[0], relations[0], relations[0].getChildren().get(0), joinKey);
        verify(indices.parentIndices.get(TestHelper.PLAYLIST_PARENT_INDICES.PLAYLIST_USER), times(1)).getIndexEntry(joinKey);
    }

    @Test
    public void testScrubParentIndices() throws Exception {
        populateIndices(relations[0], null, relations[0]);
        ByteArray rootPrimaryKey = ByteArray.toByteArray(4235);
        indices.scrubParentIndices(relations[0], relations[0], rootPrimaryKey);
        // Check playlist index
        Index playlistIndex = indices.parentIndices.get(TestHelper.PLAYLIST_PARENT_INDICES.PLAYLIST);
        assertNull(playlistIndex.getIndexEntry(rootPrimaryKey));
        assertNull(playlistIndex.getForeignKeys(rootPrimaryKey));
        TestHelper.verifyIndexState(playlistIndex);
        // Check playlist <-> media index
        Index playlistMediaIndex = indices.parentIndices.get(TestHelper.PLAYLIST_PARENT_INDICES.PLAYLIST_MEDIA);
        assertNull(playlistMediaIndex.getIndexEntry(ByteArray.toByteArray(2234)));
        assertNull(playlistMediaIndex.getIndexEntry(ByteArray.toByteArray(2235)));
        assertNull(playlistMediaIndex.getIndexEntry(ByteArray.toByteArray(2236)));
        assertNull(playlistMediaIndex.getForeignKeys(rootPrimaryKey));
        TestHelper.verifyIndexState(playlistMediaIndex);
        // Check playlist <-> tag index
        Index playlistTagIndex = indices.parentIndices.get(TestHelper.PLAYLIST_PARENT_INDICES.PLAYLIST_TAG);
        assertNull(playlistTagIndex.getIndexEntry(ByteArray.toByteArray(7234)));
        assertEquals(TestHelper.toSet(4237), playlistTagIndex.getIndexEntry(ByteArray.toByteArray(7235)));
        assertEquals(TestHelper.toSet(4236), playlistTagIndex.getIndexEntry(ByteArray.toByteArray(7236)));
        assertNull(playlistTagIndex.getForeignKeys(rootPrimaryKey));
        TestHelper.verifyIndexState(playlistTagIndex);
        // Check playlist <-> user index
        Index playlistUserIndex = indices.parentIndices.get(TestHelper.PLAYLIST_PARENT_INDICES.PLAYLIST_USER);
        assertEquals(TestHelper.toSet(4238), playlistUserIndex.getIndexEntry(ByteArray.toByteArray(1234)));
        assertNull(playlistUserIndex.getForeignKeys(rootPrimaryKey));
        TestHelper.verifyIndexState(playlistUserIndex);
    }

    @Test
    public void testUpdateJoinIndexWithChangedFK() throws Exception {
        populateIndices(relations[0], null, relations[0]);
        Relation playlistMedia = relations[0].getChildren().get(3);
        Index index = indices.joinIndices.get(Indices.createJoinIndexName(playlistMedia));
        ConsumerRecord<BaseRecord, BaseRecord> newRecord = new ConsumerRecord<>("", 0, 0L,
                new MapRecord(Collections.singletonMap("id", 6234)),
                new MapRecord(Collections.singletonMap("playlist_id", 4236)));
        indices.updateJoinIndex(playlistMedia, newRecord);
        assertEquals(TestHelper.toSet(6235, 6236), index.getIndexEntry(ByteArray.toByteArray(4235)));
        assertEquals(TestHelper.toSet(6234, 6237, 6238, 6239), index.getIndexEntry(ByteArray.toByteArray(4236)));
        assertEquals(TestHelper.toSet(4236), index.getForeignKeys(ByteArray.toByteArray(6234)));
        TestHelper.verifyIndexState(index);
    }

    @Test
    public void testUpdateJoinIndexWithExistingPair() throws Exception {
        populateIndices(relations[0], null, relations[0]);
        Relation playlistMedia = relations[0].getChildren().get(3);
        Index index = indices.joinIndices.get(Indices.createJoinIndexName(playlistMedia));
        ConsumerRecord<BaseRecord, BaseRecord> newRecord = new ConsumerRecord<>("", 0, 0L,
                new MapRecord(Collections.singletonMap("id", 6234)),
                new MapRecord(Collections.singletonMap("playlist_id", 4235)));
        indices.updateJoinIndex(playlistMedia, newRecord);
        assertEquals(TestHelper.toSet(6234, 6235, 6236), index.getIndexEntry(ByteArray.toByteArray(4235)));
        assertEquals(TestHelper.toSet(6237, 6238, 6239), index.getIndexEntry(ByteArray.toByteArray(4236)));
        assertEquals(TestHelper.toSet(4235), index.getForeignKeys(ByteArray.toByteArray(6234)));
        TestHelper.verifyIndexState(index);
    }

    @Test
    public void testUpdateJoinIndexWithNewPair() throws Exception {
        populateIndices(relations[0], null, relations[0]);
        Relation playlistMedia = relations[0].getChildren().get(3);
        Index index = indices.joinIndices.get(Indices.createJoinIndexName(playlistMedia));
        ConsumerRecord<BaseRecord, BaseRecord> newRecord = new ConsumerRecord<>("", 0, 0L,
                new MapRecord(Collections.singletonMap("id", 6270)),
                new MapRecord(Collections.singletonMap("playlist_id", 4236)));
        indices.updateJoinIndex(playlistMedia, newRecord);
        assertEquals(TestHelper.toSet(6234, 6235, 6236), index.getIndexEntry(ByteArray.toByteArray(4235)));
        assertEquals(TestHelper.toSet(6237, 6238, 6239, 6270), index.getIndexEntry(ByteArray.toByteArray(4236)));
        assertEquals(TestHelper.toSet(4236), index.getForeignKeys(ByteArray.toByteArray(6270)));
        TestHelper.verifyIndexState(index);
    }

    @Test
    public void testUpdateJoinIndexWithNullRecord() throws Exception {
        populateIndices(relations[0], null, relations[0]);
        Relation playlistMedia = relations[0].getChildren().get(3);
        Index index = indices.joinIndices.get(Indices.createJoinIndexName(playlistMedia));
        ConsumerRecord<BaseRecord, BaseRecord> newRecord = new ConsumerRecord<>("", 0, 0L,
                new MapRecord(Collections.singletonMap("id", 6234)),
                null);
        indices.updateJoinIndex(playlistMedia, newRecord);
        assertEquals(TestHelper.toSet(6235, 6236), index.getIndexEntry(ByteArray.toByteArray(4235)));
        assertEquals(TestHelper.toSet(6237, 6238, 6239), index.getIndexEntry(ByteArray.toByteArray(4236)));
        assertNull(index.getForeignKeys(ByteArray.toByteArray(6234)));
        TestHelper.verifyIndexState(index);
    }

    @Test
    public void testUpdateParentIndexWithChangedParentKey() throws Exception {
        populateIndices(relations[0], null, relations[0]);
        Relation playlistTag = relations[0].getChildren().get(1);
        Relation userTag = playlistTag.getChildren().get(0);
        Index index = indices.parentIndices.get(
                Indices.createParentIndexName(relations[0], playlistTag, userTag));
        ByteArray newParentKey = ByteArray.toByteArray(7237);
        ByteArray rootPrimaryKey = ByteArray.toByteArray(4235);
        indices.updateParentIndex(relations[0], playlistTag, userTag, rootPrimaryKey, newParentKey);
        assertEquals(TestHelper.toSet(4235, 4236), index.getIndexEntry(newParentKey));
        assertEquals(TestHelper.toSet(7234, 7235, 7236, 7237), index.getForeignKeys(rootPrimaryKey));
        TestHelper.verifyIndexState(index);
    }

    @Test
    public void testUpdateParentIndexWithChangedRootKey() throws Exception {
        populateIndices(relations[0], null, relations[0]);
        Relation playlistTag = relations[0].getChildren().get(1);
        Relation userTag = playlistTag.getChildren().get(0);
        Index index = indices.parentIndices.get(
                Indices.createParentIndexName(relations[0], playlistTag, userTag));
        ByteArray newParentKey = ByteArray.toByteArray(7235);
        ByteArray rootPrimaryKey = ByteArray.toByteArray(4236);
        indices.updateParentIndex(relations[0], playlistTag, userTag, rootPrimaryKey, newParentKey);
        assertEquals(TestHelper.toSet(4235, 4236, 4237), index.getIndexEntry(newParentKey));
        assertEquals(TestHelper.toSet(7235, 7236, 7237), index.getForeignKeys(rootPrimaryKey));
        TestHelper.verifyIndexState(index);
    }

    @Test
    public void testUpdateParentIndexWithExistingPair() throws Exception {
        populateIndices(relations[0], null, relations[0]);
        Relation playlistTag = relations[0].getChildren().get(1);
        Relation userTag = playlistTag.getChildren().get(0);
        Index index = indices.parentIndices.get(
                Indices.createParentIndexName(relations[0], playlistTag, userTag));
        ByteArray newParentKey = ByteArray.toByteArray(7235);
        ByteArray rootPrimaryKey = ByteArray.toByteArray(4235);
        indices.updateParentIndex(relations[0], playlistTag, userTag, rootPrimaryKey, newParentKey);
        assertEquals(TestHelper.toSet(4235, 4237), index.getIndexEntry(newParentKey));
        assertEquals(TestHelper.toSet(7234, 7235, 7236), index.getForeignKeys(rootPrimaryKey));
        TestHelper.verifyIndexState(index);
    }

    @Test
    public void testUpdateParentIndexWithNullKey() throws Exception {
        populateIndices(relations[0], null, relations[0]);
        Relation playlistTag = relations[0].getChildren().get(1);
        Relation userTag = playlistTag.getChildren().get(0);
        Index index = indices.parentIndices.get(
                Indices.createParentIndexName(relations[0], playlistTag, userTag));
        ByteArray newParentKey = null;
        ByteArray rootPrimaryKey = ByteArray.toByteArray(4235);
        indices.updateParentIndex(relations[0], playlistTag, userTag, rootPrimaryKey, newParentKey);
        assertEquals(TestHelper.toSet(4235, 4237), index.getIndexEntry(ByteArray.toByteArray(7235)));
        assertEquals(TestHelper.toSet(7234, 7235, 7236), index.getForeignKeys(rootPrimaryKey));
        TestHelper.verifyIndexState(index);
    }

    @Test
    public void testVerifyIndicesFailOnForwardIndex() {
        Index mediaIndex = indices.joinIndices.get(TestHelper.PLAYLIST_JOIN_INDICES.MEDIA);
        when(mediaIndex.verifyIndexState()).thenReturn(Collections.singleton("some_value"));
        assertFalse(indices.verifyIndices());
    }

    @Test
    public void testVerifyIndicesFailOnReverseIndex() {
        Index mediaIndex = indices.joinIndices.get(TestHelper.PLAYLIST_JOIN_INDICES.MEDIA);
        when(mediaIndex.verifyReverseIndexState()).thenReturn(Collections.singleton("some_value"));
        assertFalse(indices.verifyIndices());
    }

    @Test
    public void testVerifyIndicesPass() {
        assertTrue(indices.verifyIndices());
    }
}
