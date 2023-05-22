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

import com.jwplayer.southpaw.state.InMemoryState;
import com.jwplayer.southpaw.TestHelper;
import com.jwplayer.southpaw.metric.Metrics;
import com.jwplayer.southpaw.state.BaseState;
import com.jwplayer.southpaw.util.ByteArray;
import com.jwplayer.southpaw.util.ByteArraySet;
import org.apache.commons.codec.binary.Hex;
import org.junit.*;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class IndexTest {
    private Map<String, Object> config;
    private Index index;
    private BaseState state;

    @Before
    public void setup() {
        config = new HashMap<>();
        config.put(Index.INDEX_LRU_CACHE_SIZE, 2);
        config.put(Index.INDEX_WRITE_BATCH_SIZE, 5);
        state = spy(new InMemoryState());
        state.open();
        index = createEmptyIndex(config, state);
    }

    @After
    public void cleanup() {
        state.close();
        state.delete();
    }

    private Index createEmptyIndex(Map<String, Object> config, BaseState state) {
        Index index = spy(new Index());
        index.configure(TestHelper.PLAYLIST_JOIN_INDICES.PLAYLIST_MEDIA, config, new Metrics(), state);
        return index;
    }

    private void verifyIndexSate() {
        assertEquals(Collections.emptySet(), index.verifyIndexState());
        assertEquals(Collections.emptySet(), index.verifyReverseIndexState());
    }

    private void verifyPopulatedIndex() {
        assertEquals(TestHelper.toSet(6234, 6235, 6236), index.getIndexEntry(new ByteArray(4235)));
        assertEquals(TestHelper.toSet(6237, 6238, 6239), index.getIndexEntry(new ByteArray(4236)));
        assertEquals(TestHelper.toSet(4235), index.getForeignKeys(new ByteArray(6234)));
        assertEquals(TestHelper.toSet(4235), index.getForeignKeys(new ByteArray(6235)));
        assertEquals(TestHelper.toSet(4235), index.getForeignKeys(new ByteArray(6236)));
        assertEquals(TestHelper.toSet(4236), index.getForeignKeys(new ByteArray(6237)));
        assertEquals(TestHelper.toSet(4236), index.getForeignKeys(new ByteArray(6238)));
        assertEquals(TestHelper.toSet(4236), index.getForeignKeys(new ByteArray(6239)));
    }

    @Test
    public void testAddConsistency() {
        //Populate a record only to the index
        ByteArraySet indexKeys = new ByteArraySet();
        indexKeys.add(new ByteArray(0L));
        indexKeys.add(new ByteArray(1L));
        index.writeToState(new ByteArray("A"), indexKeys);

        //Populate a record only to the reverse index
        ByteArraySet revIndexKeys = new ByteArraySet();
        revIndexKeys.add(new ByteArray(2L));
        revIndexKeys.add(new ByteArray(3L));
        index.writeToState(new ByteArray("B"), revIndexKeys);


        //Flush the data
        index.flush();

        //Populate all records to both indices correctly
        index.add(new ByteArray("A"), new ByteArray(0L));
        index.add(new ByteArray("A"), new ByteArray(1L));
        index.add(new ByteArray("B"), new ByteArray(2L));
        index.add(new ByteArray("B"), new ByteArray(3L));

        //Flush the data
        index.flush();

        //The record originally written only to the index now exists in both
        Set<ByteArray> actualKeys = index.getIndexEntry(new ByteArray("A"));
        assertTrue(actualKeys.containsAll(indexKeys));

        actualKeys = index.getForeignKeys(new ByteArray(0L));
        assertNotNull(actualKeys);
        assertEquals(1, actualKeys.size());
        assertEquals(new ByteArray("A"), actualKeys.toArray(new ByteArray[1])[0]);

        actualKeys = index.getForeignKeys(new ByteArray(1L));
        assertNotNull(actualKeys);
        assertEquals(1, actualKeys.size());
        assertEquals(new ByteArray("A"), actualKeys.toArray(new ByteArray[1])[0]);


        //The record originally written only to the reverse index now exists in both
        actualKeys = index.getIndexEntry(new ByteArray("B"));
        assertTrue(actualKeys.containsAll(revIndexKeys));

        actualKeys = index.getForeignKeys(new ByteArray(2L));
        assertNotNull(actualKeys);
        assertEquals(1, actualKeys.size());
        assertEquals(new ByteArray("B"), actualKeys.toArray(new ByteArray[1])[0]);

        actualKeys = index.getForeignKeys(new ByteArray(3L));
        assertNotNull(actualKeys);
        assertEquals(1, actualKeys.size());
        assertEquals(new ByteArray("B"), actualKeys.toArray(new ByteArray[1])[0]);
    }

    @Test
    public void testAddEmpty() {
        ByteArray fk = ByteArray.toByteArray(4235);
        ByteArray pk = ByteArray.toByteArray(6237);
        index.add(fk, pk);
        assertEquals(Collections.singleton(pk), index.getIndexEntry(fk));
        assertEquals(Collections.singleton(fk), index.getForeignKeys(pk));
        verifyIndexSate();
    }

    @Test
    public void testAddPopulated() throws Exception {
        TestHelper.populateIndex(index);
        verifyPopulatedIndex();
        ByteArray fk = ByteArray.toByteArray(4235);
        ByteArray pk = ByteArray.toByteArray(6237);
        assertEquals(TestHelper.toSet(6234, 6235, 6236), index.getIndexEntry(fk));
        assertEquals(TestHelper.toSet(4236), index.getForeignKeys(pk));
        index.add(fk, pk);
        assertEquals(TestHelper.toSet(6234, 6235, 6236, 6237), index.getIndexEntry(fk));
        assertEquals(TestHelper.toSet(4235, 4236), index.getForeignKeys(pk));
        verifyIndexSate();
    }

    @Test
    public void testFlushEmpty() {
        index.flush();
        assertEquals(0, index.pendingRIWrites.size());
        assertEquals(0, index.pendingWrites.size());
        verify(state, times(1)).flush(index.indexName);
        verify(state, times(1)).flush(index.reverseIndexName);
        verifyIndexSate();
    }

    @Test
    public void testFlushPopulated() throws Exception {
        TestHelper.populateIndex(index);
        verifyPopulatedIndex();
        index.flush();
        assertEquals(0, index.pendingRIWrites.size());
        assertEquals(0, index.pendingWrites.size());
        verify(state, atLeast(1)).flush(index.indexName);
        verify(state, atLeast(1)).flush(index.reverseIndexName);
        verifyPopulatedIndex();
        verifyIndexSate();
    }

    @Test
    public void testGetIndexName() {
        assertEquals(TestHelper.PLAYLIST_JOIN_INDICES.PLAYLIST_MEDIA, index.getIndexName());
    }

    @Test
    public void testRemoveIndexConsistency() {
        // Configuration assumes index batch size of 5 records to trigger writes to state
        // Add 6 unique primary keys with 4 unique foreign keys in order to trigger only index writes
        index.add(new ByteArray("A"), new ByteArray(0L));
        index.add(new ByteArray("B"), new ByteArray(1L));
        index.add(new ByteArray("C"), new ByteArray(2L));
        index.add(new ByteArray("D"), new ByteArray(3L));
        index.add(new ByteArray("E"), new ByteArray(1L));
        index.add(new ByteArray("F"), new ByteArray(3L));

        // Flush the state
        state.flush();

        // Create a new MultiIndex object to simulate an unhealthy restart to wipe in memory, non-persisted data structures
        index = createEmptyIndex(config, state);

        // Ensure setup is correct and that we can retrieve an expected primary key from state
        Set<ByteArray> actualKeys;
        actualKeys = index.getIndexEntry(new ByteArray("A"));
        assertNotNull(actualKeys);
        assertEquals(1, actualKeys.size());
        assertEquals(new ByteArray(0L), actualKeys.toArray(new ByteArray[1])[0]);

        // Ensure setup is correct and that we have lost data in our reverse index
        actualKeys = index.getForeignKeys(new ByteArray(0L));
        assertNull(actualKeys);

        // Attempt to remove key
        index.remove(new ByteArray("A"), new ByteArray(0L));

        // Flush the index to persist all in memory data
        index.flush();

        // The key should not exist in our index anymore
        actualKeys = index.getIndexEntry(new ByteArray("A"));
        assertNull(actualKeys);

        // The foreign key should still not exist
        actualKeys = index.getForeignKeys(new ByteArray(0L));
        assertNull(actualKeys);
    }

    @Test
    public void testRemoveEmpty() {
        assertFalse(index.remove(new ByteArray("A"), new ByteArray(0L)));
        assertNull(index.getForeignKeys(new ByteArray(0L)));
        assertNull(index.getIndexEntry(new ByteArray("A")));
        verifyIndexSate();
    }

    @Test
    public void testRemovePopulated() throws Exception {
        TestHelper.populateIndex(index);
        verifyPopulatedIndex();
        assertFalse(index.remove(new ByteArray(4235), new ByteArray(6237)));
        assertTrue(index.remove(new ByteArray(4235), new ByteArray(6234)));
        assertNull(index.getForeignKeys(new ByteArray(6234)));
        assertEquals(TestHelper.toSet(4236), index.getForeignKeys(new ByteArray(6237)));
        assertEquals(TestHelper.toSet(6235, 6236), index.getIndexEntry(new ByteArray(4235)));
        verifyIndexSate();
    }

    @Test
    public void testRemoveReverseIndexConsistency() {
        //Configuration assumes index batch size of 5 records to trigger writes to state
        //Add 6 keys with less than 5 unique primary keys and 6 unique foreign keys in order to trigger only reverse index writes
        index.add(new ByteArray("A"), new ByteArray(0L));
        index.add(new ByteArray("B"), new ByteArray(1L));
        index.add(new ByteArray("B"), new ByteArray(2L));
        index.add(new ByteArray("B"), new ByteArray(3L));
        index.add(new ByteArray("C"), new ByteArray(4L));
        index.add(new ByteArray("C"), new ByteArray(5L));

        // Flush the state
        state.flush();

        // Create a new MultiIndex object to simulate an unhealthy restart to wipe in memory, non-persisted data structures
        index = createEmptyIndex(config, state);

        // Ensure setup is correct and that we have lost data in our index
        Set<ByteArray> actualKeys;
        actualKeys = index.getIndexEntry(new ByteArray("A"));
        assertNull(actualKeys);

        // Ensure setup is correct and that we can retrieve an expected foreign key from reverse index state
        actualKeys = index.getForeignKeys(new ByteArray(0L));
        assertNotNull(actualKeys);
        assertEquals(1, actualKeys.size());
        assertEquals(new ByteArray("A"), actualKeys.toArray(new ByteArray[1])[0]);

        // Attempt to remove key
        index.remove(new ByteArray("A"), new ByteArray(0L));

        // Flush the index to persist all in memory data
        index.flush();

        // The primary key should still not exist
        actualKeys = index.getIndexEntry(new ByteArray("A"));
        assertNull(actualKeys);

        // The foreign key should no longer exist
        actualKeys = index.getForeignKeys(new ByteArray(0L));
        assertNull(actualKeys);
    }

    @Test
    public void testToStringEmpty() {
        assertNotNull(index.toString());
    }

    @Test
    public void testToStringPopulated() throws Exception {
        TestHelper.populateIndex(index);
        verifyPopulatedIndex();
        assertNotNull(index.toString());
    }

    @Test
    public void testVerifyIndexStateFail() throws Exception {
        TestHelper.populateIndex(index);
        verifyPopulatedIndex();

        //Populate a record only on the reverse index
        ByteArraySet foreignKeys = new ByteArraySet();
        foreignKeys.add(new ByteArray(6L));
        foreignKeys.add(new ByteArray(7L));
        index.writeRIToState(new ByteArray("D"), foreignKeys);

        // Should find the record that was only written to the reverse index
        Set<String> missingKeys =  index.verifyIndexState();
        assertEquals(Collections.singleton(Hex.encodeHexString("D".getBytes())), missingKeys);
    }

    @Test
    public void testVerifyReverseIndexStateFail() throws Exception {
        TestHelper.populateIndex(index);
        verifyPopulatedIndex();

        //Populate a record only on the index
        ByteArraySet primaryKeys = new ByteArraySet();
        primaryKeys.add(new ByteArray(6L));
        primaryKeys.add(new ByteArray(7L));
        index.writeToState(new ByteArray("D"), primaryKeys);

        //Should find the record that was only written to the index
        Set<String> missingKeys =  index.verifyReverseIndexState();
        assertEquals(Collections.singleton(Hex.encodeHexString("D".getBytes())), missingKeys);
    }
}
