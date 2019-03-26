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
package com.jwplayer.southpaw.state;

import com.jwplayer.southpaw.util.ByteArray;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class RocksDBStateTest {
    private static final String KEY_SPACE = "Default";

    private RocksDBState state;
    private String dbUri;
    private String backupUri;

    private static Map<String, Object> createConfig(String dbUri, String backupUri) {
        Map<String, Object> config = new HashMap<>();
        config.put(RocksDBState.BACKUP_URI_CONFIG, backupUri);
        config.put(RocksDBState.BACKUPS_TO_KEEP_CONFIG, 5);
        config.put(RocksDBState.COMPACTION_READ_AHEAD_SIZE_CONFIG, 1048675);
        config.put(RocksDBState.MEMTABLE_SIZE, 1048675);
        config.put(RocksDBState.PARALLELISM_CONFIG, 4);
        config.put(RocksDBState.PUT_BATCH_SIZE, 5);
        config.put(RocksDBState.URI_CONFIG, dbUri);
        return config;
    }

    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();

    @Rule
    public TemporaryFolder backupFolder = new TemporaryFolder();

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        dbUri = dbFolder.getRoot().toURI().toString();
        backupUri = backupFolder.getRoot().toURI().toString();
        state = new RocksDBState();
    }

    @After
    public void tearDown() {
        state.close();
        state.delete();
    }

    @Test
    public void backupAndRestore() {
        Map<String, Object> config = createConfig(dbUri, backupUri);
        testBackupAndRestore(config);
    }

    @Test
    public void backupAndRestoreAutoRollback() {
        Map<String, Object> config = createConfig(dbUri, backupUri);
        config.put(RocksDBState.BACKUPS_AUTO_ROLLBACK_CONFIG, true);
        testBackupAndRestore(config);
    }

    private void testBackupAndRestore(Map<String, Object> config) {
        state.configure(config);
        state.createKeySpace(KEY_SPACE);
        writeData(0,100);

        state.deleteBackups();
        state.backup();
        state.close();

        state.restore();
        BaseState.Iterator iter = state.iterate(KEY_SPACE);
        Integer count = 0;
        while (iter.hasNext()) {
            AbstractMap.SimpleEntry<byte[], byte[]> pair = iter.next();
            assertEquals(new ByteArray(count), new ByteArray(pair.getKey()));
            assertEquals(count.toString(), new String(pair.getValue()));
            count++;
        }
        assertEquals(100, (int) count);
        state.deleteBackups();
    }

    @Test
    public void backupAndRestoreCorrupt() throws URISyntaxException, IOException {
        // Expected exception on corrupted backup
        thrown.expect( RuntimeException.class );
        thrown.expectMessage("org.rocksdb.RocksDBException: Checksum check failed");

        state.configure(createConfig(dbUri, backupUri));
        state.createKeySpace(KEY_SPACE);
        writeData(0,100);

        state.deleteBackups();
        state.backup();
        state.close();

        corruptLatestSST();

        state.restore();
    }

    @Test
    public void BackupAndRestoreLatestCorruptAutoRollback() throws URISyntaxException, IOException {
        Map<String, Object> config = createConfig(dbUri, backupUri);
        config.put(RocksDBState.BACKUPS_AUTO_ROLLBACK_CONFIG, true);
        state.configure(config);
        state.createKeySpace(KEY_SPACE);

        state.deleteBackups();

        // Create a few backups
        writeData(0,100);
        state.backup();
        writeData(100,200);
        state.backup();
        writeData(200,250);
        state.backup();
        state.close();

        //Corrupt the most recent backup
        corruptLatestSST();

        state.restore();

        //Check that the second backup restores with the expected data
        BaseState.Iterator iter = state.iterate(KEY_SPACE);
        Integer count = 0;
        while (iter.hasNext()) {
            AbstractMap.SimpleEntry<byte[], byte[]> pair = iter.next();
            assertEquals(new ByteArray(count), new ByteArray(pair.getKey()));
            assertEquals(count.toString(), new String(pair.getValue()));
            count++;
        }
        iter.close();
        assertEquals(200, (int) count);

        //Write more data and backup
        writeData(200,250);
        state.backup();
        state.close();

        state.restore();

        //Check that the newest backup contains the data from the last successfully restored backup and the new data
        iter = state.iterate(KEY_SPACE);
        count = 0;
        while (iter.hasNext()) {
            AbstractMap.SimpleEntry<byte[], byte[]> pair = iter.next();
            assertEquals(new ByteArray(count), new ByteArray(pair.getKey()));
            assertEquals(count.toString(), new String(pair.getValue()));
            count++;
        }
        iter.close();

        //Check that all expected data exists
        assertEquals(250, (int) count);

        state.deleteBackups();
    }

    @Test
    public void backupAndRestoreAllCorruptedAutoRollback() throws URISyntaxException, IOException {
        // Expected exception on corrupted backup
        thrown.expect( RuntimeException.class );
        thrown.expectMessage("org.rocksdb.RocksDBException: Checksum check failed");

        Map<String, Object> config = createConfig(dbUri, backupUri);
        config.put(RocksDBState.BACKUPS_AUTO_ROLLBACK_CONFIG, true);
        state.configure(config);
        state.createKeySpace(KEY_SPACE);

        state.deleteBackups();

        writeData(0,100);
        state.backup();
        state.close();

        //Corrupt the most recent backup
        corruptLatestSST();

        state.restore();
    }

    @Test
    public void configureRestoreNever() {
        RocksDBState spyState = spy(state);

        Map<String, Object> config = createConfig(dbUri, backupUri);
        config.put(RocksDBState.RESTORE_MODE_CONFIG, "never");
        spyState.configure(config);
        spyState.createKeySpace(KEY_SPACE);

        // The database should be open
        assertTrue(spyState.isOpen());

        // And we should not have tried to restore
        verify(spyState, never()).restore();
    }

    @Test
    public void configureRestoreWhenNeededNoLocalDB() {
        Map<String, Object> config = createConfig(dbUri, backupUri);
        config.put(RocksDBState.RESTORE_MODE_CONFIG, "when_needed");

        RocksDBState spyState = spy(state);

        spyState.configure(config);
        spyState.createKeySpace(KEY_SPACE);

        // The database should be open
        assertTrue(spyState.isOpen());

        // We should have attempted to restore
        verify(spyState, times(1)).restore();
    }

    @Test
    public void configureRestoreWhenNeededWithLocalDB() {
        Map<String, Object> config = createConfig(dbUri, backupUri);
        config.put(RocksDBState.RESTORE_MODE_CONFIG, "when_needed");

        state.configure(config);
        state.createKeySpace(KEY_SPACE);
        writeData(0,100);
        state.close();

        state = new RocksDBState();

        RocksDBState spyState = spy(state);
        spyState.configure(config);
        spyState.createKeySpace(KEY_SPACE);

        // The database should be open
        assertTrue(spyState.isOpen());

        // We should not have attempted to restore
        verify(spyState, never()).restore();
    }

    @Test
    public void close() {
        state.configure(createConfig(dbUri, backupUri));
        state.createKeySpace(KEY_SPACE);
        writeData(0,100);

        state.close();
    }

    @Test
    public void createKeySpace() {
        state.configure(createConfig(dbUri, backupUri));

        String newKeySpace = "NewKeySpace";
        state.createKeySpace(newKeySpace);
        state.put(newKeySpace, "A".getBytes(), "B".getBytes());
        state.flush();
        String value = new String(state.get(newKeySpace, "A".getBytes()));
        assertEquals("B", value);
    }

    @Test
    public void delete() {
        state.configure(createConfig(dbUri, backupUri));
        state.createKeySpace(KEY_SPACE);
        writeData(0,100);
        state.close();

        state.delete();
    }

    @Test
    public void deleteWithDBOpen() {
        thrown.expect( RuntimeException.class );
        thrown.expectMessage("RocksDB is currently open. Must call close() first.");

        state.configure(createConfig(dbUri, backupUri));
        state.delete();
    }

    @Test
    public void deleteValue() {
        state.configure(createConfig(dbUri, backupUri));
        state.createKeySpace(KEY_SPACE);

        byte[] key = new ByteArray(1).getBytes();
        state.delete(KEY_SPACE, key);
        byte[] value = state.get(KEY_SPACE, key);

        assertNull(value);
    }

    @Test
    public void flush() {
        state.configure(createConfig(dbUri, backupUri));
        state.createKeySpace(KEY_SPACE);

        state.put(KEY_SPACE, "AA".getBytes(), "B".getBytes());
        String value = new String(state.get(KEY_SPACE, "AA".getBytes()));
        assertEquals("B", value);
        state.flush();
        value = new String(state.get(KEY_SPACE, "AA".getBytes()));
        assertEquals("B", value);
    }

    @Test
    public void flushKeySpace() {
        state.configure(createConfig(dbUri, backupUri));
        state.createKeySpace(KEY_SPACE);

        state.put(KEY_SPACE, "AA".getBytes(), "B".getBytes());
        String value = new String(state.get(KEY_SPACE, "AA".getBytes()));
        assertEquals("B", value);
        state.flush(KEY_SPACE);
        value = new String(state.get(KEY_SPACE, "AA".getBytes()));
        assertEquals("B", value);
    }

    @Test
    public void get() {
        state.configure(createConfig(dbUri, backupUri));
        state.createKeySpace(KEY_SPACE);
        writeData(0,100);

        String value = new String(state.get(KEY_SPACE, new ByteArray(1).getBytes()));
        assertEquals("1", value);
    }

    @Test
    public void iterate() {
        state.configure(createConfig(dbUri, backupUri));
        state.createKeySpace(KEY_SPACE);
        writeData(0,100);

        BaseState.Iterator iter = state.iterate(KEY_SPACE);
        Integer count = 0;
        while(iter.hasNext()) {
            AbstractMap.SimpleEntry<byte[], byte[]> pair = iter.next();
            assertEquals(new ByteArray(count), new ByteArray(pair.getKey()));
            assertEquals(count.toString(), new String(pair.getValue()));
            count++;
        }
        iter.close();
        assertEquals(100, (int) count);
    }

    @Test
    public void put() {
        state.configure(createConfig(dbUri, backupUri));
        state.createKeySpace(KEY_SPACE);

        state.put(KEY_SPACE, "A".getBytes(), "B".getBytes());
        state.flush(KEY_SPACE);
        String value = new String(state.get(KEY_SPACE, "A".getBytes()));
        assertEquals("B", value);
    }

    private void corruptLatestSST() throws URISyntaxException, IOException {
        Path dir = Paths.get(new URI(backupFolder.getRoot().toURI().toString() + "/shared"));
        Optional<Path> lastFilePath = Files.list(dir)
                .filter(f -> !Files.isDirectory(f))
                .max(Comparator.naturalOrder());

        Files.write(lastFilePath.get(), "garbage".getBytes(), StandardOpenOption.APPEND);
    }

    private void writeData(int start, int end) {
        for(Integer i = start; i < end; i++) {
            state.put(KEY_SPACE, new ByteArray(i).getBytes(), i.toString().getBytes());
        }
        state.flush();
    }
}
