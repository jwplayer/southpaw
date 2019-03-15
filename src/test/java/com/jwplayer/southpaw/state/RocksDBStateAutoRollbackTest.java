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
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class RocksDBStateAutoRollbackTest {
    private static final String ROCKSDB_BASE_URI = "file:///tmp/RocksDB/";

    protected static final String BACKUP_URI = "file:///tmp/RocksDB/RocksDBStateTestBackup";
    protected static final String KEY_SPACE = "Default";
    protected static final String URI = "file:///tmp/RocksDB/RocksDBStateTest";
    protected static final int BACKUPS_TO_KEEP = 4;

    protected RocksDBState state;

    public static Map<String, Object> createConfig(String uri) {
        Map<String, Object> config = new HashMap<>();
        config.put(RocksDBState.BACKUP_URI_CONFIG, BACKUP_URI);
        config.put(RocksDBState.BACKUPS_TO_KEEP_CONFIG, BACKUPS_TO_KEEP);
        config.put(RocksDBState.COMPACTION_READ_AHEAD_SIZE_CONFIG, 1048675);
        config.put(RocksDBState.MEMTABLE_SIZE, 1048675);
        config.put(RocksDBState.PARALLELISM_CONFIG, 4);
        config.put(RocksDBState.PUT_BATCH_SIZE, 5);
        config.put(RocksDBState.URI_CONFIG, uri);
        config.put(RocksDBState.BACKUPS_AUTO_ROLLBACK_CONFIG, true);
        return config;
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void classSetup() throws URISyntaxException, IOException {
        File folder = new File(new URI(ROCKSDB_BASE_URI));
        FileUtils.deleteDirectory(folder);
        folder.mkdirs();
    }

    @AfterClass
    public static void classCleanup() throws URISyntaxException, IOException {
        FileUtils.deleteDirectory(new File(new URI(ROCKSDB_BASE_URI)));
    }

    @Before
    public void setUp() {
        state = new RocksDBState();
        state.configure(createConfig(URI));
        state.createKeySpace(KEY_SPACE);
    }

    @After
    public void tearDown() {
        state.delete();
    }

    @Test
    public void backupAndRestore() {
        state.deleteBackups();
        writeData(0,100);
        state.backup();
        state.restore();
        BaseState.Iterator iter = state.iterate(KEY_SPACE);
        Integer count = 0;
        while (iter.hasNext()) {
            AbstractMap.SimpleEntry<byte[], byte[]> pair = iter.next();
            assertEquals(new ByteArray(count), new ByteArray(pair.getKey()));
            assertEquals(count.toString(), new String(pair.getValue()));
            count++;
        }
        iter.close();
        assertEquals(100, (int) count);
        state.deleteBackups();
    }

    @Test
    public void BackupAndRestoreLatestCorrupt() throws URISyntaxException, IOException {
        state.deleteBackups();

        // Create a few backups
        writeData(0,100);
        state.backup();
        writeData(100,200);
        state.backup();
        writeData(200,250);
        state.backup();

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
    public void backupAndRestoreAllCorrupted() throws URISyntaxException, IOException {
        // Expected exception on corrupted backup
        thrown.expect( RuntimeException.class );
        thrown.expectMessage("org.rocksdb.RocksDBException: Checksum check failed");

        state.deleteBackups();

        writeData(0,100);
        state.backup();

        //Corrupt the most recent backup
        corruptLatestSST();

        state.restore();
    }

    private void corruptLatestSST() throws URISyntaxException, IOException {
        Path dir = Paths.get(new URI(BACKUP_URI + "/shared"));
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
