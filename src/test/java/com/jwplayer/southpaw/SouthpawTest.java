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
import com.jwplayer.southpaw.json.Record;
import com.jwplayer.southpaw.json.Relation;
import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.record.MapRecord;
import com.jwplayer.southpaw.state.RocksDBState;
import com.jwplayer.southpaw.strategy.QueueingStrategy;
import com.jwplayer.southpaw.util.ByteArray;
import com.jwplayer.southpaw.util.ByteArraySet;
import com.jwplayer.southpaw.util.FileHelper;
import com.jwplayer.southpaw.util.RelationHelper;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.mockito.internal.stubbing.answers.AnswersWithDelay;
import org.mockito.internal.stubbing.answers.Returns;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.yaml.snakeyaml.Yaml;

import java.net.URI;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class SouthpawTest {
    private Map<String, Object> config;
    private Southpaw southpaw;

    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        Yaml yaml = new Yaml();
        config = yaml.load(FileHelper.getInputStream(new URI(TestHelper.CONFIG_PATH)));
        // Override the db and backup URI's with the managed temp folder
        config.put(RocksDBState.URI_CONFIG, dbFolder.getRoot().toURI() + "/db");
        config.put(RocksDBState.BACKUP_URI_CONFIG, dbFolder.getRoot().toURI() + "/backup");

        southpaw = spy(new Southpaw(config, Collections.singletonList(new URI(TestHelper.RELATION_PATHS.PLAYLIST))));
        Southpaw.deleteBackups(config);
    }

    @After
    public void cleanup() {
        southpaw.close();
        Southpaw.deleteBackups(config);
        Southpaw.deleteState(config);
    }

    @Test
    public void testCreateDenormalizedRecordsFull() {
        doReturn(new DenormalizedRecord()).when(southpaw).createDenormalizedRecord(any(), any(), any(), any());
        Set<ByteArray> expectedPKs = Collections.emptySet();
        Set<ByteArray> inputPKs = TestHelper.toSet(1, 2, 3, 4, 5);
        southpaw.createDenormalizedRecords(southpaw.relations[0], QueueingStrategy.Priority.MEDIUM, inputPKs);
        assertEquals(expectedPKs, inputPKs);
    }

    @Test
    public void testCreateDenormalizedRecordsPartial() {
        southpaw.config.createRecordsTimeS = 1;
        doAnswer(new AnswersWithDelay(1001, new Returns(new DenormalizedRecord())))
                .when(southpaw).createDenormalizedRecord(any(), any(), any(), any());
        Set<ByteArray> expectedPKs = TestHelper.toSet(2, 3, 4, 5);
        Set<ByteArray> inputPKs = TestHelper.toSet(1, 2, 3, 4, 5);
        southpaw.createDenormalizedRecords(southpaw.relations[0], QueueingStrategy.Priority.MEDIUM, inputPKs);
        assertEquals(expectedPKs, inputPKs);
    }

    @Test
    public void testCreateInternalRecord() {
        Map<String, Comparable<?>> map = new HashMap<>();
        map.put("A", 1);
        map.put("B", false);
        map.put("C", "Badger");
        BaseRecord mapRecord = new MapRecord(map);
        Record internalRecord = southpaw.createInternalRecord(mapRecord);
        Map<String, Object> internalMap = internalRecord.getAdditionalProperties();

        for(Map.Entry<String, Comparable<?>> entry: map.entrySet()) {
            assertTrue(internalMap.containsKey(entry.getKey()));
            assertEquals(entry.getValue(), internalMap.get(entry.getKey()));
        }
    }

    @Test
    public void testCreateRecordsToBeCreatedKeyspaceHigh() throws Exception {
        Relation relation = RelationHelper.loadRelations(Collections.singletonList(new URI(TestHelper.RELATION_PATHS.PLAYLIST)))[0];
        String expected = "PK|high|DenormalizedPlaylist";
        String actual = Southpaw.createRecordsToBeCreatedKeyspace(relation, QueueingStrategy.Priority.HIGH);
        assertEquals(expected, actual);
    }

    @Test
    public void testCreateRecordsToBeCreatedKeyspaceLow() throws Exception {
        Relation relation = RelationHelper.loadRelations(Collections.singletonList(new URI(TestHelper.RELATION_PATHS.PLAYLIST)))[0];
        String expected = "PK|low|DenormalizedPlaylist";
        String actual = Southpaw.createRecordsToBeCreatedKeyspace(relation, QueueingStrategy.Priority.LOW);
        assertEquals(expected, actual);
    }

    @Test
    public void testCreateRecordsToBeCreatedKeyspaceMedium() throws Exception {
        Relation relation = RelationHelper.loadRelations(Collections.singletonList(new URI(TestHelper.RELATION_PATHS.PLAYLIST)))[0];
        String expected = "PK|DenormalizedPlaylist";
        String actual = Southpaw.createRecordsToBeCreatedKeyspace(relation, QueueingStrategy.Priority.MEDIUM);
        assertEquals(expected, actual);
    }

    @Test
    public void testHandleTimersDoNothing() {
        southpaw.backupWatch = spy(southpaw.backupWatch);
        southpaw.commitWatch = spy(southpaw.commitWatch);
        southpaw.metricsWatch = spy(southpaw.metricsWatch);
        southpaw.state = spy(southpaw.state);
        when(southpaw.backupWatch.getTime()).thenReturn(southpaw.config.backupTimeS * 1000L - 1L);
        when(southpaw.commitWatch.getTime()).thenReturn(southpaw.config.commitTimeS * 1000L - 1L);
        when(southpaw.metricsWatch.getTime()).thenReturn(southpaw.config.metricsReportTimeS * 1000L - 1L);
        southpaw.handleTimers();
        verify(southpaw, times(0)).commit();
        verify(southpaw.state, times(0)).backup();
        verify(southpaw, times(0)).reportMetrics();
        verify(southpaw.backupWatch, times(0)).reset();
        verify(southpaw.backupWatch, times(0)).start();
        verify(southpaw.commitWatch, times(0)).reset();
        verify(southpaw.commitWatch, times(0)).start();
        verify(southpaw.metricsWatch, times(0)).reset();
        verify(southpaw.metricsWatch, times(0)).start();
    }

    @Test
    public void testHandleTimersTriggerBackup() {
        southpaw.backupWatch = spy(southpaw.backupWatch);
        southpaw.state = spy(southpaw.state);
        when(southpaw.backupWatch.getTime()).thenReturn(southpaw.config.backupTimeS * 1000L + 1L);
        southpaw.handleTimers();
        verify(southpaw, times(1)).commit();
        verify(southpaw.state, times(1)).backup();
        verify(southpaw.backupWatch, times(1)).reset();
        verify(southpaw.backupWatch, times(1)).start();
    }

    @Test
    public void testHandleTimersCommit() {
        southpaw.commitWatch = spy(southpaw.commitWatch);
        southpaw.state = spy(southpaw.state);
        when(southpaw.commitWatch.getTime()).thenReturn(southpaw.config.commitTimeS * 1000L + 1L);
        southpaw.handleTimers();
        verify(southpaw, times(1)).commit();
        verify(southpaw.commitWatch, times(1)).reset();
        verify(southpaw.commitWatch, times(1)).start();
    }

    @Test
    public void testHandleTimersReportMetrics() {
        southpaw.metricsWatch = spy(southpaw.metricsWatch);
        southpaw.state = spy(southpaw.state);
        when(southpaw.metricsWatch.getTime()).thenReturn(southpaw.config.metricsReportTimeS * 1000L + 1L);
        southpaw.handleTimers();
        verify(southpaw, times(1)).reportMetrics();
        verify(southpaw.metricsWatch, times(1)).reset();
        verify(southpaw.metricsWatch, times(1)).start();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProcessDenormalizedPKs() {
        doAnswer(i -> {
            Set<ByteArray> pks = (Set<ByteArray>) i.getArguments()[2];
            if(!pks.isEmpty()) {
                pks.remove(pks.stream().findFirst().get());
            }
            return null;
        }).when(southpaw).createDenormalizedRecords(any(), any(), any());
        Map<QueueingStrategy.Priority, ByteArraySet> queues = southpaw.recordsToBeCreated.get(southpaw.relations[0]);
        queues.put(QueueingStrategy.Priority.HIGH, TestHelper.toSet(1, 2, 3, 4, 5));
        queues.put(QueueingStrategy.Priority.LOW, TestHelper.toSet(1, 2, 3, 4, 5));
        queues.put(QueueingStrategy.Priority.MEDIUM, TestHelper.toSet(1, 2, 3, 4, 5));
        southpaw.processDenormalizedPKs();
        assertEquals(Collections.emptySet(), queues.get(QueueingStrategy.Priority.HIGH));
        assertEquals(TestHelper.toSet(2, 3, 4, 5), queues.get(QueueingStrategy.Priority.LOW));
        assertEquals(Collections.emptySet(), queues.get(QueueingStrategy.Priority.MEDIUM));
    }
}
