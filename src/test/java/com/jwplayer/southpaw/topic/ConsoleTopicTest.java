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
package com.jwplayer.southpaw.topic;

import com.jwplayer.southpaw.filter.DefaultFilter;
import com.jwplayer.southpaw.state.BaseState;
import com.jwplayer.southpaw.state.RocksDBState;
import com.jwplayer.southpaw.state.RocksDBStateTest;
import com.jwplayer.southpaw.util.ByteArray;
import org.apache.commons.lang.NotImplementedException;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.*;
import org.junit.rules.TestName;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;


public class ConsoleTopicTest {
    private static final String ROCKSDB_BASE_URI = "file:///tmp/RocksDB/";

    BaseState state;
    public ConsoleTopic<String, String> topic;

    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static void classSetup() throws URISyntaxException {
        File folder = new File(new URI(ROCKSDB_BASE_URI));
        folder.mkdirs();
    }

    @AfterClass
    public static void classCleanup() throws URISyntaxException {
        File folder = new File(new URI(ROCKSDB_BASE_URI));
        folder.delete();
    }

    @Before
    public void setUp() {
        topic = new ConsoleTopic<>();
        Map<String, Object> config = RocksDBStateTest.createConfig(ROCKSDB_BASE_URI + testName);
        state = new RocksDBState();
        state.configure(config);
        topic.configure("TestTopic", config, state, Serdes.String(), Serdes.String(), new DefaultFilter());
    }

    @After
    public void tearDown() {
        state.delete();
    }

    @Test
    public void testCommit() {
        topic.commit();
    }

    @Test
    public void testFlush() {
        topic.flush();
    }

    @Test(expected = NotImplementedException.class)
    public void testGetCurrentOffset() {
        topic.getCurrentOffset();
    }

    @Test(expected = NotImplementedException.class)
    public void testGetLag() {
        topic.getLag();
    }

    @Test(expected = NotImplementedException.class)
    public void testReadByPK() {
        topic.readByPK(new ByteArray(1));
    }

    @Test(expected = NotImplementedException.class)
    public void testReadNext() {
        topic.readNext();
    }

    @Test(expected = NotImplementedException.class)
    public void testResetCurrentOffset() {
        topic.resetCurrentOffset();
    }

    @Test
    public void write() {
        topic.write("A", "B");
    }
}
