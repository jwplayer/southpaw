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

import com.jwplayer.southpaw.state.BaseState;
import com.jwplayer.southpaw.state.RocksDBState;
import com.jwplayer.southpaw.state.RocksDBStateTest;
import com.jwplayer.southpaw.util.ByteArray;
import org.apache.commons.lang.NotImplementedException;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;


public class ConsoleTopicTest {
    BaseState state;
    public ConsoleTopic<String, String> topic;

    @Before
    public void setUp() {
        topic = new ConsoleTopic<>();
        Map<String, Object> config = RocksDBStateTest.createConfig("file:///tmp/RocksDB/ConsoleTopicTest");
        state = new RocksDBState();
        state.configure(config);
        topic.configure("TestTopic", config, state, Serdes.String(), Serdes.String());
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
