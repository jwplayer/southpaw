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

import com.jwplayer.southpaw.util.ByteArray;
import org.apache.commons.lang.NotImplementedException;
import org.junit.Before;
import org.junit.Test;

public class BlackHoleTopicTest {
    public BlackHoleTopic<Object, Object> topic;

    @Before
    public void setUp() {
        topic = new BlackHoleTopic<>();
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
        topic.getCurrentOffsets();
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
        topic.resetCurrentOffsets();
    }

    @Test
    public void write() {
        topic.write(null, null);
    }
}
