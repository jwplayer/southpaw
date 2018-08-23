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
package com.jwplayer.southpaw.util;

import org.apache.commons.lang.NotImplementedException;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;


public class ByteArraySetTest {
    public ByteArraySet createBigSet() {
        ByteArraySet set = new ByteArraySet();
        List<ByteArray> numbers = new ArrayList<>();
        for(int i = 1; i <= 3000; i++) {
            numbers.add(new ByteArray(i));
        }
        Collections.shuffle(numbers);
        set.addAll(numbers);
        return set;
    }

    public ByteArraySet createSmallSet() {
        ByteArraySet set = new ByteArraySet();
        List<ByteArray> numbers = new ArrayList<>();
        for(int i = 1; i <= 10; i++) {
            numbers.add(new ByteArray(i));
        }
        Collections.shuffle(numbers);
        set.addAll(numbers);
        return set;
    }

    @Test
    public void add() {
        ByteArraySet set = createBigSet();
        boolean added = set.add(new ByteArray("Badger"));

        assertTrue(added);
        assertTrue(set.contains(new ByteArray("Badger")));
    }

    @Test
    public void addAll() {
        ByteArraySet set = createBigSet();
        boolean added = set.addAll(Arrays.asList(new ByteArray("Entropy"), new ByteArray("BlackHole"), new ByteArray("FireWall")));

        assertTrue(added);
        assertTrue(set.contains(new ByteArray("Entropy")));
        assertTrue(set.contains(new ByteArray("BlackHole")));
        assertTrue(set.contains(new ByteArray("FireWall")));
    }

    @Test
    public void clear() {
        ByteArraySet set = createBigSet();
        set.clear();

        assertEquals(0, set.size());
    }

    @Test
    public void contains() {
        ByteArraySet set = createBigSet();
        assertTrue(set.contains(new ByteArray(1)));
    }

    @Test
    public void containsAll() {
        ByteArraySet set = createBigSet();
        assertTrue(set.containsAll(Arrays.asList(new ByteArray(1), new ByteArray(2), new ByteArray(3))));
    }

    @Test
    public void deserialize() {
    }

    @Test
    public void isEmpty() {
        ByteArraySet set = createBigSet();
        assertFalse(set.isEmpty());
        set.clear();
        assertTrue(set.isEmpty());
    }

    @Test
    public void iterator() {
        ByteArraySet set = createBigSet();

        Iterator<ByteArray> iter = set.iterator();
        int count = 0;
        while(iter.hasNext()) {
            iter.next();
            count++;
        }
        assertEquals(3000, count);

    }

    @Test
    public void remove() {
        ByteArraySet set = createBigSet();
        set.remove(new ByteArray(10));

        assertFalse(set.contains(new ByteArray(10)));
    }

    @Test
    public void removeAll() {
        ByteArraySet set = createBigSet();
        set.removeAll(Arrays.asList(new ByteArray(10), new ByteArray(999), new ByteArray(123456)));

        assertFalse(set.contains(new ByteArray(10)));
        assertFalse(set.contains(new ByteArray(999)));
        assertFalse(set.contains(new ByteArray(123456)));
    }

    @Test
    public void serializeEmpty() {
        ByteArraySet emptySet = new ByteArraySet();
        byte[] actualBytes = emptySet.serialize();
        byte[] expectedBytes = { (byte) 0 };

        assertArrayEquals(expectedBytes, actualBytes);
    }

    @Test
    public void serializeMultiChunks() {
        ByteArraySet set = createBigSet();
        byte[] bytes = set.serialize();
        ByteArraySet deSet = ByteArraySet.deserialize(bytes);

        for(Integer i = 1; i <= 3000; i++) {
            assertTrue(i.toString(), deSet.contains(new ByteArray(i)));
        }
    }

    @Test
    public void serializeSingleChunk() {
        ByteArraySet set = createSmallSet();
        byte[] bytes = set.serialize();
        ByteArraySet deSet = ByteArraySet.deserialize(bytes);

        set.size();
        deSet.size();
        for(Integer i = 1; i <= 10; i++) {
            assertTrue(i.toString(), deSet.contains(new ByteArray(i)));
        }
    }

    @Test
    public void serializeSingleValue() {
        ByteArraySet set = new ByteArraySet();
        set.add(new ByteArray(6));
        byte[] bytes = set.serialize();
        ByteArraySet deSet = ByteArraySet.deserialize(bytes);

        assertEquals(1, set.size());
        assertEquals(1, deSet.size());
        assertTrue(deSet.contains(new ByteArray(6)));
    }

    @Test
    public void size() {
        ByteArraySet set = createBigSet();
        assertEquals(3000, set.size());
    }

    @Test(expected = NotImplementedException.class)
    public void retainAll() {
        ByteArraySet set = new ByteArraySet();
        set.retainAll(null);
    }

    @Test
    public void toArray() {
        ByteArraySet set = createBigSet();
        ByteArray[] bas = set.toArray();
        assertEquals(3000, bas.length);
        set.remove(new ByteArray(1));
        bas = set.toArray();
        assertEquals(2999, bas.length);
    }
}
