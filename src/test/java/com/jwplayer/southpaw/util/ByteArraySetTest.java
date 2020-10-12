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
import org.apache.commons.lang.RandomStringUtils;
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

    public List<ByteArray> getRandomByteArrays(int count) {
        List<ByteArray> vals = new ArrayList<>();
        while(vals.size() < count) {
            String val = RandomStringUtils.randomAlphanumeric(6);
            if (!vals.contains(val)) {
                vals.add(new ByteArray(val));
            }
        }
        Collections.shuffle(vals);
        return vals;
    }

    public void testAdd(int size) {
        ByteArraySet set = new ByteArraySet();

        List<ByteArray> vals = getRandomByteArrays(size);

        List<ByteArray> insertedVals = new ArrayList<>();
        for (ByteArray val: vals) {
            assertTrue(set.add(val));
            assertTrue(set.contains(val));
            insertedVals.add(val);
            assertEquals(insertedVals.size(), set.size());
        }

        for (ByteArray val: vals) {
            assertFalse(set.add(val));
            assertTrue(set.contains(val));
        }
        assertEquals(size, set.size());

        for (ByteArray val: vals) {
            assertTrue(set.contains(val));
        }
    }

    @Test
    public void emptyAdd() {
        testAdd(0);
    }

    @Test
    public void reallySmallAdd() {
        testAdd(1);
    }

    @Test
    public void smallAdd() {
        testAdd(2);
    }

    @Test
    public void regularAdd() {
        testAdd(150);
    }

    @Test
    public void bigAdd() {
        testAdd(750);
    }

    @Test
    public void reallyBigAdd() {
        testAdd(3567);
    }

    public void testAddAll(int size) {
        ByteArraySet set = new ByteArraySet();

        List<ByteArray> vals = getRandomByteArrays(size);

        for (ByteArray val: vals) {
            assertTrue(set.add(val));
        }
        assertEquals(size, set.size());

        List<ByteArray> stagedVals = getRandomByteArrays(size);
        Set<ByteArray> extraVals = new HashSet<ByteArray>();
        while(extraVals.size() < size) {
            if (stagedVals.size() == 0) {
                stagedVals = getRandomByteArrays(size);
            }
            ByteArray val = stagedVals.get(0);
            stagedVals.remove(0);
            if (!vals.contains(val) && !extraVals.contains(val)) {
                extraVals.add(val);
            }
        }

        set.addAll(extraVals);
        assertEquals(2 * size, set.size());

        for (ByteArray val: vals) {
            assertTrue(set.contains(val));
        }
        for (ByteArray val: extraVals) {
            assertTrue(set.contains(val));
        }
        for (ByteArray val: set) {
            assertTrue(vals.contains(val) || extraVals.contains(val));
        }
    }

    @Test
    public void emptyAddAll() {
        testAddAll(0);
    }

    @Test
    public void reallySmallAddAll() {
        testAddAll(1);
    }

    @Test
    public void smallAddAll() {
        testAddAll(2);
    }

    @Test
    public void regularAddAll() {
        testAddAll(150);
    }

    @Test
    public void bigAddAll() {
        testAddAll(750);
    }

    @Test
    public void reallyBigAddAll() {
        testAddAll(3567);
    }

    public void testSimilarAdd(boolean forceMerger) {
        ByteArraySet set = new ByteArraySet();

        List<ByteArray> vals = new ArrayList<>();
        vals.add(new ByteArray("bytear"));
        vals.add(new ByteArray("ByteAr"));
        vals.add(new ByteArray("bytea0"));
        vals.add(new ByteArray("0ytear"));

        List<ByteArray> insertedVals = new ArrayList<>();
        for (ByteArray val: vals) {
            if (forceMerger) {
                set.serialize();
            }
            assertTrue(set.add(val));
            assertTrue(set.contains(val));
            insertedVals.add(val);
            assertEquals(insertedVals.size(), set.size());
        }

        assertEquals(4, set.size());

        for (ByteArray val: vals) {
            assertTrue(set.contains(val));
        }
    }

    @Test
    public void testMergedSimilarAdd() {
        testSimilarAdd(true);
    }

    @Test
    public void testUnmergedSimilarAdd() {
        testSimilarAdd(false);
    }

    public void testSerializeDeserialize(int size, byte leadingByte) {
        ByteArraySet set = new ByteArraySet();

        List<ByteArray> vals = getRandomByteArrays(size);

        for (ByteArray val: vals) {
            assertTrue(set.add(val));
        }
        if (size > 0) {
            assertFalse(set.isEmpty());
        } else {
            assertTrue(set.isEmpty());
        }
        Collections.shuffle(vals);

        byte[] bytes = set.serialize();
        assertEquals(leadingByte, bytes[0]);
        ByteArraySet deSet = ByteArraySet.deserialize(bytes);

        for(ByteArray val: vals) {
            assertTrue(deSet.contains(val));
        }
        assertEquals(size, deSet.size());

        int count = 0;
        for(ByteArray val: deSet) {
            assertTrue(vals.contains(val));
            count++;
        }
        assertEquals(size, count);
    }

    @Test
    public void emptySerializeDeserialize() {
        testSerializeDeserialize(0, (byte) 0);
    }

    @Test
    public void reallySmallSerializeDeserialize() {
        testSerializeDeserialize(1, (byte) 1);
    }

    @Test
    public void smallSerializeDeserialize() {
        testSerializeDeserialize(2, (byte) 2);
    }

    @Test
    public void regularSerializeDeserialize() {
        testSerializeDeserialize(150, (byte) 2);
    }

    @Test
    public void bigSerializeDeserialize() {
        testSerializeDeserialize(750, (byte) 3);
    }

    @Test
    public void reallyBigSerializeDeserialize() {
        testSerializeDeserialize(3567, (byte) 3);
    }

    public void testIterator(int size) {
        ByteArraySet set = new ByteArraySet();

        List<ByteArray> vals = getRandomByteArrays(size);

        for (ByteArray val: vals) {
            assertTrue(set.add(val));
        }
        if (size > 0) {
            assertFalse(set.isEmpty());
        } else {
            assertTrue(set.isEmpty());
        }
        assertEquals(size, set.size());

        Iterator<ByteArray> iter = set.iterator();
        List<ByteArray> alreadySeenVals = new ArrayList<>();
        int count = 0;
        while(iter.hasNext()) {
            ByteArray val = iter.next();
            assertTrue(!alreadySeenVals.contains(val));
            alreadySeenVals.add(val);
            assertTrue(vals.contains(val));
            count++;
        }
        assertEquals(size, count);
    }

    @Test
    public void emptyIterator() {
        testIterator(0);
    }

    @Test
    public void reallySmallIterator() {
        testIterator(1);
    }

    @Test
    public void smallIterator() {
        testIterator(2);
    }

    @Test
    public void regularIterator() {
        testIterator(150);
    }

    @Test
    public void bigIterator() {
        testIterator(750);
    }

    @Test
    public void reallyBigIterator() {
        testIterator(3567);
    }

    public void testRemove(int size) {
        ByteArraySet set = new ByteArraySet();

        List<ByteArray> vals = getRandomByteArrays(size);

        for (ByteArray val: vals) {
            assertTrue(set.add(val));
        }
        if (size > 0) {
            assertFalse(set.isEmpty());
        } else {
            assertTrue(set.isEmpty());
        }
        Collections.shuffle(vals);

        int count = 0;
        for(ByteArray val: set) {
            assertTrue(vals.contains(val));
            count++;
        }
        assertEquals(size, count);

        int currentSize = size;
        for(ByteArray val: vals) {
            assertTrue(set.remove(val));
            assertFalse(set.contains(val));
            currentSize -= 1;
            assertEquals(currentSize, set.size());
        }
    }

    @Test
    public void emptyRemove() {
        testRemove(0);
    }

    @Test
    public void reallySmallRemove() {
        testRemove(1);
    }

    @Test
    public void smallRemove() {
        testRemove(2);
    }

    @Test
    public void regularRemove() {
        testRemove(150);
    }

    @Test
    public void bigRemove() {
        testRemove(750);
    }

    @Test
    public void reallyBigRemove() {
        testRemove(3567);
    }

    public void testSimilarRemove(boolean forceMerger) {
        ByteArraySet set = new ByteArraySet();

        List<ByteArray> vals = new ArrayList<>();
        vals.add(new ByteArray("bytear"));
        vals.add(new ByteArray("ByteAr"));
        vals.add(new ByteArray("bytea0"));
        vals.add(new ByteArray("0ytear"));

        for (ByteArray val: vals) {
            assertTrue(set.add(val));
        }
        assertEquals(4, set.size());
        Collections.shuffle(vals);

        if (forceMerger) {
            set.serialize();
        }

        List<ByteArray> deletedVals = new ArrayList<>();
        for(ByteArray val: vals) {
            assertTrue(set.remove(val));
            assertFalse(set.contains(val));

            deletedVals.add(val);
            assertEquals(4, set.size() + deletedVals.size());
        }
    }

    @Test
    public void testMergedSimilarRemove() {
        testSimilarRemove(true);
    }

    @Test
    public void testUnmergedSimilarRemove() {
        testSimilarRemove(false);
    }

    public void testToArray(int size) {
        ByteArraySet set = new ByteArraySet();

        List<ByteArray> vals = getRandomByteArrays(size);

        for (ByteArray val: vals) {
            assertTrue(set.add(val));
        }
        if (size > 0) {
            assertFalse(set.isEmpty());
        } else {
            assertTrue(set.isEmpty());
        }

        List<ByteArray> alreadySeenVals = new ArrayList<>();
        int seenCount = 0;
        for(ByteArray val: set.toArray()) {
            assertFalse(alreadySeenVals.contains(val));
            alreadySeenVals.add(val);
            assertTrue(vals.contains(val));
            seenCount++;
        }
        assertEquals(size, seenCount);
    }

    @Test
    public void emptyToArray() {
        testToArray(0);
    }

    @Test
    public void reallySmallToArray() {
        testToArray(1);
    }

    @Test
    public void smallToArray() {
        testToArray(2);
    }

    @Test
    public void regularToArray() {
        testToArray(150);
    }

    @Test
    public void bigToArray() {
        testToArray(750);
    }

    @Test
    public void reallyBigToArray() {
        testToArray(3567);
    }

    public void testContains(int size) {
        ByteArraySet set = new ByteArraySet();

        List<ByteArray> vals = getRandomByteArrays(size);

        for (ByteArray val: vals) {
            assertTrue(set.add(val));
        }
        if (size > 0) {
            assertFalse(set.isEmpty());
        } else {
            assertTrue(set.isEmpty());
        }
        Collections.shuffle(vals);

        for(ByteArray val: vals) {
            assertTrue(set.contains(val));
        }

        List<ByteArray> absentVals = getRandomByteArrays(10);
        for(ByteArray val: absentVals) {
            if (vals.contains(val)) {
                assertTrue(set.contains(val));
            } else {
                assertFalse(set.contains(val));
            }
        }
    }

    @Test
    public void emptyContains() {
        testContains(0);
    }

    @Test
    public void reallySmallContains() {
        testContains(1);
    }

    @Test
    public void smallContains() {
        testContains(2);
    }

    @Test
    public void regularContains() {
        testContains(150);
    }

    @Test
    public void bigContains() {
        testContains(750);
    }

    @Test
    public void reallyBigContains() {
        testContains(3567);
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
