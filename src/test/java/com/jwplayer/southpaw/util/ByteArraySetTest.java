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

import com.jwplayer.southpaw.util.ByteArraySet.Chunk;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.Collectors;

import static org.junit.Assert.*;


public class ByteArraySetTest {
    private static final int RANDOM_STRING_SIZE = 6;
    private static final long RANDOM_SEED = new Random().nextLong();

    /*
     * The below variable corresponds to the maximum number of Byte Arrays a single
     * chunk can contain. One is added to the divisor as Byte Arrays are preceded
     * by their size in chunks.
     */
    private static final int PER_CHUNK_MAX_BYTE_ARRAY_COUNT = (int) Math.floor(Chunk.MAX_CHUNK_SIZE / (1 + RANDOM_STRING_SIZE));

    private static final int SIZE_EMPTY = 0;
    private static final int SIZE_REALLY_SMALL = 1;
    private static final int SIZE_SMALL = 2;
    /*
     * Regular size corresponds to data fitting in one single chunk.
     */
    private static final int SIZE_REGULAR = Math.min((int) Math.floor(ByteArraySet.MAX_FRONTING_SET_SIZE / 7), PER_CHUNK_MAX_BYTE_ARRAY_COUNT);
    /*
     * Big and really big sizes correspond to data not fitting in one single chunk.
     */
    private static final int SIZE_BIG = 2 * PER_CHUNK_MAX_BYTE_ARRAY_COUNT;
    private static final int SIZE_REALLY_BIG = ByteArraySet.MAX_FRONTING_SET_SIZE + 2 * PER_CHUNK_MAX_BYTE_ARRAY_COUNT;

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

    public List<ByteArray> getRandomByteArrays(int count, long seed) {
        System.out.println("Generating random " + count + " Byte Array(s) with " + seed + " seed");
        Random randomSeed = new Random(seed);
        List<ByteArray> vals = Stream.generate(() -> RandomStringUtils.random(RANDOM_STRING_SIZE, 0, 0, true, true, null, randomSeed))
                .limit(count)
                .collect(Collectors.toSet())
                .stream()
                .map(str -> new ByteArray(str.getBytes()))
                .collect(Collectors.toList());
        assertEquals(count, vals.size());
        return vals;
    }

    public List<ByteArray> getRandomByteArrays(int count) {
        return getRandomByteArrays(count, RANDOM_SEED);
    }

    private void testAdd(int size) {
        System.out.println("Testing add method with " + size + " size");
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
    }

    @Test
    public void emptyAdd() {
        testAdd(SIZE_EMPTY);
    }

    @Test
    public void reallySmallAdd() {
        testAdd(SIZE_REALLY_SMALL);
    }

    @Test
    public void smallAdd() {
        testAdd(SIZE_SMALL);
    }

    @Test
    public void regularAdd() {
        testAdd(SIZE_REGULAR);
    }

    @Test
    public void bigAdd() {
        testAdd(SIZE_BIG);
    }

    @Test
    public void reallyBigAdd() {
        testAdd(SIZE_REALLY_BIG);
    }

    private long getDifferentRandomSeed() {
        long differentRandomSeed = RANDOM_SEED;
        while (differentRandomSeed == RANDOM_SEED ) {
            differentRandomSeed = new Random().nextLong();
        }
        return differentRandomSeed;
    }

    private void testAddAll(int size, boolean forceByteArraySet) {
        System.out.println("Testing addAll method with " + size + " size and " + forceByteArraySet + " forceByteArraySet");
        ByteArraySet set = new ByteArraySet();

        List<ByteArray> vals = getRandomByteArrays(size);

        for (ByteArray val: vals) {
            assertTrue(set.add(val));
        }
        assertEquals(size, set.size());

        List<ByteArray> stagedVals = getRandomByteArrays(size, getDifferentRandomSeed());
        Set<ByteArray> extraVals;
        if (forceByteArraySet) {
            extraVals = new ByteArraySet();
        } else {
            extraVals = new HashSet<ByteArray>();
        }
        while(extraVals.size() < size) {
            if (stagedVals.size() == 0) {
                stagedVals = getRandomByteArrays(size, getDifferentRandomSeed());
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
        testAddAll(0, false);
        testAddAll(0, true);
    }

    @Test
    public void reallySmallAddAll() {
        testAddAll(1, false);
        testAddAll(1, true);
    }

    @Test
    public void smallAddAll() {
        testAddAll(2, false);
        testAddAll(2, true);
    }

    @Test
    public void regularAddAll() {
        testAddAll(SIZE_REGULAR, false);
        testAddAll(SIZE_REGULAR, true);
    }

    @Test
    public void bigAddAll() {
        testAddAll(SIZE_BIG, false);
        testAddAll(SIZE_BIG, true);
    }

    @Test
    public void reallyBigAddAll() {
        testAddAll(SIZE_REALLY_BIG, false);
        testAddAll(SIZE_REALLY_BIG, true);
    }

    private void testSimilarAdd(boolean forceMerger) {
        System.out.println("Testing add method for similar inputs with " + forceMerger + " forceMerger");
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

    private void testSerializeDeserialize(int size, byte leadingByte) {
        System.out.println("Testing serialize/deserialize methods with " + size + " size and " + leadingByte + " leading byte");
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
        testSerializeDeserialize(SIZE_REGULAR, (byte) 2);
    }

    @Test
    public void bigSerializeDeserialize() {
        testSerializeDeserialize(SIZE_BIG, (byte) 3);
    }

    @Test
    public void reallyBigSerializeDeserialize() {
        testSerializeDeserialize(SIZE_REALLY_BIG, (byte) 3);
    }

    private void testIterator(int size) {
        System.out.println("Testing iterator method with " + size + " size");
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
            assertFalse(alreadySeenVals.contains(val));
            alreadySeenVals.add(val);
            assertTrue(vals.contains(val));
            count++;
        }
        assertEquals(size, count);
    }

    @Test
    public void emptyIterator() {
        testIterator(SIZE_EMPTY);
    }

    @Test
    public void reallySmallIterator() {
        testIterator(SIZE_REALLY_SMALL);
    }

    @Test
    public void smallIterator() {
        testIterator(SIZE_SMALL);
    }

    @Test
    public void regularIterator() {
        testIterator(SIZE_REGULAR);
    }

    @Test
    public void bigIterator() {
        testIterator(SIZE_BIG);
    }

    @Test
    public void reallyBigIterator() {
        testIterator(SIZE_REALLY_BIG);
    }

    private void testRemove(int size) {
        System.out.println("Testing remove method with " + size + " size");
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
        testRemove(SIZE_EMPTY);
    }

    @Test
    public void reallySmallRemove() {
        testRemove(SIZE_REALLY_SMALL);
    }

    @Test
    public void smallRemove() {
        testRemove(SIZE_SMALL);
    }

    @Test
    public void regularRemove() {
        testRemove(SIZE_REGULAR);
    }

    @Test
    public void bigRemove() {
        testRemove(SIZE_BIG);
    }

    @Test
    public void reallyBigRemove() {
        testRemove(SIZE_REALLY_BIG);
    }

    private void testRandomSerializeDerializeSizeCheckRemove(int size) {
        System.out.println("Testing serialize/deserialize methods with random-size check with " + size + " size");
        ByteArraySet set = new ByteArraySet();

        List<ByteArray> originalVals = getRandomByteArrays(size);

        List<ByteArray> vals = new ArrayList<>();
        for (ByteArray val: originalVals) {
            vals.add(val);
        }
        Collections.shuffle(vals);

        int forceMergerSize = (new Random()).nextInt(size);
        List<String> insertedStringVals = new ArrayList<>();
        for (ByteArray val: vals) {
            insertedStringVals.add(new String(val.getBytes()));
            assertTrue(set.add(val));
            if (set.size() == forceMergerSize) {
                // Forces the merger of the fronting set into the list of chunks
                set.serialize();
            }
        }

        int deserializedCheckSize = (new Random()).nextInt(size);
        int currentSize = size;
        List<String> deletedStringVals = new ArrayList<>();
        for (ByteArray val: originalVals) {
            if (currentSize == deserializedCheckSize) {
                if (currentSize != ByteArraySet.deserialize(set.serialize()).size()) {
                    assertEquals("",
                            String.format("\nInserted Vals: %s\nDeleted Vals: %s\nMerger Size: %d\nCheck Size: %d\n",
                                    insertedStringVals.toString(),
                                    deletedStringVals.toString(),
                                    forceMergerSize,
                                    deserializedCheckSize));
                }
            }
            assertTrue(set.remove(val));
            assertFalse(set.contains(val));
            deletedStringVals.add(new String(val.getBytes()));
            currentSize -= 1;
            assertEquals(currentSize, set.size());
        }
    }

    @Test
    public void reallySmallRandomSerializeDerializeSizeCheckRemove() {
        testRandomSerializeDerializeSizeCheckRemove(SIZE_REALLY_SMALL);
    }

    @Test
    public void smallRandomSerializeDerializeSizeCheckRemove() {
        testRandomSerializeDerializeSizeCheckRemove(SIZE_SMALL);
    }

    @Test
    public void regularRandomSerializedRemove() {
        testRandomSerializeDerializeSizeCheckRemove(SIZE_REGULAR);
    }

    @Test
    public void bigRandomSerializeDerializeSizeCheckRemove() {
        testRandomSerializeDerializeSizeCheckRemove(SIZE_BIG);
    }

    @Test
    public void reallyBigRandomSerializeDerializeSizeCheckRemove() {
        testRandomSerializeDerializeSizeCheckRemove(SIZE_REALLY_BIG);
    }

    private void testSimilarRemove(boolean forceMerger) {
        System.out.println("Testing remove method for similar inputs with " + forceMerger + " forceMerger");
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

    private void testToArray(int size) {
        System.out.println("Testing toArray method with " + size + " size");
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
        testToArray(SIZE_EMPTY);
    }

    @Test
    public void reallySmallToArray() {
        testToArray(SIZE_REALLY_SMALL);
    }

    @Test
    public void smallToArray() {
        testToArray(SIZE_SMALL);
    }

    @Test
    public void regularToArray() {
        testToArray(SIZE_REGULAR);
    }

    @Test
    public void bigToArray() {
        testToArray(SIZE_BIG);
    }

    @Test
    public void reallyBigToArray() {
        testToArray(SIZE_REALLY_BIG);
    }

    private void testContains(int size) {
        System.out.println("Testing contains method with " + size + " size");
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
        testContains(SIZE_EMPTY);
    }

    @Test
    public void reallySmallContains() {
        testContains(SIZE_REALLY_SMALL);
    }

    @Test
    public void smallContains() {
        testContains(SIZE_SMALL);
    }

    @Test
    public void regularContains() {
        testContains(SIZE_REGULAR);
    }

    @Test
    public void bigContains() {
        testContains(SIZE_BIG);
    }

    @Test
    public void reallyBigContains() {
        testContains(SIZE_REALLY_BIG);
    }

    @Test
    public void testEmptyLastValueChunkIteratorBug() {
        System.out.println("Testing iterator bug for a chunk with an empty last value");
        ByteArraySet set = new ByteArraySet();

        /*
         * This bug only arises when:
         *  - there are multiple chunks
         *  - the first chunk has trailing zeros (a.k.a its last value was deleted)
         */

        int ChunkByteArrayCount = 2 * PER_CHUNK_MAX_BYTE_ARRAY_COUNT;
        int FrontingSetByteArrayCount = Math.min(5, ByteArraySet.MAX_FRONTING_SET_SIZE - 1);
        assertTrue(FrontingSetByteArrayCount > 0);

        int size = ChunkByteArrayCount + FrontingSetByteArrayCount;

        Set<ByteArray> vals = new TreeSet<>();
        for (ByteArray val: getRandomByteArrays(size)) {
            vals.add(val);
        }

        ByteArray firstChunkLastVal = null;
        for (ByteArray val: vals) {
            assertTrue(set.add(val));
            assertTrue(set.contains(val));
            if (set.size() == PER_CHUNK_MAX_BYTE_ARRAY_COUNT) {
                firstChunkLastVal = val;
            } else if (set.size() == ChunkByteArrayCount) {
                // Forces the merger of the fronting set into the list of chunks
                set.serialize();
            }
        }
        assertEquals(size, set.size());
        assertNotNull(firstChunkLastVal);

        set.remove(firstChunkLastVal);
        assertEquals(size - 1, set.size());

        assertEquals(size - 1, ByteArraySet.deserialize(set.serialize()).size());
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
