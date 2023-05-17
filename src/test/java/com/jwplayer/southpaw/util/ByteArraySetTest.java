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

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.time.StopWatch;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jwplayer.southpaw.util.ByteArraySet.Chunk;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.Collectors;

import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class ByteArraySetTest {
    private static final int RANDOM_STRING_SIZE = 6;
    private static final long RANDOM_SEED = new Random().nextLong();
    // This represents the max number of entries in a chunk given a static 6 + 1 bytes per entry
    private static final int PER_CHUNK_MAX_BYTE_ARRAY_COUNT =
            (int) Math.floor(Chunk.MAX_CHUNK_SIZE / (1f + RANDOM_STRING_SIZE));

    private static final Logger logger =  LoggerFactory.getLogger(ByteArraySetTest.class);

    public ByteArraySet set = new ByteArraySet();

    @Parameter
    public int size;

    @Parameters(name = "Test size: {0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(
            new Object[][]{
                {0},
                {1},
                {2},
                {PER_CHUNK_MAX_BYTE_ARRAY_COUNT},
                {2 * PER_CHUNK_MAX_BYTE_ARRAY_COUNT},
                {ByteArraySet.MAX_FRONTING_SET_SIZE + 20 * PER_CHUNK_MAX_BYTE_ARRAY_COUNT},
            });
    }


    private long getDifferentRandomSeed() {
        long differentRandomSeed = RANDOM_SEED;
        while (differentRandomSeed == RANDOM_SEED) {
            differentRandomSeed = new Random().nextLong();
        }
        logger.info("Getting different random seed: " + differentRandomSeed);
        return differentRandomSeed;
    }

    private List<ByteArray> getRandomByteArrays(int count) {
        return getRandomByteArrays(count, RANDOM_SEED);
    }

    private List<ByteArray> getRandomByteArrays(int count, long seed) {
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

    @BeforeClass
    public static void setupClass() throws Exception {
        logger.info("Running ByteArraySet test(s) with random seed: " + RANDOM_SEED);
        Thread.sleep(30000);
    }

    @Before
    public void setup() {
        set.addAll(getRandomByteArrays(size));
    }

    @Test
    public void testAdd() {
        final ByteArray val = new ByteArray(RandomStringUtils.random(RANDOM_STRING_SIZE, 0, 0, true, true));
        set.add(val);
        assertTrue(set.contains(val));
        assertFalse(set.add(val));
    }

    @Test
    public void testAddAll() {
        final List<ByteArray> vals = getRandomByteArrays(size, getDifferentRandomSeed());
        set.addAll(vals);
        assertTrue(set.containsAll(vals));
        assertFalse(set.addAll(vals));
    }

    @Test
    public void testAddWithMergers() {
        final List<ByteArray> vals = getRandomByteArrays(size, getDifferentRandomSeed());
        for (ByteArray val: vals) {
            set.merge();
            set.add(val);
            assertTrue(set.contains(val));
            assertFalse(set.add(val));
        }
    }

    @Test
    public void testClear() {
        set.clear();

        assertEquals(0, set.size());
    }

    @Test
    public void testContains() {
        final List<ByteArray> vals = new ArrayList<>(set);
        Collections.shuffle(vals);

        for(ByteArray val: vals) {
            assertTrue(set.contains(val));
        }
    }

    @Test
    public void testContainsAll() {
        final List<ByteArray> vals = new ArrayList<>(set);
        Collections.shuffle(vals);

        assertTrue(set.containsAll(vals));
    }

    @Test
    public void testEmptyLastValueChunkIteratorBug() {
        /*
         * This bug only arises when:
         *  - there are multiple chunks
         *  - the first chunk has trailing zeros (a.k.a its last value was deleted)
         */
        ByteArraySet vals = new ByteArraySet();
        final int chunkByteArrayCount = 2 * PER_CHUNK_MAX_BYTE_ARRAY_COUNT;

        for (ByteArray val: set) {
            assertTrue(vals.add(val));
            if (vals.size() == chunkByteArrayCount) {
                // Forces the merger of the fronting set into the list of chunks
                vals.merge();
            }
        }
        assertEquals(size, vals.size());
        if(vals.chunks.size() > 0) {
            Chunk firstChunk = vals.chunks.get(0);
            assertNotNull(firstChunk.max);
            vals.remove(firstChunk.max);
            if(firstChunk.entries == 0) {
                assertNull(firstChunk.max);
            } else {
                assertNotNull(firstChunk.max);
            }
            assertEquals(size - 1, vals.size());
            assertEquals(size - 1, ByteArraySet.deserialize(vals.serialize()).size());
        }
    }

    @Test
    public void testIsEmpty() {
        if(size > 0) {
            assertFalse(set.isEmpty());
        } else {
            assertTrue(set.isEmpty());
        }
        set.clear();
        assertTrue(set.isEmpty());
    }

    public void testIterator(ByteArraySet testSet) {
        Iterator<ByteArray> iter = testSet.iterator();
        List<ByteArray> alreadySeenVals = new ArrayList<>();
        while(iter.hasNext()) {
            ByteArray val = iter.next();
            assertNotNull(val);
            assertFalse(alreadySeenVals.contains(val));
            alreadySeenVals.add(val);
            assertTrue(testSet.contains(val));
        }
        assertEquals(testSet.size(), alreadySeenVals.size());
        assertTrue(testSet.containsAll(alreadySeenVals));
    }

    @Test
    public void testIteratorBaseSet() {
        testIterator(set);
    }

    @Test
    public void testIteratorAfterRemoveFirstHalf() {
        final List<ByteArray> valsToRemove = new ArrayList<>();
        int i = 0;
        for (ByteArray val: set) {
            if(i > set.size() / 2) break;
            valsToRemove.add(val);
            i++;
        }
        set.removeAll(valsToRemove);
        testIterator(set);
    }

    @Test
    public void testIteratorAfterRemoveOddValues() {
        final List<ByteArray> valsToRemove = set.stream()
                .filter(x -> x.getBytes()[x.getBytes().length - 1] % 2 == 1)
                .collect(Collectors.toList());
        set.removeAll(valsToRemove);
        testIterator(set);
    }

    @Test
    public void testOperationsWithRandomMerges() {
        if(size == 0) return;
        ByteArraySet set = new ByteArraySet();

        final List<ByteArray> originalVals = getRandomByteArrays(size);

        List<ByteArray> vals = new ArrayList<>(originalVals);

        /*
         * Shuffles values to make sure we do not remove them in the same
         * order we added them in.
         */
        Collections.shuffle(vals);

        int forceMergerSize = (new Random(RANDOM_SEED)).nextInt(size);
        for (ByteArray val: vals) {
            assertTrue(set.add(val));
            if (set.size() == forceMergerSize) {
                // Forces the merger of the fronting set into the list of chunks
                set.merge();
            }
        }

        int deserializedCheckSize = (new Random(RANDOM_SEED)).nextInt(size);
        int currentSize = size;
        for (ByteArray val: originalVals) {
            if (currentSize == deserializedCheckSize) {
                assertEquals(currentSize, ByteArraySet.deserialize(set.serialize()).size());
            }
            assertTrue(set.remove(val));
            currentSize -= 1;
        }
    }

    @Test
    public void testRemove() {
        final List<ByteArray> vals = new ArrayList<>(set);

        /*
         * Shuffles values to make sure we do not remove them in the same
         * order we added them in.
         */
        Collections.shuffle(vals);
        assertTrue(vals.containsAll(set));

        int currentSize = size;
        for(ByteArray val: vals) {
            assertTrue(set.remove(val));
            assertFalse(set.contains(val));
            currentSize -= 1;
            assertEquals(currentSize, set.size());
        }
    }

    @Test
    public void testRemoveAll() {
        final List<ByteArray> vals = new ArrayList<>(set);

        /*
         * Shuffles values to make sure we do not remove them in the same
         * order we added them in.
         */
        Collections.shuffle(vals);
        assertTrue(vals.containsAll(set));
        if(size > 0) {
            assertTrue(set.removeAll(vals));
        } else {
            assertFalse(set.removeAll(vals));
        }
        assertTrue(set.isEmpty());
    }

    public void testRemoveAndReAdd(ByteArraySet testSet, List<ByteArray> valsToRemove) {
        final List<ByteArray> originalVals = new ArrayList<>(testSet);
        set.removeAll(valsToRemove);
        assertEquals(originalVals.size() - valsToRemove.size(), testSet.size());
        set.addAll(valsToRemove);
        assertEquals(originalVals.size(), testSet.size());
        assertTrue(testSet.containsAll(originalVals));
    }

    @Test
    public void testRemoveAndReAddFirstHalf() {
        final List<ByteArray> valsToRemove = new ArrayList<>();
        int i = 0;
        for (ByteArray val: set) {
            if(i > set.size() / 2) break;
            valsToRemove.add(val);
            i++;
        }
        testRemoveAndReAdd(set, valsToRemove);
    }

    @Test
    public void testRemoveAndReAddOddValues() {
        final List<ByteArray> valsToRemove = set.stream()
                .filter(x -> x.getBytes()[x.getBytes().length - 1] % 2 == 1)
                .collect(Collectors.toList());
        testRemoveAndReAdd(set, valsToRemove);
    }

    @Test
    public void testSerializeDeserialize() {
        byte[] bytes = set.serialize();
        ByteArraySet deSet = ByteArraySet.deserialize(bytes);

        assertTrue(set.containsAll(deSet));
        assertEquals(size, deSet.size());
    }

    @Test
    public void testSize() {
        assertEquals(size, set.size());
    }

    @Test
    public void testToArray() {
        ByteArray[] byteArrays = set.toArray();
        assertEquals(size, byteArrays.length);
        Set<ByteArray> setFromArray = new TreeSet<>(Arrays.asList(byteArrays));
        assertTrue(set.containsAll(setFromArray));
    }
}
