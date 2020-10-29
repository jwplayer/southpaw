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

import scala.Tuple2;

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

    public List<Tuple2<String, ByteArray>> getRandomStringByteArrayTuples(int count) {
        List<Tuple2<String, ByteArray>> vals = new ArrayList<Tuple2<String, ByteArray>>();
        while(vals.size() < count) {
            String stringVal = RandomStringUtils.randomAlphanumeric(6);
            ByteArray byteArrayVal = new ByteArray(stringVal);
            Tuple2<String, ByteArray> val = new Tuple2<String, ByteArray>(stringVal, byteArrayVal);
            if (!vals.contains(val)) {
                vals.add(val);
            }
        }
        return vals;
    }

    public List<ByteArray> getRandomByteArrays(int count) {
        List<ByteArray> vals = new ArrayList<ByteArray>();
        for (Tuple2<String, ByteArray> tuple: getRandomStringByteArrayTuples(count)) {
            vals.add(tuple._2);
        }
        return vals;
    }

    public void testAdd(int size) {
        ByteArraySet set = new ByteArraySet();

        List<ByteArray> vals = getRandomByteArrays(size);

        List<ByteArray> insertedVals = new ArrayList<ByteArray>();
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

    public void testAddAll(int size, boolean forceByteArraySet) {
        ByteArraySet set = new ByteArraySet();

        List<ByteArray> vals = getRandomByteArrays(size);

        for (ByteArray val: vals) {
            assertTrue(set.add(val));
        }
        assertEquals(size, set.size());

        List<ByteArray> stagedVals = getRandomByteArrays(size);
        Set<ByteArray> extraVals;
        if (forceByteArraySet) {
            extraVals = new ByteArraySet();
        } else {
            extraVals = new HashSet<ByteArray>();
        }
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
        testAddAll(150, false);
        testAddAll(150, true);
    }

    @Test
    public void bigAddAll() {
        testAddAll(750, false);
        testAddAll(750, true);
    }

    @Test
    public void reallyBigAddAll() {
        testAddAll(3567, false);
        testAddAll(3567, true);
    }

    public void testSimilarAdd(boolean forceMerger) {
        ByteArraySet set = new ByteArraySet();

        List<ByteArray> vals = new ArrayList<ByteArray>();
        vals.add(new ByteArray("bytear"));
        vals.add(new ByteArray("ByteAr"));
        vals.add(new ByteArray("bytea0"));
        vals.add(new ByteArray("0ytear"));

        List<ByteArray> insertedVals = new ArrayList<ByteArray>();
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
        List<ByteArray> alreadySeenVals = new ArrayList<ByteArray>();
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

    public void testRandomSerializeDerializeSizeCheckRemove(int size) {
        ByteArraySet set = new ByteArraySet();

        List<Tuple2<String, ByteArray>> tuples = getRandomStringByteArrayTuples(size);

        List<Tuple2<String, ByteArray>> vals = new ArrayList<Tuple2<String, ByteArray>>();
        for (Tuple2<String, ByteArray> val: tuples) {
            vals.add(val);
        }
        Collections.shuffle(vals);

        int forceMergerSize = (new Random()).nextInt(size);
        List<String> insertedStringVals = new ArrayList<String>();
        for (Tuple2<String, ByteArray> val: vals) {
            insertedStringVals.add(val._1);
            assertTrue(set.add(val._2));
            if (set.size() == forceMergerSize) {
                // Forces the merger of the fronting set into the list of chunks
                set.serialize();
            }
        }

        int deserializedCheckSize = (new Random()).nextInt(size);
        int currentSize = size;
        List<String> deletedStringVals = new ArrayList<String>();
        for (Tuple2<String, ByteArray> val: tuples) {
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
            assertTrue(set.remove(val._2));
            assertFalse(set.contains(val._2));
            deletedStringVals.add(val._1);
            currentSize -= 1;
            assertEquals(currentSize, set.size());
        }
    }

    @Test
    public void reallySmallRandomSerializeDerializeSizeCheckRemove() {
        testRandomSerializeDerializeSizeCheckRemove(1);
    }

    @Test
    public void smallRandomSerializeDerializeSizeCheckRemove() {
        testRandomSerializeDerializeSizeCheckRemove(2);
    }

    @Test
    public void regularRandomSerializedRemove() {
        testRandomSerializeDerializeSizeCheckRemove(150);
    }

    @Test
    public void bigRandomSerializeDerializeSizeCheckRemove() {
        testRandomSerializeDerializeSizeCheckRemove(750);
    }

    @Test
    public void reallyBigRandomSerializeDerializeSizeCheckRemove() {
        testRandomSerializeDerializeSizeCheckRemove(3567);
    }

    public void testSimilarRemove(boolean forceMerger) {
        ByteArraySet set = new ByteArraySet();

        List<ByteArray> vals = new ArrayList<ByteArray>();
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

        List<ByteArray> deletedVals = new ArrayList<ByteArray>();
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

        List<ByteArray> alreadySeenVals = new ArrayList<ByteArray>();
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
    public void testNullEquals() {
        ByteArraySet set = new ByteArraySet();

        List<ByteArray> vals = getRandomByteArrays(3);

        for (ByteArray val: vals) {
            assertTrue(set.add(val));
        }
        assertNotEquals(null, set);
    }

    @Test
    public void testEmptyLastValueChunkIteratorBug() {
        ByteArraySet set = new ByteArraySet();

        List<String> insertedVals = new ArrayList<String>(Arrays.asList(
                "TwRxFt", "6qQWPy", "9ria0n", "HOi03p", "GWZn71", "XjxnZw", "n2lzAr", "ob749j", "K185en", "GVGKvr", "v2i31k", "ba6WTW",
                "mI5XWH", "nDKao0", "KUhDTU", "jWKyde", "d09HsY", "ICEGbr", "1zFMrV", "as9sLL", "F8E9X9", "SI6Mwy", "6SYe00", "qm0LRg",
                "pMRaeO", "WDayEc", "iZtOm8", "shpHbk", "fFTNIt", "atVJu3", "AAcd5o", "TtQJPh", "f2bR5U", "XuXcD8", "spjjK3", "PFJPTw",
                "0gZQBc", "wmvdFl", "0u9hBM", "vawaKC", "DHiDPM", "odgBY5", "s3sLpX", "RZjHsY", "SQiQwm", "9KRUtG", "4XMbls", "cTqilQ",
                "tWhTf8", "7cCVOG", "wnlMUC", "z5Lze0", "H3viHU", "T0uS0W", "2qNE1i", "RFhtaa", "uXLMQ3", "mBbGP5", "d8DSQO", "A5SD0C",
                "S9CODO", "KxWhtn", "jaO8TT", "0GRsjy", "M9trwS", "T9bnd4", "MVZWuM", "eRGh2C", "tMOrvQ", "CFnZE9", "HkfKpp", "5yzNKb",
                "Hg9thE", "HtQevs", "JqANlk", "RjYmGK", "CFvDWw", "aoYz3p", "qdLuPq", "t16LBl", "9ABwgQ", "Fm38GP", "FWs7MI", "ViOr77",
                "5KuLkT", "MIBS0Y", "h97lM4", "KCvLiA", "vFlCeO", "Bx4CfG", "Fo9UjO", "MTV0lK", "R0ovoR", "1U9gBQ", "tRI2xN", "nBoYID",
                "1rqsnz", "h7t4yb", "EemAVa", "EUoQz4", "Y4BUtj", "KPRZuu", "VkttSp", "sQdUGf", "YudArm", "kpJTzf", "vj9c3o", "jOcqcp",
                "MlUoVL", "jBw1vg", "cJUXT6", "VHdORy", "wJ50kS", "Fm12tT", "CC6h9I", "QCYRv3", "hA1A9F", "O1PmKC", "Va9z4Z", "MGMPIQ",
                "WQBgcN", "IiUXnX", "p6amEz", "929jH3", "PE2IyV", "8ZI9zE", "DrQlq3", "1ui24g", "2Xz7qw", "JSvu7m", "wG9dlX", "jKZ9tm",
                "i8B3jn", "0SKCfP", "YuYveN", "8MpiKm", "AAqZ4A", "voLOfL", "1RRokq", "1skkcu", "g4f3gI", "PxmP3F", "Gy956M", "9POJS3",
                "xgax5o", "Cp2uwR", "EKRQ6a", "EUkurY", "IgUp00", "npiwoL", "GLPvUV", "4k48au", "q2b4ro", "bLjjyL", "JnLW2h", "RcXZWQ",
                "FsstmX", "RWTMSQ", "RpIaEC", "yfmjTx", "2UPdaJ", "6Rxc2Q", "LBFQRS", "9qoU1a", "ddy4hE", "SKjC6c", "c6iHyZ", "3TwerJ",
                "i8ptpc", "haAdXY", "JyZeKI", "I47kCO", "SBVeEn", "Rv3grc", "EYFeRb", "lpefj1", "J18TrG", "ZINiuf", "P6zKqF", "JzZqcI",
                "auZajU", "as2QnY", "MdQAB9", "aru9f4", "x13oIr", "qqGSGE", "OckD3p", "PthXva", "Q69BGu", "6Qorzv", "OGoGZX", "050LmB",
                "coFGQZ", "tmtdqz", "LtfMK3", "K33R0O", "jC4otu", "DEDTPi", "TpL13C", "8LTFO3", "Ktielo", "4Y3kDE", "YEQKME", "HunGuN",
                "BtuVed", "nhemmJ", "Ac8Xxk", "X8hzPX", "768Q5e", "7YZt1N", "0R1nxS", "HtxupF", "iSYb2C", "8rewmf", "Eduk6o", "ZMrm8Y",
                "TTtl2P", "pKE7ee", "58xLPZ", "WCXGBd", "qek3bt", "8wfuuz", "1TEmi5", "IJTVEf", "PfzKqz", "6OvHIp", "9lPdSQ", "TOQoxG",
                "tgXX4g", "m1VMoD", "svd4Mu", "6IT9C1", "P1dZQd", "WBxPi2", "WoTEuD", "GaYJXk", "u3vEJ0", "50c6Rl", "rrbgZH", "Cgofp5",
                "N8xUBF", "gCcqhT", "QTCrr5", "Jpy88G", "VS3pes", "TxMqtd", "e5jmHQ", "06yu4C", "l5V3Wu", "rqMtZ1", "EuRjAQ", "HPQhG0",
                "AH4Jfb", "dsDUii", "i80ftE", "d7BL7P", "ZwnvBs", "x817jt", "nkBqiv", "1QIFdF", "nvDP8z", "QaWdln", "RWlrXQ", "ZGrWwf",
                "6laTiv", "NyvXw5", "JWFxTw", "7gPIJt", "PdRHRK", "dssiW0", "5ZRlRy", "lWJYcV", "cnWpcJ", "t6y5kl", "nB7IWh", "SZ0nkL",
                "weTJ5o", "s5QFwL", "meJcRt", "ChWf3j", "FZDkgB", "sbZqwn", "vVDt50", "IlZu2r", "U1gnfz", "NeJTjz", "pBSE2Z", "YkJpf0",
                "ZMk1XH", "bJcQBu", "ebWVhF", "41crxT", "rv2czZ", "i1K7mJ", "e2lz91", "mNPAhe", "JVOudM", "9lrUya", "KHc5y7", "K9kKz5",
                "3SK8xF", "P91u2d", "2qjAXU", "EVy6Ka", "Hcc0km", "qS8NWH", "U62S1S", "THGBuV", "qDjUOz", "hbexIB", "DYhmT7", "qdZHrs",
                "KQvdhL", "p9CcgB", "jFUjWw", "Be9F7C", "wHmPte", "Jy5dEu", "42QAEa", "8ShaKs", "PmgTWl", "kNzPlZ", "WipVMz", "hKRotO",
                "L9YbeK", "lMntb4", "k9jIJq", "25utZ1", "tOpshu", "GvNh4F", "C5tAZG", "yKvRUm", "EoPjsZ", "gSVs5C", "HB64lo", "yQZPo7",
                "J0ssLr", "KnCrw0", "oFa1wT", "23WrD7", "9PDJAx", "ywFWcR", "i079XR", "vhhpsU", "wKQHhA", "53NycK", "WQ3DrB", "SWYiQQ",
                "wmQhet", "DxV2I6", "q38VCc", "KzHqlj", "VtuPmY", "OIDFnU", "iWRNp4", "ts70KW", "McrcHq", "mRCHYg", "k30tdl", "2c9T5B",
                "8bmbTP", "irLSEE", "RdzkpR", "zxfl4B", "0IA82Y", "buR4iU", "SqzP5f", "FPFGVu", "qg9QcV", "qRxN8A", "h8BM2t", "IELvVb",
                "RqlJnW", "cxPuxC", "mutu8V", "wgTMYH", "lYziiJ", "sntwc0", "DryNwp", "YamgkV", "E8zabs", "YhMRm1", "atJcvE", "UVuLX2",
                "yQ9My6", "rVeBYM", "nX1zWU", "DMWYxI", "Iym6aZ", "FQdTWA", "2dy03W", "00xljA", "3cqwv1", "RVgiIj", "A37AdQ", "aC3UjB",
                "TSqmCl", "v0LKC0", "nUOX26", "z7CpD4", "GqAroN", "QK89O4", "fxxFb5", "PvrkeJ", "N5lSV0", "nWtFFX", "ZLwE59", "LkG5Ua",
                "c8EPBG", "woXVD6", "dhx63O", "utgrk7", "hqP4Pa", "KFPCwH", "6mjTZu", "H7lhha", "RrXrho", "H23edp", "BqgUGZ", "xGcGtM",
                "3Au5kb", "YTv2eB", "UcYDzi", "KBJyRt", "Wx3MCS", "VHmpoZ", "HlPb7P", "CEdv3e", "eumbNw", "blF3UE", "DLjoGf", "KrdSns",
                "0KyRL0", "r0QPRy", "bEMMOH", "ixajk6", "3cMXSF", "psENi6", "Vpcppc", "UdnxGI", "KX3yZ4", "wPyNV3", "EDjCh9", "qK86fM",
                "W5P0fA", "dDckeg", "4IX9Wa", "eK4ICZ", "t31ujb", "R5KjH5", "GaU4Z5", "iUoee3", "8F2oWQ", "kvG1IA", "BNIgt4", "6BULzp",
                "zQs0ic", "g3B3S3", "4rNJsQ", "szlwZC", "UZTEMM", "eci1SJ", "dUSeh4", "sbTanP", "D1vGRZ", "udVeUL", "bkaEXp", "NFFpsD",
                "3m4C3I", "V75wGg", "XdsXzs", "Z76Dea", "363T4u", "oyqEME", "blN3tQ", "dWKHGh", "quFPoj", "XMHm3e", "FAox7C", "ksuGGf",
                "fywwZf", "lwWZoG", "KjMa5n", "MgjT8y", "u0oihY", "Ywsj9s", "iENvyz", "EE1Ypz", "O3HFzi", "16Jxmg", "sEgcK7", "0cqGtO",
                "6qFIW2", "rYDMov", "qCoDie", "x3ARgx", "KsUZ86", "gB9ssU", "vZk7TM", "FwIaQG", "1K3fmU", "1Mfmqi", "c0PyMC", "69yVKp",
                "qyc4f0", "MgKDN5", "WMP5on", "39qP0N", "j1yfap", "GrqSFN", "0PgoAB", "YCkw4r", "2hQ5rt", "N756Pz", "TDCYko", "uSKCpj",
                "Xifcyu", "JLGIwO", "2Z7II9", "ICuPD6", "94cU3i", "5xnWk4", "es4Sxn", "rqsLPW", "o5sRoC", "Ne2VQ2", "aNtYh2", "zgMj3T",
                "Fd5tDE", "JsyJBv", "af7epq", "2IXLJL", "WJFzRc", "1kAQBN", "EZ4hN5", "WPrdN4", "wyDRRe", "gcjaEN", "sgKGFa", "AZb6fR",
                "xngXyt", "xoIti4", "TR0i2k", "kLtWmJ", "byazyY", "RPz9jB", "OMU22b", "PyZcKQ", "K1fQJ3", "REJBU8", "8aff2b", "gxlzkT",
                "RYR2av", "StDElz", "HvrDXJ", "AtVcyd", "ynnW6e", "gQ3Eup", "kRRnEu", "psRJzv", "vvBpCh", "CQyvbN", "ZV8EAb", "90847n",
                "6TYyph", "aYOIb9", "JQtjeP", "XaEK2f", "FZj0PD", "ZSuHZ9", "E7kSuu", "bwQ3a8", "iX2EvT", "hkXjuc", "N6YI0m", "vHX2nd",
                "ZYdwdP", "v097nz", "8ZoPeZ", "d82FW1", "pF9LN4", "Xzn72l", "HHGiKG", "a3RBZD", "LwW0N8", "Ddk1Px", "7NLH0N", "n5iGth",
                "X68LEe", "Tra8s9", "q7Qgci", "cF8ii8", "mbsDaU", "RAaVlL", "DvFPFP", "5MxkXD", "nMXEHS", "tLsWGH", "KtrL2a", "4hIFPk",
                "BEYM2q", "JGFTjJ", "KaoLbX", "9XCeHo", "eJzwxP", "1egwtf", "OuaaLM", "ohlXZ2", "klsCV9", "ak4mK8", "2MfH4h", "igYF8B",
                "AvJSkL", "lg8yjp", "55Iybb", "nt4neo", "RSjaA8", "yg7fI9", "vAbYQi", "QkESbu", "koFGPf", "tD0QG9", "mt1jb7", "0UPyR6",
                "bq9oGD", "YuIPBI", "kvUO25", "bcHCET", "KPFmuF", "X2P4Jy", "wlms22", "QcMEYw", "zF78Rd", "9Hg2UP", "Z5I6bs", "gIWpzm",
                "1CIN57", "cA3v56", "PE9FAZ", "9n7LrD", "weRWA3", "1XjadO", "hub9jz", "Ff9FKT", "6OtucJ", "hfw6gj", "EUBBLs", "kb8aCl",
                "O6XW6r", "KH8N3t"
        ));
            
        List<String> deletedVals = new ArrayList<String>(Arrays.asList(
                "O1PmKC", "5KuLkT", "KPFmuF", "i80ftE", "9ABwgQ", "GLPvUV", "PthXva", "haAdXY", "zQs0ic", "irLSEE", "ob749j", "CEdv3e",
                "PFJPTw", "YuYveN", "5MxkXD", "AH4Jfb", "2IXLJL", "0KyRL0", "t16LBl", "EKRQ6a", "ZLwE59", "RVgiIj", "58xLPZ", "HtxupF",
                "THGBuV", "6OvHIp", "00xljA", "9POJS3", "KH8N3t", "K9kKz5", "OMU22b", "dUSeh4", "2Z7II9", "X8hzPX", "KjMa5n", "l5V3Wu",
                "fFTNIt", "PmgTWl", "FZj0PD", "WDayEc", "PvrkeJ", "t6y5kl", "6Rxc2Q", "Iym6aZ", "U1gnfz", "wG9dlX", "QcMEYw", "cnWpcJ",
                "FWs7MI", "HPQhG0", "8rewmf", "ZINiuf", "Fo9UjO", "vAbYQi", "OGoGZX", "WoTEuD", "0gZQBc", "N5lSV0", "EoPjsZ", "XjxnZw",
                "6SYe00", "H3viHU", "r0QPRy", "XMHm3e", "dhx63O", "4IX9Wa", "xngXyt", "RYR2av", "gxlzkT", "z5Lze0", "wJ50kS", "9lPdSQ",
                "rqMtZ1", "wyDRRe", "npiwoL", "vvBpCh", "2UPdaJ", "psRJzv", "rqsLPW", "0cqGtO", "cxPuxC", "iZtOm8", "ddy4hE", "F8E9X9",
                "tMOrvQ", "lMntb4", "ynnW6e", "L9YbeK", "vZk7TM", "gSVs5C", "weRWA3", "buR4iU", "XuXcD8", "JLGIwO", "C5tAZG", "u3vEJ0",
                "8LTFO3", "Wx3MCS", "aoYz3p", "wPyNV3", "t31ujb", "JsyJBv", "PE2IyV", "WJFzRc", "RZjHsY", "cJUXT6", "HunGuN", "DYhmT7",
                "bkaEXp", "JVOudM", "udVeUL", "x817jt", "pBSE2Z", "RqlJnW", "O3HFzi", "UVuLX2", "atJcvE", "psENi6", "Jpy88G", "u0oihY",
                "1skkcu", "16Jxmg", "OIDFnU", "JWFxTw", "5xnWk4", "2qjAXU", "K1fQJ3", "spjjK3", "o5sRoC", "GqAroN", "DHiDPM", "BqgUGZ",
                "768Q5e", "kvUO25", "RWlrXQ", "VS3pes", "3TwerJ", "as9sLL", "klsCV9", "AtVcyd", "c0PyMC", "iX2EvT", "aru9f4", "RcXZWQ",
                "Xifcyu", "VHdORy", "W5P0fA", "SZ0nkL", "RAaVlL", "lWJYcV", "Xzn72l", "HkfKpp", "nhemmJ", "Y4BUtj", "O6XW6r", "929jH3",
                "PxmP3F", "KsUZ86", "ba6WTW", "nX1zWU", "g3B3S3", "dDckeg", "Tra8s9", "Ff9FKT", "1kAQBN", "xoIti4", "shpHbk", "QkESbu",
                "EUkurY", "iENvyz", "i8B3jn", "NeJTjz", "StDElz", "qS8NWH", "szlwZC", "1QIFdF", "fxxFb5", "JzZqcI", "dWKHGh", "eK4ICZ",
                "REJBU8", "EUoQz4", "wmvdFl", "TR0i2k", "Rv3grc", "ixajk6", "AAqZ4A", "T9bnd4", "TxMqtd", "mNPAhe", "odgBY5", "c6iHyZ",
                "sgKGFa", "ak4mK8", "hub9jz", "hqP4Pa", "YEQKME", "qdLuPq", "GvNh4F", "qRxN8A", "25utZ1", "HB64lo", "KtrL2a", "8aff2b",
                "sntwc0", "uXLMQ3", "iSYb2C", "d8DSQO", "bcHCET", "KCvLiA", "zxfl4B", "ts70KW", "jOcqcp", "i079XR", "JSvu7m", "svd4Mu",
                "Jy5dEu", "eci1SJ", "7YZt1N", "4hIFPk", "qqGSGE", "K185en", "lg8yjp", "7gPIJt", "sEgcK7", "uSKCpj", "WipVMz", "MgjT8y",
                "7cCVOG", "woXVD6", "YTv2eB", "KHc5y7", "g4f3gI", "s3sLpX", "nUOX26", "s5QFwL", "gCcqhT", "N756Pz", "Hg9thE", "JqANlk",
                "KrdSns", "xGcGtM", "a3RBZD", "Va9z4Z", "voLOfL", "UdnxGI", "as2QnY", "SWYiQQ", "hkXjuc", "tmtdqz"
        ));

        int insertedValCount = 0;
        for (String stringVal: insertedVals) {
            if (insertedValCount == 603) {
                // Forces the merger of the fronting set into the list of chunks
                set.serialize();
            }
        	ByteArray val = new ByteArray(stringVal);
            assertTrue(set.add(val));
            assertTrue(set.contains(val));
            insertedValCount++;
        }
        assertEquals(650, set.size());

        for (String deletedVal: deletedVals) {
            ByteArray val = new ByteArray(deletedVal);
            assertTrue(set.remove(val));
            assertFalse(set.contains(val));
        }
        assertEquals(400, set.size());

        assertEquals(400, ByteArraySet.deserialize(set.serialize()).size());
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
