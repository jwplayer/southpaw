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

        List<String> stringVals = new ArrayList<String>();
        List<Tuple2<String, ByteArray>> vals = new ArrayList<Tuple2<String, ByteArray>>();
        for (Tuple2<String, ByteArray> val: tuples) {
            stringVals.add(val._1);
            vals.add(val);
        }
        Collections.shuffle(vals);

        for (Tuple2<String, ByteArray> val: vals) {
            assertTrue(set.add(val._2));
        }

        int deserializedCheckSize = (new Random()).nextInt(size);
        int currentSize = size;
        List<String> deletedStringVals = new ArrayList<String>();
        for (Tuple2<String, ByteArray> val: tuples) {
            if (currentSize == deserializedCheckSize) {
            	if (currentSize != ByteArraySet.deserialize(set.serialize()).size()) {
            		assertEquals("",
            				String.format("\nString Vals: %s\nDeleted Vals: %s\nCheck Size: %d\n",
            						stringVals.toString(),
            						deletedStringVals.toString(),
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

        List<String> stringVals = new ArrayList<String>();
        stringVals.add("DWLdcy");
        stringVals.add("jL7OXc");
        stringVals.add("xL4AOX");
        stringVals.add("BhM92q");
        stringVals.add("mjSVUp");
        stringVals.add("QvHVXT");
        stringVals.add("hTZirP");
        stringVals.add("5FMjjz");
        stringVals.add("WymqjW");
        stringVals.add("bdYRTz");
        stringVals.add("qbe7z7");
        stringVals.add("0bwTpP");
        stringVals.add("wMwKWR");
        stringVals.add("Q98kPf");
        stringVals.add("BhhhQF");
        stringVals.add("l0Uzxn");
        stringVals.add("ksNDO3");
        stringVals.add("C2j50q");
        stringVals.add("b3fliX");
        stringVals.add("P7PSzd");
        stringVals.add("vnuMAd");
        stringVals.add("viJzON");
        stringVals.add("7yOEjn");
        stringVals.add("40CARS");
        stringVals.add("0kz8Qa");
        stringVals.add("hCg160");
        stringVals.add("RovvLs");
        stringVals.add("z7p8EL");
        stringVals.add("J1xwIN");
        stringVals.add("FPc0JB");
        stringVals.add("4YGDE1");
        stringVals.add("diytUA");
        stringVals.add("Uao7u3");
        stringVals.add("27bFr5");
        stringVals.add("WqW8Bz");
        stringVals.add("dvsr3B");
        stringVals.add("qSLPoV");
        stringVals.add("I9kOsc");
        stringVals.add("xMh2Xh");
        stringVals.add("C1eWEA");
        stringVals.add("73bo7W");
        stringVals.add("FtN6oj");
        stringVals.add("WaWTSb");
        stringVals.add("TR4UMf");
        stringVals.add("Uh4x1D");
        stringVals.add("HjFicA");
        stringVals.add("fk3AR0");
        stringVals.add("AIMPuu");
        stringVals.add("sHVxmR");
        stringVals.add("0cMSHC");
        stringVals.add("1vNtj2");
        stringVals.add("JeLaaZ");
        stringVals.add("LNk7vT");
        stringVals.add("oIakoY");
        stringVals.add("dLHzQ4");
        stringVals.add("5WqYGv");
        stringVals.add("suIOlv");
        stringVals.add("qrKKMq");
        stringVals.add("Fmxe7S");
        stringVals.add("XGxsEL");
        stringVals.add("sDXmUO");
        stringVals.add("5dg4hJ");
        stringVals.add("voW03P");
        stringVals.add("FLkUVo");
        stringVals.add("O8ZVKw");
        stringVals.add("8YqO12");
        stringVals.add("33F7TK");
        stringVals.add("zYv8aC");
        stringVals.add("ODv5Ky");
        stringVals.add("iz3N1i");
        stringVals.add("mBAgLd");
        stringVals.add("bjYxys");
        stringVals.add("JieBhQ");
        stringVals.add("FL0QM3");
        stringVals.add("OQg9yC");
        stringVals.add("uzzaq1");
        stringVals.add("RVkmuq");
        stringVals.add("KN3hBd");
        stringVals.add("KiTYEm");
        stringVals.add("KP3ZyZ");
        stringVals.add("h0UgIy");
        stringVals.add("AONOyN");
        stringVals.add("RSV1gw");
        stringVals.add("MwLl3P");
        stringVals.add("LFvfTU");
        stringVals.add("QwFaOP");
        stringVals.add("e11AYM");
        stringVals.add("DqEfiN");
        stringVals.add("Dr00ys");
        stringVals.add("qoyvDw");
        stringVals.add("XqLS20");
        stringVals.add("5xajbG");
        stringVals.add("oZjHz9");
        stringVals.add("UMTxx4");
        stringVals.add("MwY9PR");
        stringVals.add("JkD21q");
        stringVals.add("NhOlRa");
        stringVals.add("ICGSBQ");
        stringVals.add("oZuXy3");
        stringVals.add("LJ3hWF");
        stringVals.add("imJI7g");
        stringVals.add("T6ENXj");
        stringVals.add("slt1cI");
        stringVals.add("UosQAC");
        stringVals.add("dNJ8Ye");
        stringVals.add("XVYllu");
        stringVals.add("wuq0c5");
        stringVals.add("VTeWzU");
        stringVals.add("6zvwiV");
        stringVals.add("2jj6eV");
        stringVals.add("POKBSd");
        stringVals.add("Cwu4EK");
        stringVals.add("wG6lbm");
        stringVals.add("mJTQM6");
        stringVals.add("4TYlwC");
        stringVals.add("hCjGKB");
        stringVals.add("VxIpM0");
        stringVals.add("O7VhjP");
        stringVals.add("8AzEh5");
        stringVals.add("6K2wbh");
        stringVals.add("YfVDI8");
        stringVals.add("pWWC55");
        stringVals.add("G38Qh5");
        stringVals.add("OcQ3OX");
        stringVals.add("7bZ9HX");
        stringVals.add("vLuwwZ");
        stringVals.add("oepP2E");
        stringVals.add("vmo9lA");
        stringVals.add("z4QYlx");
        stringVals.add("u1oF8o");
        stringVals.add("uAaV8p");
        stringVals.add("XkQQ0H");
        stringVals.add("HHWlaQ");
        stringVals.add("jvzSeS");
        stringVals.add("tQuq0f");
        stringVals.add("EUkPQ0");
        stringVals.add("TmpzWl");
        stringVals.add("3kEshJ");
        stringVals.add("K4hMSL");
        stringVals.add("hzKvYp");
        stringVals.add("pN3Gz6");
        stringVals.add("dMVk9D");
        stringVals.add("u8NoY4");
        stringVals.add("3VElw2");
        stringVals.add("Gdh177");
        stringVals.add("jLE46v");
        stringVals.add("IkG3RV");
        stringVals.add("sbMUK6");
        stringVals.add("KzQEgK");
        stringVals.add("QYJbRP");
        stringVals.add("9eZNJD");
        stringVals.add("sWJWU1");
        stringVals.add("O4i1Jp");
        stringVals.add("KllQRE");
        stringVals.add("yNJfdh");
        stringVals.add("s0IxAi");
        stringVals.add("Je8DAp");
        stringVals.add("jAEe4k");
        stringVals.add("Dg7aT0");
        stringVals.add("QBCAaN");
        stringVals.add("sxNpX0");
        stringVals.add("G50aHa");
        stringVals.add("qwFwy4");
        stringVals.add("yovKU2");
        stringVals.add("wHinWy");
        stringVals.add("2L53ul");
        stringVals.add("WG3u57");
        stringVals.add("QpWuBT");
        stringVals.add("qd6Pbs");
        stringVals.add("azoPHo");
        stringVals.add("Cgl49F");
        stringVals.add("Lf3grL");
        stringVals.add("eBtxxe");
        stringVals.add("UIUN0u");
        stringVals.add("REAvwj");
        stringVals.add("9kPThi");
        stringVals.add("IwdKbv");
        stringVals.add("5ocU0A");
        stringVals.add("GuIaPi");
        stringVals.add("Vq2g76");
        stringVals.add("HUIC5t");
        stringVals.add("VOHpK8");
        stringVals.add("xjwzhh");
        stringVals.add("0aw2qT");
        stringVals.add("EfzABi");
        stringVals.add("Nua6T1");
        stringVals.add("ePgL82");
        stringVals.add("fegMBS");
        stringVals.add("XxmX6M");
        stringVals.add("JRHdrY");
        stringVals.add("Vatu4j");
        stringVals.add("9QjKYj");
        stringVals.add("ObHPUj");
        stringVals.add("h3hiR3");
        stringVals.add("4TyCJc");
        stringVals.add("FjRFno");
        stringVals.add("n8dFNQ");
        stringVals.add("sR2D28");
        stringVals.add("HkzI1w");
        stringVals.add("q4EvCJ");
        stringVals.add("8KRfg9");
        stringVals.add("z1h0GR");
        stringVals.add("HYZiFl");
        stringVals.add("lr0RCn");
        stringVals.add("Nt0b81");
        stringVals.add("FNelS9");
        stringVals.add("29Xl6A");
        stringVals.add("KdEPuH");
        stringVals.add("a3NxOI");
        stringVals.add("Cy6PeR");
        stringVals.add("E3xpdP");
        stringVals.add("Eb6D78");
        stringVals.add("pxNbUf");
        stringVals.add("MuaQjS");
        stringVals.add("0SzjMP");
        stringVals.add("TNufro");
        stringVals.add("YflLnT");
        stringVals.add("NkttZq");
        stringVals.add("NRoA0Q");
        stringVals.add("9KhI3y");
        stringVals.add("hPlpiG");
        stringVals.add("i2eBTc");
        stringVals.add("awv0ik");
        stringVals.add("JIcqo6");
        stringVals.add("yaEkK7");
        stringVals.add("T4DETz");
        stringVals.add("ecpT2p");
        stringVals.add("nWJ4NJ");
        stringVals.add("UMMJbb");
        stringVals.add("JKJ0zv");
        stringVals.add("NaVAXE");
        stringVals.add("zzKlOe");
        stringVals.add("otD6T6");
        stringVals.add("04c90d");
        stringVals.add("2nYgge");
        stringVals.add("3dGhih");
        stringVals.add("781RXF");
        stringVals.add("eJ0ttZ");
        stringVals.add("b1ocj2");
        stringVals.add("WSdqg7");
        stringVals.add("OaqOwx");
        stringVals.add("xTWj46");
        stringVals.add("epnkAd");
        stringVals.add("rI5RSZ");
        stringVals.add("sSkfCB");
        stringVals.add("wsFFNw");
        stringVals.add("hGoIda");
        stringVals.add("HaTWAW");
        stringVals.add("2sEln9");
        stringVals.add("4NXza6");
        stringVals.add("RxnU5b");
        stringVals.add("zAWyxS");
        stringVals.add("d0yIIg");
        stringVals.add("8xwCdY");
        stringVals.add("22oQtO");
        stringVals.add("OkY96A");
        stringVals.add("0fFwYZ");
        stringVals.add("GQ9Dmr");
        stringVals.add("2HBgcW");
        stringVals.add("MaqLxn");
        stringVals.add("sTUDPh");
        stringVals.add("sJS5Ew");
        stringVals.add("DiwiYr");
        stringVals.add("EZKEkK");
        stringVals.add("AD1K5J");
        stringVals.add("atabKE");
        stringVals.add("lB4vkl");
        stringVals.add("3qdilS");
        stringVals.add("GU1vJH");
        stringVals.add("KHNOej");
        stringVals.add("xwDDMK");
        stringVals.add("PUfNFo");
        stringVals.add("QHch3W");
        stringVals.add("btTIhb");
        stringVals.add("JsLwcm");
        stringVals.add("sIMsyL");
        stringVals.add("n02Nz4");
        stringVals.add("GYA2sT");
        stringVals.add("ohrk7w");
        stringVals.add("jBYw9w");
        stringVals.add("g2H60A");
        stringVals.add("DJAaSd");
        stringVals.add("tiaYSQ");
        stringVals.add("AvnH1J");
        stringVals.add("g2r5rh");
        stringVals.add("6GEx8v");
        stringVals.add("GgbijX");
        stringVals.add("tiK2gB");
        stringVals.add("jnE57q");
        stringVals.add("3UjAQh");
        stringVals.add("Q46fw1");
        stringVals.add("C85zkK");
        stringVals.add("x110ne");
        stringVals.add("6zOjkR");
        stringVals.add("vIGoYv");
        stringVals.add("paWYbc");
        stringVals.add("xQ3OKK");
        stringVals.add("qt0fNw");
        stringVals.add("5JcAHh");
        stringVals.add("5p8bXP");
        stringVals.add("odcOPR");
        stringVals.add("pHuA5J");
        stringVals.add("koOFoG");
        stringVals.add("lUgLIe");
        stringVals.add("C9bhtR");
        stringVals.add("7QtHzP");
        stringVals.add("MT2cCk");
        stringVals.add("8ptjvA");
        stringVals.add("mWs2AR");
        stringVals.add("hAyRSP");
        stringVals.add("v0XaYK");
        stringVals.add("khiM99");
        stringVals.add("o9hQ37");
        stringVals.add("K9zjK8");
        stringVals.add("iAMLya");
        stringVals.add("lJt4eJ");
        stringVals.add("ssr1Tt");
        stringVals.add("6w8nok");
        stringVals.add("GfkJZO");
        stringVals.add("u99inX");
        stringVals.add("oYKpoo");
        stringVals.add("z7lchg");
        stringVals.add("nmRW3X");
        stringVals.add("hx7dJj");
        stringVals.add("NjGtTh");
        stringVals.add("tL7dP4");
        stringVals.add("AxNQtu");
        stringVals.add("D1zwdm");
        stringVals.add("InWTM3");
        stringVals.add("cHgRgJ");
        stringVals.add("l8Zwlb");
        stringVals.add("1bHyIu");
        stringVals.add("YlCXIg");
        stringVals.add("NLpYlb");
        stringVals.add("CWg8R3");
        stringVals.add("blG1eX");
        stringVals.add("yolJ6M");
        stringVals.add("nzjtWh");
        stringVals.add("zKh8z3");
        stringVals.add("Bi8kCE");
        stringVals.add("oBh2ix");
        stringVals.add("q2NFqi");
        stringVals.add("IsTnnG");
        stringVals.add("AdnbQg");
        stringVals.add("3THOKT");
        stringVals.add("8T8rWe");
        stringVals.add("ynZurt");
        stringVals.add("iY2BYb");
        stringVals.add("tzxqXC");
        stringVals.add("DxmgDq");
        stringVals.add("EtfKac");
        stringVals.add("m15TKW");
        stringVals.add("vAONHa");
        stringVals.add("r7USP5");
        stringVals.add("ZSep7i");
        stringVals.add("WG9D6m");
        stringVals.add("krvThE");
        stringVals.add("rIcvSQ");
        stringVals.add("tkOJfh");
        stringVals.add("QjlDwE");
        stringVals.add("Ro29kj");
        stringVals.add("L0g74d");
        stringVals.add("vXBrmf");
        stringVals.add("D64uit");
        stringVals.add("h7uOfJ");
        stringVals.add("tIbnLw");
        stringVals.add("U6Unu2");
        stringVals.add("rDpyc0");
        stringVals.add("2rU6fy");
        stringVals.add("aGLjaY");
        stringVals.add("RYFPxC");
        stringVals.add("3N0ybj");
        stringVals.add("aHJEnD");
        stringVals.add("JyS36d");
        stringVals.add("Yi0VjX");
        stringVals.add("TeSr1X");
        stringVals.add("TJQU5m");
        stringVals.add("HhR0DZ");
        stringVals.add("QFA2TR");
        stringVals.add("zhxnX4");
        stringVals.add("VJYnaU");
        stringVals.add("0VTuao");
        stringVals.add("rUybzH");
        stringVals.add("5p05Gu");
        stringVals.add("MBLfkP");
        stringVals.add("Zw50Ig");
        stringVals.add("0Kdyof");
        stringVals.add("uTPB6Y");
        stringVals.add("ywDlRH");
        stringVals.add("AlwwDr");
        stringVals.add("YsSYSd");
        stringVals.add("Tnr7Rn");
        stringVals.add("ZU2Cy6");
        stringVals.add("DLdMGq");
        stringVals.add("oROcH2");
        stringVals.add("jLuZAF");
        stringVals.add("tS26OC");
        stringVals.add("E5Ih5u");
        stringVals.add("pFAMfC");
        stringVals.add("umSIPL");
        stringVals.add("JZd2OJ");
        stringVals.add("HaWlPV");
        stringVals.add("ExB57s");
        stringVals.add("pn1fJ3");
        stringVals.add("VnOXW4");
        stringVals.add("WoTTqq");
        stringVals.add("0yK44i");
        stringVals.add("FgYcCL");
        stringVals.add("X0eGHR");
        stringVals.add("gSNJyf");
        stringVals.add("9mlz3R");
        stringVals.add("aodSoM");
        stringVals.add("hrXD8g");
        stringVals.add("l5JQhv");
        stringVals.add("pO8ZXL");
        stringVals.add("FnNkK2");
        stringVals.add("p58d72");
        stringVals.add("RaOkNB");
        stringVals.add("KBFCIS");
        stringVals.add("9pxV7V");
        stringVals.add("VWLD6j");
        stringVals.add("qnbaso");
        stringVals.add("9w2uOo");
        stringVals.add("uenUJu");
        stringVals.add("V951PG");
        stringVals.add("zj2Gys");
        stringVals.add("Zguwyc");
        stringVals.add("L1ZCYf");
        stringVals.add("1sAZg2");
        stringVals.add("FFpXAk");
        stringVals.add("Umoj9f");
        stringVals.add("oxUfZK");
        stringVals.add("eW417Y");
        stringVals.add("rMeyuP");
        stringVals.add("C3TLGU");
        stringVals.add("sp3wHn");
        stringVals.add("dr0x0J");
        stringVals.add("Yl72A3");
        stringVals.add("fluFN2");
        stringVals.add("GBKqCb");
        stringVals.add("ENgnNi");
        stringVals.add("DDjdLE");
        stringVals.add("Oj5bfb");
        stringVals.add("VmoiGD");
        stringVals.add("pTefbr");
        stringVals.add("8J825L");
        stringVals.add("N68K4E");
        stringVals.add("7DuauE");
        stringVals.add("Gm9PS6");
        stringVals.add("RrSoyr");
        stringVals.add("Tb7Dts");
        stringVals.add("AamtVA");
        stringVals.add("CFyfbQ");
        stringVals.add("nudQep");
        stringVals.add("Aph5hu");
        stringVals.add("7E6yn8");
        stringVals.add("cTCbfD");
        stringVals.add("AtVGey");
        stringVals.add("pAXZ2o");
        stringVals.add("dkp1eF");
        stringVals.add("wUbDS2");
        stringVals.add("7FldFs");
        stringVals.add("cSUFs8");
        stringVals.add("btR7HY");
        stringVals.add("mnQX74");
        stringVals.add("8t2s9i");
        stringVals.add("G3aHKm");
        stringVals.add("dDZyhC");
        stringVals.add("D5nnVB");
        stringVals.add("WifzAy");
        stringVals.add("eSo6WX");
        stringVals.add("pUjnUp");
        stringVals.add("PtagFk");
        stringVals.add("izsHMF");
        stringVals.add("lDCfmM");
        stringVals.add("FkBguK");
        stringVals.add("GdV6MN");
        stringVals.add("6UBXM3");
        stringVals.add("FAodGq");
        stringVals.add("91iD8d");
        stringVals.add("vn4oZT");
        stringVals.add("mLyy2W");
        stringVals.add("X1PHwl");
        stringVals.add("BEp5ZG");
        stringVals.add("CNIzhn");
        stringVals.add("eema6Q");
        stringVals.add("DnU1NK");
        stringVals.add("OPSjYK");
        stringVals.add("BAsHfB");
        stringVals.add("Qbq53X");
        stringVals.add("IAsYf4");
        stringVals.add("t9Mr0F");
        stringVals.add("xXGSbA");
        stringVals.add("DZhm0V");
        stringVals.add("8tS30r");
        stringVals.add("iW80YS");
        stringVals.add("TOU5cM");
        stringVals.add("QaJyER");
        stringVals.add("9cGC55");
        stringVals.add("MDUPR5");
        stringVals.add("5gfOZI");
        stringVals.add("sexOL6");
        stringVals.add("3oIB2e");
        stringVals.add("8gpvGK");
        stringVals.add("ZeY2Xs");
        stringVals.add("gV8ZJn");
        stringVals.add("2spzj2");
        stringVals.add("zZKmuc");
        stringVals.add("oQYrKq");
        stringVals.add("1LaF1N");
        stringVals.add("TXXxUq");
        stringVals.add("1mnDJA");
        stringVals.add("63HvK2");
        stringVals.add("4tS0Qz");
        stringVals.add("j5kq0X");
        stringVals.add("r0dN1a");
        stringVals.add("gjp07H");
        stringVals.add("T8XCJb");
        stringVals.add("AsyQZv");
        stringVals.add("HelZ27");
        stringVals.add("0WJJPh");
        stringVals.add("lEN0Jg");
        stringVals.add("RUW83W");
        stringVals.add("5sLqZj");
        stringVals.add("jMkfP9");
        stringVals.add("VREjvs");
        stringVals.add("xgJD6s");
        stringVals.add("v3DZFb");
        stringVals.add("elMpHw");
        stringVals.add("6jhX6n");
        stringVals.add("yUtick");
        stringVals.add("dlIMvY");
        stringVals.add("cTDSIy");
        stringVals.add("e6BmvR");
        stringVals.add("KcaH7s");
        stringVals.add("IsXazc");
        stringVals.add("K3GoXM");
        stringVals.add("HqWt6f");
        stringVals.add("R8bV3L");
        stringVals.add("AalMtm");
        stringVals.add("lt7H5E");
        stringVals.add("gp66it");
        stringVals.add("z8dp8K");
        stringVals.add("ERMENd");
        stringVals.add("grtdBQ");
        stringVals.add("ygYvpN");
        stringVals.add("SKt00S");
        stringVals.add("ksm20F");
        stringVals.add("AP8IAt");
        stringVals.add("NzCodd");
        stringVals.add("1cwmzX");
        stringVals.add("YHPuam");
        stringVals.add("uvmYuK");
        stringVals.add("Y6dC19");
        stringVals.add("fOwo9Z");
        stringVals.add("YUAdw4");
        stringVals.add("vKkcBf");
        stringVals.add("V8fMtC");
        stringVals.add("d9ilk6");
        stringVals.add("FilEPf");
        stringVals.add("zN9PSq");
        stringVals.add("t75jKe");
        stringVals.add("D366GK");
        stringVals.add("bnkxZq");
        stringVals.add("mlY0Rs");
        stringVals.add("UZNmct");
        stringVals.add("kiTHGz");
        stringVals.add("A6e1bp");
        stringVals.add("rj1TvR");
        stringVals.add("7SBsWJ");
        stringVals.add("xYQbDZ");
        stringVals.add("2wZSJO");
        stringVals.add("FHDhRM");
        stringVals.add("jeSvTy");
        stringVals.add("62gbhe");
        stringVals.add("sI6HDF");
        stringVals.add("NQrbCA");
        stringVals.add("WgNtYB");
        stringVals.add("f9HGRz");
        stringVals.add("hpaIIp");
        stringVals.add("K5iacx");
        stringVals.add("szqC4e");
        stringVals.add("dcfDLz");
        stringVals.add("OihNU8");
        stringVals.add("xsnYpz");
        stringVals.add("NoKyJZ");
        stringVals.add("8KTEgC");
        stringVals.add("KWWKDb");
        stringVals.add("GgPHs8");
        stringVals.add("kDNcI9");
        stringVals.add("wIwqsy");
        stringVals.add("UmlInd");
        stringVals.add("xK5sti");
        stringVals.add("kKzq9y");
        stringVals.add("7uiKi4");
        stringVals.add("CSxQVw");
        stringVals.add("Txcgxp");
        stringVals.add("BLKsda");
        stringVals.add("JLP9gz");
        stringVals.add("vtD3C6");
        stringVals.add("xz9jTF");
        stringVals.add("BB8QyZ");
        stringVals.add("meIjTd");
        stringVals.add("8qo6JZ");
        stringVals.add("dy2UqN");
        stringVals.add("wVK8UJ");
        stringVals.add("Jc4NH8");
        stringVals.add("TQ8bk6");
        stringVals.add("4Lb77d");
        stringVals.add("Idpyc8");
        stringVals.add("x9hzVV");
        stringVals.add("iDh9rS");
        stringVals.add("KzGSJu");
        stringVals.add("uQxDmt");
        stringVals.add("qm2bs0");
        stringVals.add("RuS3M0");
        stringVals.add("QAIvD0");
        stringVals.add("t8zWPb");
        stringVals.add("VKFunI");
        stringVals.add("cROmrn");
        stringVals.add("Xv7Ivq");
        stringVals.add("CPhyqn");
        stringVals.add("Pz2ypB");
        stringVals.add("KtZS0L");
        stringVals.add("1VHJgC");
        stringVals.add("epj6gk");
        stringVals.add("YBhJcT");
        stringVals.add("KnddW4");
        stringVals.add("xBfMKM");
        stringVals.add("XDlZkV");
        stringVals.add("1qs1xp");
        stringVals.add("455hg1");
        stringVals.add("BnzEvy");
        stringVals.add("tspTdS");
        stringVals.add("lpwEKc");
        stringVals.add("9FCuAd");
        stringVals.add("bj1gKo");
        stringVals.add("ab80bG");
        stringVals.add("BWpGcl");
        stringVals.add("lMXcOv");
        stringVals.add("nHuG7l");
        stringVals.add("1RwL3O");
        stringVals.add("A8kgc1");
        stringVals.add("5KJGcC");
        stringVals.add("c7YyZw");
        stringVals.add("WsFLwq");
        stringVals.add("iG2ZkB");
        stringVals.add("RYk5Ib");
        stringVals.add("Dq6Lbi");
        stringVals.add("C3YLSI");
        stringVals.add("ueui53");
        stringVals.add("lVIw6q");
        stringVals.add("X8A8G0");
        stringVals.add("2I6UTe");
        stringVals.add("b6Ncsq");
        stringVals.add("YEVzHn");
        stringVals.add("gNFjhA");
        stringVals.add("nK7IFm");
        stringVals.add("PufPI6");
        stringVals.add("h7NhDh");
        stringVals.add("ovzqg2");
        stringVals.add("EiWrrc");
        stringVals.add("HxKCb8");
        stringVals.add("gCsLdx");
        stringVals.add("jq5W69");
        stringVals.add("xx5Lxz");
        stringVals.add("dUZsaK");
        stringVals.add("1ldkKt");
        stringVals.add("NjhunK");
        stringVals.add("1m09Mj");
        stringVals.add("pS1QYi");
        stringVals.add("IzZqui");
        stringVals.add("Q2YY4S");
        stringVals.add("2ahZ4l");
        stringVals.add("gaJGG5");
        stringVals.add("xj3blp");
        stringVals.add("xMhGUA");
        stringVals.add("1MlUVQ");
        stringVals.add("g1NDDq");
        stringVals.add("EY6vXO");
        stringVals.add("9cTQSZ");
        stringVals.add("MWuj4w");
        stringVals.add("hasr7r");
        stringVals.add("nWqpj5");
        stringVals.add("MgBiXt");
        stringVals.add("t65T7v");
        stringVals.add("wHlIco");
        stringVals.add("hRAHav");
        stringVals.add("If0jLA");
        stringVals.add("pBel9o");
        stringVals.add("tXxNdk");
        stringVals.add("uWRdwz");
        stringVals.add("Lbv3b8");
        stringVals.add("rW0V4L");
        stringVals.add("rKhcGH");
        stringVals.add("ztWe8U");
        stringVals.add("zLTNNO");
        stringVals.add("Ola90f");
        stringVals.add("ohOqgT");
        stringVals.add("mbZoS0");
        stringVals.add("PcDCiI");
        stringVals.add("56vH6o");
        stringVals.add("XMPBK7");
        stringVals.add("K5RNsR");
        stringVals.add("k9AZ4y");
        stringVals.add("JxnerK");
        stringVals.add("zFAtxy");
        stringVals.add("lfB8RK");
        stringVals.add("9sQ2gq");
        stringVals.add("cPaxfM");
        stringVals.add("rXX054");
        stringVals.add("wWYLN8");
        stringVals.add("Ni4st9");
        stringVals.add("hG3lCA");
        stringVals.add("rWTIjz");
        stringVals.add("P2B01G");
        stringVals.add("PnBv3g");
        stringVals.add("W7BDeN");
        stringVals.add("ipnNjI");
        stringVals.add("gF1LVD");
        stringVals.add("EWm8nh");
        stringVals.add("mIL0ef");
        stringVals.add("opuSW5");
        stringVals.add("BCUw88");
        stringVals.add("cn4dKT");
        stringVals.add("CNYAsx");
        stringVals.add("64aymk");
        stringVals.add("0MpN1R");
        stringVals.add("il8dgq");
        stringVals.add("TQKRtN");
        stringVals.add("zmt8p1");
        stringVals.add("NB2ztN");
        stringVals.add("b4BK74");
        stringVals.add("t3uNny");
        stringVals.add("2TL7lC");
        stringVals.add("xbQxTL");
        stringVals.add("m1QBnY");
        stringVals.add("H33uUn");
        stringVals.add("YYIOMC");
        stringVals.add("Uo16hH");
        stringVals.add("Eli8du");
        stringVals.add("PY3nmS");
        stringVals.add("PYLQRm");
        stringVals.add("eVlgM0");
        stringVals.add("zl99JF");
        stringVals.add("QIBt20");
        stringVals.add("OaaY6R");
        stringVals.add("G8U76E");
        stringVals.add("Ir06pg");
        stringVals.add("DVT2j5");

        List<String> deletedVals = new ArrayList<String>();
        deletedVals.add("DWLdcy");
        deletedVals.add("jL7OXc");
        deletedVals.add("xL4AOX");
        deletedVals.add("BhM92q");
        deletedVals.add("mjSVUp");
        deletedVals.add("QvHVXT");
        deletedVals.add("hTZirP");
        deletedVals.add("5FMjjz");
        deletedVals.add("WymqjW");
        deletedVals.add("bdYRTz");
        deletedVals.add("qbe7z7");
        deletedVals.add("0bwTpP");
        deletedVals.add("wMwKWR");
        deletedVals.add("Q98kPf");
        deletedVals.add("BhhhQF");
        deletedVals.add("l0Uzxn");
        deletedVals.add("ksNDO3");
        deletedVals.add("C2j50q");
        deletedVals.add("b3fliX");
        deletedVals.add("P7PSzd");
        deletedVals.add("vnuMAd");
        deletedVals.add("viJzON");
        deletedVals.add("7yOEjn");
        deletedVals.add("40CARS");
        deletedVals.add("0kz8Qa");
        deletedVals.add("hCg160");
        deletedVals.add("RovvLs");
        deletedVals.add("z7p8EL");
        deletedVals.add("J1xwIN");
        deletedVals.add("FPc0JB");
        deletedVals.add("4YGDE1");
        deletedVals.add("diytUA");
        deletedVals.add("Uao7u3");
        deletedVals.add("27bFr5");
        deletedVals.add("WqW8Bz");
        deletedVals.add("dvsr3B");
        deletedVals.add("qSLPoV");
        deletedVals.add("I9kOsc");
        deletedVals.add("xMh2Xh");
        deletedVals.add("C1eWEA");
        deletedVals.add("73bo7W");
        deletedVals.add("FtN6oj");
        deletedVals.add("WaWTSb");
        deletedVals.add("TR4UMf");
        deletedVals.add("Uh4x1D");
        deletedVals.add("HjFicA");
        deletedVals.add("fk3AR0");
        deletedVals.add("AIMPuu");
        deletedVals.add("sHVxmR");
        deletedVals.add("0cMSHC");
        deletedVals.add("1vNtj2");
        deletedVals.add("JeLaaZ");
        deletedVals.add("LNk7vT");
        deletedVals.add("oIakoY");
        deletedVals.add("dLHzQ4");
        deletedVals.add("5WqYGv");
        deletedVals.add("suIOlv");
        deletedVals.add("qrKKMq");
        deletedVals.add("Fmxe7S");
        deletedVals.add("XGxsEL");
        deletedVals.add("sDXmUO");
        deletedVals.add("5dg4hJ");
        deletedVals.add("voW03P");
        deletedVals.add("FLkUVo");
        deletedVals.add("O8ZVKw");
        deletedVals.add("8YqO12");
        deletedVals.add("33F7TK");
        deletedVals.add("zYv8aC");
        deletedVals.add("ODv5Ky");
        deletedVals.add("iz3N1i");
        deletedVals.add("mBAgLd");
        deletedVals.add("bjYxys");
        deletedVals.add("JieBhQ");
        deletedVals.add("FL0QM3");
        deletedVals.add("OQg9yC");
        deletedVals.add("uzzaq1");
        deletedVals.add("RVkmuq");
        deletedVals.add("KN3hBd");
        deletedVals.add("KiTYEm");
        deletedVals.add("KP3ZyZ");
        deletedVals.add("h0UgIy");
        deletedVals.add("AONOyN");
        deletedVals.add("RSV1gw");
        deletedVals.add("MwLl3P");
        deletedVals.add("LFvfTU");
        deletedVals.add("QwFaOP");
        deletedVals.add("e11AYM");
        deletedVals.add("DqEfiN");
        deletedVals.add("Dr00ys");
        deletedVals.add("qoyvDw");
        deletedVals.add("XqLS20");
        deletedVals.add("5xajbG");
        deletedVals.add("oZjHz9");
        deletedVals.add("UMTxx4");
        deletedVals.add("MwY9PR");
        deletedVals.add("JkD21q");
        deletedVals.add("NhOlRa");
        deletedVals.add("ICGSBQ");
        deletedVals.add("oZuXy3");
        deletedVals.add("LJ3hWF");
        deletedVals.add("imJI7g");
        deletedVals.add("T6ENXj");
        deletedVals.add("slt1cI");
        deletedVals.add("UosQAC");
        deletedVals.add("dNJ8Ye");
        deletedVals.add("XVYllu");
        deletedVals.add("wuq0c5");
        deletedVals.add("VTeWzU");
        deletedVals.add("6zvwiV");
        deletedVals.add("2jj6eV");
        deletedVals.add("POKBSd");
        deletedVals.add("Cwu4EK");
        deletedVals.add("wG6lbm");
        deletedVals.add("mJTQM6");
        deletedVals.add("4TYlwC");
        deletedVals.add("hCjGKB");
        deletedVals.add("VxIpM0");
        deletedVals.add("O7VhjP");
        deletedVals.add("8AzEh5");
        deletedVals.add("6K2wbh");
        deletedVals.add("YfVDI8");
        deletedVals.add("pWWC55");
        deletedVals.add("G38Qh5");
        deletedVals.add("OcQ3OX");
        deletedVals.add("7bZ9HX");
        deletedVals.add("vLuwwZ");
        deletedVals.add("oepP2E");
        deletedVals.add("vmo9lA");
        deletedVals.add("z4QYlx");
        deletedVals.add("u1oF8o");
        deletedVals.add("uAaV8p");
        deletedVals.add("XkQQ0H");
        deletedVals.add("HHWlaQ");
        deletedVals.add("jvzSeS");
        deletedVals.add("tQuq0f");
        deletedVals.add("EUkPQ0");
        deletedVals.add("TmpzWl");
        deletedVals.add("3kEshJ");
        deletedVals.add("K4hMSL");
        deletedVals.add("hzKvYp");
        deletedVals.add("pN3Gz6");
        deletedVals.add("dMVk9D");
        deletedVals.add("u8NoY4");
        deletedVals.add("3VElw2");
        deletedVals.add("Gdh177");
        deletedVals.add("jLE46v");
        deletedVals.add("IkG3RV");
        deletedVals.add("sbMUK6");
        deletedVals.add("KzQEgK");
        deletedVals.add("QYJbRP");
        deletedVals.add("9eZNJD");
        deletedVals.add("sWJWU1");
        deletedVals.add("O4i1Jp");
        deletedVals.add("KllQRE");
        deletedVals.add("yNJfdh");
        deletedVals.add("s0IxAi");
        deletedVals.add("Je8DAp");
        deletedVals.add("jAEe4k");
        deletedVals.add("Dg7aT0");
        deletedVals.add("QBCAaN");
        deletedVals.add("sxNpX0");
        deletedVals.add("G50aHa");
        deletedVals.add("qwFwy4");
        deletedVals.add("yovKU2");
        deletedVals.add("wHinWy");
        deletedVals.add("2L53ul");
        deletedVals.add("WG3u57");
        deletedVals.add("QpWuBT");
        deletedVals.add("qd6Pbs");
        deletedVals.add("azoPHo");
        deletedVals.add("Cgl49F");
        deletedVals.add("Lf3grL");
        deletedVals.add("eBtxxe");
        deletedVals.add("UIUN0u");
        deletedVals.add("REAvwj");
        deletedVals.add("9kPThi");
        deletedVals.add("IwdKbv");
        deletedVals.add("5ocU0A");
        deletedVals.add("GuIaPi");
        deletedVals.add("Vq2g76");
        deletedVals.add("HUIC5t");
        deletedVals.add("VOHpK8");
        deletedVals.add("xjwzhh");
        deletedVals.add("0aw2qT");
        deletedVals.add("EfzABi");
        deletedVals.add("Nua6T1");
        deletedVals.add("ePgL82");
        deletedVals.add("fegMBS");
        deletedVals.add("XxmX6M");
        deletedVals.add("JRHdrY");
        deletedVals.add("Vatu4j");
        deletedVals.add("9QjKYj");
        deletedVals.add("ObHPUj");
        deletedVals.add("h3hiR3");
        deletedVals.add("4TyCJc");
        deletedVals.add("FjRFno");
        deletedVals.add("n8dFNQ");
        deletedVals.add("sR2D28");
        deletedVals.add("HkzI1w");
        deletedVals.add("q4EvCJ");
        deletedVals.add("8KRfg9");
        deletedVals.add("z1h0GR");
        deletedVals.add("HYZiFl");
        deletedVals.add("lr0RCn");
        deletedVals.add("Nt0b81");
        deletedVals.add("FNelS9");
        deletedVals.add("29Xl6A");
        deletedVals.add("KdEPuH");
        deletedVals.add("a3NxOI");
        deletedVals.add("Cy6PeR");
        deletedVals.add("E3xpdP");
        deletedVals.add("Eb6D78");
        deletedVals.add("pxNbUf");
        deletedVals.add("MuaQjS");
        deletedVals.add("0SzjMP");
        deletedVals.add("TNufro");
        deletedVals.add("YflLnT");
        deletedVals.add("NkttZq");
        deletedVals.add("NRoA0Q");
        deletedVals.add("9KhI3y");
        deletedVals.add("hPlpiG");
        deletedVals.add("i2eBTc");
        deletedVals.add("awv0ik");
        deletedVals.add("JIcqo6");
        deletedVals.add("yaEkK7");
        deletedVals.add("T4DETz");
        deletedVals.add("ecpT2p");
        deletedVals.add("nWJ4NJ");
        deletedVals.add("UMMJbb");
        deletedVals.add("JKJ0zv");
        deletedVals.add("NaVAXE");
        deletedVals.add("zzKlOe");
        deletedVals.add("otD6T6");
        deletedVals.add("04c90d");
        deletedVals.add("2nYgge");
        deletedVals.add("3dGhih");
        deletedVals.add("781RXF");
        deletedVals.add("eJ0ttZ");
        deletedVals.add("b1ocj2");
        deletedVals.add("WSdqg7");
        deletedVals.add("OaqOwx");
        deletedVals.add("xTWj46");
        deletedVals.add("epnkAd");
        deletedVals.add("rI5RSZ");
        deletedVals.add("sSkfCB");
        deletedVals.add("wsFFNw");
        deletedVals.add("hGoIda");
        deletedVals.add("HaTWAW");
        deletedVals.add("2sEln9");
        deletedVals.add("4NXza6");
        deletedVals.add("RxnU5b");
        deletedVals.add("zAWyxS");
        deletedVals.add("d0yIIg");
        deletedVals.add("8xwCdY");
        deletedVals.add("22oQtO");
        deletedVals.add("OkY96A");
        deletedVals.add("0fFwYZ");
        deletedVals.add("GQ9Dmr");
        deletedVals.add("2HBgcW");
        deletedVals.add("MaqLxn");
        deletedVals.add("sTUDPh");
        deletedVals.add("sJS5Ew");
        deletedVals.add("DiwiYr");
        deletedVals.add("EZKEkK");
        deletedVals.add("AD1K5J");
        deletedVals.add("atabKE");
        deletedVals.add("lB4vkl");
        deletedVals.add("3qdilS");
        deletedVals.add("GU1vJH");
        deletedVals.add("KHNOej");
        deletedVals.add("xwDDMK");
        deletedVals.add("PUfNFo");
        deletedVals.add("QHch3W");
        deletedVals.add("btTIhb");
        deletedVals.add("JsLwcm");
        deletedVals.add("sIMsyL");
        deletedVals.add("n02Nz4");
        deletedVals.add("GYA2sT");
        deletedVals.add("ohrk7w");
        deletedVals.add("jBYw9w");
        deletedVals.add("g2H60A");
        deletedVals.add("DJAaSd");
        deletedVals.add("tiaYSQ");
        deletedVals.add("AvnH1J");
        deletedVals.add("g2r5rh");
        deletedVals.add("6GEx8v");
        deletedVals.add("GgbijX");
        deletedVals.add("tiK2gB");
        deletedVals.add("jnE57q");
        deletedVals.add("3UjAQh");
        deletedVals.add("Q46fw1");
        deletedVals.add("C85zkK");
        deletedVals.add("x110ne");
        deletedVals.add("6zOjkR");
        deletedVals.add("vIGoYv");
        deletedVals.add("paWYbc");
        deletedVals.add("xQ3OKK");
        deletedVals.add("qt0fNw");
        deletedVals.add("5JcAHh");
        deletedVals.add("5p8bXP");
        deletedVals.add("odcOPR");
        deletedVals.add("pHuA5J");
        deletedVals.add("koOFoG");
        deletedVals.add("lUgLIe");
        deletedVals.add("C9bhtR");
        deletedVals.add("7QtHzP");
        deletedVals.add("MT2cCk");
        deletedVals.add("8ptjvA");
        deletedVals.add("mWs2AR");
        deletedVals.add("hAyRSP");
        deletedVals.add("v0XaYK");
        deletedVals.add("khiM99");
        deletedVals.add("o9hQ37");
        deletedVals.add("K9zjK8");

        for (String stringVal: stringVals) {
        	ByteArray val = new ByteArray(stringVal);
            assertTrue(set.add(val));
            assertTrue(set.contains(val));
        }
        assertEquals(750, set.size());

        for (String deletedVal: deletedVals) {
        	ByteArray val = new ByteArray(deletedVal);
            assertTrue(set.remove(val));
            assertFalse(set.contains(val));
        }
        assertEquals(436, set.size());

        assertEquals(436, ByteArraySet.deserialize(set.serialize()).size());
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
