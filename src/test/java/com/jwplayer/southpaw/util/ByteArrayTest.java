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

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;


public class ByteArrayTest {
    @Test
    public void testBoolean() {
        byte[] bytes = { 1 };
        ByteArray byteArray = ByteArray.toByteArray(true);

        assertTrue(Arrays.equals(bytes, byteArray.getBytes()));
    }

    @Test
    public void testByte() {
        byte[] bytes = { 9, 9, 9 };
        ByteArray byteArray = ByteArray.toByteArray(bytes);

        assertTrue(Arrays.equals(bytes, byteArray.getBytes()));
    }

    @Test
    public void testConcat() {
        ByteArray byteArray = new ByteArray("Test");
        byteArray.concat(new ByteArray(1));

        assertTrue(Arrays.equals(ArrayUtils.add("Test".getBytes(), (byte) 1), byteArray.getBytes()));
    }

    @Test
    public void testEquals() {
        ByteArray left = new ByteArray(1234);
        ByteArray right = new ByteArray(1234L);
        ByteArray right2 = new ByteArray("Banana");

        assertTrue(left.equals(right));
        assertFalse(left.equals(right2));
    }

    @Test
    public void testFromBytes() throws Exception {
        byte[] bytes = Hex.decodeHex("01410204d20100".toCharArray());
        Set<ByteArray> set = new HashSet<>(3);
        set.add(new ByteArray("A"));
        set.add(new ByteArray(1234));
        set.add(new ByteArray(false));
        assertEquals(set, ByteArray.fromBytes(bytes));
    }

    @Test
    public void testHashCode() {
        ByteArray byteArray = new ByteArray(1234);

        assertEquals(1039, byteArray.hashCode());
    }

    @Test
    public void testInteger() throws Exception {
        ByteArray byteArray = ByteArray.toByteArray(1234);

        assertTrue(Arrays.equals(Hex.decodeHex("04d2".toCharArray()), byteArray.getBytes()));
    }

    @Test
    public void testLong() throws Exception {
        ByteArray byteArray = ByteArray.toByteArray(1234L);

        assertTrue(Arrays.equals(Hex.decodeHex("04d2".toCharArray()), byteArray.getBytes()));
    }

    @Test
    public void testNull() {
        assertNull(ByteArray.toByteArray(null));
    }

    @Test
    public void testString() {
        ByteArray byteArray = ByteArray.toByteArray("Test");

        assertTrue(Arrays.equals("Test".getBytes(), byteArray.getBytes()));
    }

    @Test
    public void testToBytes() throws Exception {
        byte[] bytes = Hex.decodeHex("01410204d20100".toCharArray());
        List<ByteArray> list = new ArrayList<>(3);
        list.add(new ByteArray("A"));
        list.add(new ByteArray(1234));
        list.add(new ByteArray(false));
        assertTrue(Arrays.equals(bytes, ByteArray.toBytes(list)));
    }

    @Test
    public void testToString() {
        ByteArray byteArray = ByteArray.toByteArray(1234L);

        assertEquals("04d2", byteArray.toString());
    }
}
