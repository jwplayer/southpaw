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

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.jwplayer.southpaw.record.BaseRecord;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.kafka.common.utils.Bytes;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;


/**
 * Wrapper class for byte arrays so we can use them as keys in maps.
 */
public class ByteArray implements Comparable<ByteArray>, Serializable {
    private static final long serialVersionUID = -6277178128299377659L;
    private static final Bytes.ByteArrayComparator comparator = Bytes.BYTES_LEXICO_COMPARATOR;
    private byte[] bytes;

    /**
     * Default constructor
     */
    public ByteArray() {
        this.bytes = new byte[0];
    }

    public ByteArray(boolean b) {
        this.bytes = new byte[1];
        this.bytes[0] = b ? (byte) 1 : (byte) 0;
    }

    /**
     * Constructor
     * @param bytes - byte array to wrap
     */
    public ByteArray(byte[] bytes) {
        Preconditions.checkNotNull(bytes);
        this.bytes = bytes;
    }

    /**
     * Constructor
     * @param i - Integer to convert to a byte array and wrap
     */
    public ByteArray(int i) {
        this.bytes = removeZeroes(Ints.toByteArray(i));
    }

    /**
     * Constructor
     * @param l - Long to convert to a byte array and wrap
     */
    public ByteArray(long l) {
        this.bytes = removeZeroes(Longs.toByteArray(l));
    }

    /**
     * Constructor
     * @param s - String to convert to a byte array using the default charset and wrap
     */
    public ByteArray(String s) {
        Preconditions.checkNotNull(s);
        this.bytes = s.getBytes();
    }

    @Override
    public int compareTo(ByteArray byteArray) {
        return comparator.compare(this.getBytes(), byteArray.getBytes());
    }

    /**
     * Allows comparing this byte array to a segment of another byte array
     * @param bytes - The byte array to compare to
     * @param offset - The offset to start the comparison at
     * @param length - The number of bytes to compare against
     * @return Standard comparison value based on this byte array and the provided byte array
     */
    public int compareTo(byte[] bytes, int offset, int length) {
        return comparator.compare(this.getBytes(), 0, this.getBytes().length, bytes, offset, length);
    }

    /**
     * Concatenates another byte array to this one
     * @param byteArray - The byte array to concatenate to this one
     */
    public void concat(ByteArray byteArray) {
        bytes = ArrayUtils.addAll(bytes, byteArray.getBytes());
    }

    /**
     * Determines if this ByteArray is equal to the given object.
     * @param object - The object to check for equality
     * @return - True if the object is equal to this ByteArray, otherwise false
     */
    @Override
    public boolean equals(Object object) {
        return object instanceof ByteArray && Arrays.equals(bytes, ((ByteArray) object).getBytes());
    }

    /**
     * Converts the bytes into a set of ByteArrays. Opposite of toBytes().
     * @param bytes - The bytes to convert
     * @return - A collection of ByteArrays
     */
    public static Set<ByteArray> fromBytes(byte[] bytes) {
        Set<ByteArray> set = new HashSet<>();
        int index = 0;
        while(index < bytes.length) {
            int size = Ints.fromBytes((byte) 0, (byte) 0, (byte) 0, bytes[index]);
            index++;
            set.add(new ByteArray(Arrays.copyOfRange(bytes, index, index + size)));
            index += size;
        }
        return set;
    }

    /**
     * Accessor for the wrapped byte array
     * @return - The wrapped byte array
     */
    public byte[] getBytes() {
        return bytes;
    }

    /**
     * Overrides hashcode so it returns a deterministic value based on the contents of the byte array.
     * @return - Hash code
     */
    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    /**
     * Removes preceding zeroes from the byte array to save space since they are not meaningful
     * @param bytes - The byte array to remove preceding zeroes from
     * @return A new byte array without the preceding zeroes
     */
    protected static byte[] removeZeroes(byte[] bytes) {
        int index = 0;
        for(byte b: bytes) {
            if(b == (byte) 0) {
                index++;
            } else {
                break;
            }
        }
        if(index == bytes.length) {
            return Arrays.copyOfRange(bytes, bytes.length - 1, bytes.length);
        } else if(index > 0) {
            return Arrays.copyOfRange(bytes, index, bytes.length);
        } else {
            return bytes;
        }
    }

    /**
     * Gets the size of the internal byte array
     * @return The size of the internal byte array
     */
    public int size() {
        return bytes.length;
    }

    /**
     * Converts the given object (String, Integer, Long) into a ByteArray. Other types are not supported.
     * @param object - The object to convert
     * @return A ByteArray wrapper for a byte array
     */
    public static ByteArray toByteArray(Object object) {
        if(object == null) {
            return null;
        } else if(object instanceof String) {
            return new ByteArray((String) object);
        } else if(object instanceof Integer) {
            return new ByteArray((Integer) object);
        } else if(object instanceof Long) {
            return new ByteArray((Long) object);
        } else if(object instanceof byte[]) {
            return new ByteArray((byte[]) object);
        } else if(object instanceof Boolean) {
            return new ByteArray((Boolean) object);
        } else if(object instanceof BaseRecord) {
            return ((BaseRecord) object).toByteArray();
        } else {
            throw new RuntimeException(String.format("Cannot convert type %s into a byte array", object.getClass()));
        }
    }

    /**
     * Converts the given collection of ByteArrays into bytes. Opposite of fromBytes().
     * @param collection - The collection to convert
     * @return A byte array representation of the collection
     */
    public static byte[] toBytes(Collection<ByteArray> collection) {
        int size = 0;
        for (ByteArray byteArray : collection) {
            Preconditions.checkArgument(byteArray.getBytes().length <= 255);
            size++;
            size += byteArray.getBytes().length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (ByteArray byteArray : collection) {
            byte sizeByte = Ints.toByteArray(byteArray.getBytes().length)[3];
            buffer.put(sizeByte);
            buffer.put(byteArray.getBytes());
        }
        return buffer.array();
    }

    /**
     * Gives a nice readable String version of the wrapped byte array. Useful for debugging.
     * @return - A pretty string representation
     */
    @JsonValue
    public String toString() {
        return Hex.encodeHexString(bytes);
    }
}
