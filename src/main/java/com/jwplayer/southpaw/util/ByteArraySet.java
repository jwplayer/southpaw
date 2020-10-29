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
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import java.nio.ByteBuffer;
import java.util.*;


/**
 * A set class that optimizes (de)serialization while maintaining good (O(log n)) put/remove/contains performance,
 * especially for large index entries (e.g. user_id -> media_id)
 */
public class ByteArraySet implements Set<ByteArray> {
    /**
     * Enum for the different byte array formats
     */
    public enum FORMAT {
        EMPTY((byte) 0),
        SINGLE_VALUE((byte) 1),
        SINGLE_CHUNK((byte) 2),
        MULTI_CHUNK((byte) 3);

        private byte value;

        FORMAT(byte value) {
            this.value = value;
        }

        public byte getValue() {
            return value;
        }

        public static FORMAT valueOf(byte b) {
            for(FORMAT value: FORMAT.values()) {
                if(value.getValue() == b) return value;
            }
            throw new EnumConstantNotPresentException(FORMAT.class, ((Byte) b).toString());
        }
    }

    /**
     * Static empty chunk for easy reference
     */
    public static final byte[] EMPTY_SET_BYTES = { FORMAT.EMPTY.getValue() };
    /**
     * Force a max size on the fronting set to prevent uncontrolled growth and OOM errors
     */
    public static final int MAX_FRONTING_SET_SIZE = 1000;

    /**
     * A chunk contains a sorted set of ByteArrays within a single byte array. The set values are formatted such that
     * the first byte is the size of an individual ByteArray value, then the ByteArray after that. The chunk also
     * contains information around it's min and max values, as well as the number of entries and the overall size
     * of byte array chunk.
     */
    protected static class Chunk {
        /**
         * The max size of the byte array in a chunk. Chunk byte arrays may be smaller than this length.
         */
        public static final int MAX_CHUNK_SIZE = 4096;
        /**
         * The representation of the chunk as a byte array
         */
        public byte[] bytes;
        /**
         * The number of entries contained in the chunk
         */
        public int entries;
        /**
         * The maximum byte array value contained within the chunk
         */
        public ByteArray max;
        /**
         * The minimum byte array value contained within the chunk
         */
        public ByteArray min;
        /**
         * The actual size of the byte array data, not including trailing 0s. For example, if the full size of
         * byte array data was 4k, but it only contained 10 entries, the actual size might be 40.
         */
        public int size;

        /**
         * Constructor
         * @param chunkSize - The size of the internal byte array
         */
        public Chunk(int chunkSize) {
            this.bytes = new byte[chunkSize];
            this.size = 0;
        }

        /**
         * Constructor
         * @param bytes - The internal byte array
         * @param entries - The number of entries in the byte array
         * @param max - The largest entry in this chunk
         * @param min - The smallest entry in this chunk
         * @param size - The actual size of the data in the internal byte array, NOT the full size of the internal
         *             byte array
         */
        public Chunk(byte[] bytes, int entries, ByteArray max, ByteArray min, int size) {
            this.bytes = bytes;
            this.entries = entries;
            this.max = max;
            this.min = min;
            this.size = size;
        }

        /**
         * Adds a non-null, non-empty entry to the byte array. Note that this does not ensure the ordering of the
         * entries in the byte array, nor does it check if this is the largest entry. It does update the number
         * of entries, total size and the min and max entries, assuming that this is the largest entry.
         * @param byteArray - The ByteArray to add to this chunk
         */
        public void add(ByteArray byteArray) {
            if(byteArray != null && byteArray.size() > 0){
                if(min == null) min = byteArray;
                max = byteArray;
                entries++;
                bytes[size] = (byte) byteArray.size();
                System.arraycopy(byteArray.getBytes(), 0, bytes, size + 1, byteArray.size());
                size += 1 + byteArray.size();
            }
        }

        /**
         * Determines if this chunk contains the given ByteArray
         * @param byteArray - The ByteArray to check membership for
         * @return True if this chunk contains the given ByteArray, otherwise false
         */
        public boolean contains(ByteArray byteArray) {
            if(entries == 0) return false;
            if(byteArray == null || byteArray.size() == 0) return false;
            if(min.compareTo(byteArray) > 0 || max.compareTo(byteArray) < 0) return false;
            int index = 0;
            byte[] baBytes = byteArray.getBytes();
            while(index < size) {
                int baSize = (int) bytes[index];
                int i;
                index++;
                if(baSize == baBytes.length) {
                    for(i = 0; i < baSize; i++) {
                        if(baBytes[i] != bytes[index + i]) break;
                    }
                    if(i == baSize) return true;
                }
                index += baSize;
            }
            return false;
        }

        /**
         * Deserializes a segment of a byte array into a Chunk
         * @param bytes - The bytes to deserialize a subsection of
         * @param from - The start of the serialized Chunk segment
         * @param to - The end of the serialized Chunk segment
         * @return - A deserialized Chunk instance
         */
        public static Chunk deserialize(byte[] bytes, int from, int to) {
            int size = Ints.fromBytes(bytes[to - 3], bytes[to - 2], bytes[to - 1], bytes[to]);
            int maxSize = (int) bytes[to - 4];
            ByteArray max = new ByteArray(Arrays.copyOfRange(bytes, to - 4 - maxSize, to - 4));
            int minSize = (int) bytes[to - 5 - maxSize];
            ByteArray min = new ByteArray(Arrays.copyOfRange(bytes, to - 5 - maxSize - minSize, to - 5 - maxSize));
            size = size - 2 - minSize - maxSize - Integer.BYTES;
            int entries = 0;
            int index = 0;
            byte[] chunkBytes = Arrays.copyOfRange(bytes, from, from + size);
            while(index < size) {
                int baSize = (int) chunkBytes[index];
                if(baSize != 0) entries++;
                index += 1 + baSize;
            }
            return new Chunk(chunkBytes, entries, max, min, size);
        }

        /**
         * Deserializes a segment of a byte array into a Chunk, assuming it was serialized into a compact format
         * @param bytes - The bytes to deserialize a subsection of
         * @param from - The start of the serialized Chunk segment
         * @param to - The end of the serialized Chunk segent
         * @return - A deserialized Chunk instance
         */
        public static Chunk deserializeCompact(byte[] bytes, int from, int to) {
            byte[] chunkBytes = Arrays.copyOfRange(bytes, from, to);
            int entries = 0;
            int index = 0;
            ByteArray min = null;
            ByteArray max = null;
            while(index < chunkBytes.length) {
                int baSize = (int) chunkBytes[index];
                index++;
                if(baSize != 0) {
                    entries++;
                    if(min == null) min = new ByteArray(Arrays.copyOfRange(chunkBytes, index, index + baSize));
                    max = new ByteArray(Arrays.copyOfRange(chunkBytes, index, index + baSize));
                }
                index += baSize;
            }
            return new Chunk(chunkBytes, entries, max, min, to - from);
        }

        /**
         * Removes the given ByteArray from this chunk by filling in its section of the chunk with 0s
         * @param byteArray - The ByteArray to remove
         * @return True if the value was removed, otherwise false.
         */
        public boolean remove(ByteArray byteArray) {
            if(entries == 0 || byteArray == null || byteArray.size() == 0) return false;
            if(min.compareTo(byteArray) > 0 || max.compareTo(byteArray) < 0) return false;
            int index = 0;
            byte[] baBytes = byteArray.getBytes();
            while(index < size) {
                int baSize = (int) bytes[index];
                int i;
                index++;
                if(baSize == baBytes.length) {
                    for(i = 0; i < baSize; i++) {
                        if(baBytes[i] != bytes[index + i]) break;
                    }
                    if(i == baSize) {
                        bytes[index - 1] = 0;
                        for(i = 0; i < baSize; i++) {
                            bytes[index + i] = 0;
                        }
                        entries--;
                        if(entries > 0) {
                            if(min.equals(byteArray)) {
                                index = 0;
                                while(index < size) {
                                    baSize = (int) bytes[index];
                                    index++;
                                    if(baSize != 0) {
                                        min = new ByteArray(Arrays.copyOfRange(bytes, index, index + baSize));
                                        break;
                                    }
                                    index += baSize;
                                }
                            }
                            if(max.equals(byteArray)) {
                                index = 0;
                                while(index < size) {
                                    baSize = (int) bytes[index];
                                    index++;
                                    if(baSize != 0) {
                                        max = new ByteArray(Arrays.copyOfRange(bytes, index, index + baSize));
                                    }
                                    index += baSize;
                                }
                            }
                        }
                        return true;
                    }
                }
                index += baSize;
            }
            return false;
        }

        /**
         * Serializes this chunk into a byte array representation
         * bytes / min size (1 byte) / min / max size (1 byte) / max / bytes size (4 bytes)
         * @return This chunk as a byte array
         */
        public byte[] serialize() {
            int bufferSize = size + 2 + min.size() + max.size() + Integer.BYTES;
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
            buffer.put(Arrays.copyOfRange(bytes, 0, size));
            buffer.put(min.getBytes());
            buffer.put((byte) min.size());
            buffer.put(max.getBytes());
            buffer.put((byte) max.size());
            buffer.put(Ints.toByteArray(size + 2 + min.size() + max.size() + Integer.BYTES));
            return buffer.array();
        }
    }

    protected class ChunksIterator implements Iterator<ByteArray> {
        protected Chunk currentChunk = null;
        protected int currentOffset = 0;
        protected Iterator<Chunk> chunksIter;

        protected ChunksIterator() {
            this.chunksIter = chunks.iterator();
            if(chunksIter.hasNext()) currentChunk = chunksIter.next();
        }

        @Override
        public boolean hasNext() {
            return currentChunk != null && currentOffset < currentChunk.size;
        }

        @Override
        public ByteArray next() {
            int size = (int) currentChunk.bytes[currentOffset];
            if(size == 0) {
                currentOffset += 1;
                return null;
            }
            byte[] retVal = Arrays.copyOfRange(currentChunk.bytes, currentOffset + 1, currentOffset + size + 1);
            currentOffset += 1 + size;
            if(currentOffset >= currentChunk.size) {
                if(chunksIter.hasNext()) {
                    currentChunk = chunksIter.next();
                    currentOffset = 0;
                } else {
                    currentChunk = null;
                }
            }
            return new ByteArray(retVal);
        }
    }

    protected class FullIterator implements Iterator<ByteArray> {
        protected ChunksIterator chunksIterator;
        protected Iterator<ByteArray> frontingSetIterator;

        protected FullIterator() {
            this.chunksIterator = new ChunksIterator();
            this.frontingSetIterator = frontingSet.iterator();
        }

        @Override
        public boolean hasNext() {
            return chunksIterator.hasNext() || frontingSetIterator.hasNext();
        }

        @Override
        public ByteArray next() {
            if(frontingSetIterator.hasNext()) {
                return frontingSetIterator.next();
            } else {
                return chunksIterator.next();
            }
        }
    }

    protected List<Chunk> chunks = new ArrayList<>();
    protected Set<ByteArray> frontingSet = new TreeSet<>();

    @Override
    public boolean add(ByteArray byteArray) {
        if(byteArray == null || byteArray.size() == 0) return false;
        boolean retVal = false;
        if(!contains(byteArray)) {
            frontingSet.add(byteArray);
            if(frontingSet.size() > MAX_FRONTING_SET_SIZE) {
                merge();
            }
            retVal = true;
        }
        return retVal;
    }

    @Override
    public boolean addAll(Collection<? extends ByteArray> c) {
        boolean retVal = false;
        for(ByteArray byteArray: c) {
            retVal = add(byteArray) || retVal;
        }
        return retVal;
    }

    @Override
    public void clear() {
        chunks.clear();
        frontingSet.clear();
    }

    @Override
    public boolean contains(Object o) {
        Preconditions.checkArgument(o instanceof ByteArray);
        if(frontingSet.contains(o)) {
            return true;
        } else {
            for(Chunk chunk: chunks) {
                if(chunk.contains((ByteArray) o)) return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for(Object o: c) {
            if(!contains(o)) return false;
        }
        return true;
    }

    /**
     * Deserializes the given bytes into a ByteSetArray instance
     * @param bytes - The bytes to deserialize
     * @return A shiny, new ByteArraySet
     */
    public static ByteArraySet deserialize(byte[] bytes) {
        if(bytes == null || bytes.length == 0) return new ByteArraySet();
        ByteArraySet set = new ByteArraySet();
        Chunk chunk;
        switch(FORMAT.valueOf(bytes[0])) {
            case EMPTY:
                break;
            case SINGLE_VALUE:
                ByteArray singleValue = new ByteArray(Arrays.copyOfRange(bytes, 1, bytes.length));
                byte[] chunkBytes = Arrays.copyOf(bytes, bytes.length);
                chunkBytes[0] = (byte) (bytes.length - 1);
                chunk = new Chunk(chunkBytes, 1, singleValue, singleValue, bytes.length);
                set.chunks.add(chunk);
                break;
            case SINGLE_CHUNK:
                chunk = Chunk.deserializeCompact(bytes, 1, bytes.length);
                set.chunks.add(chunk);
                break;
            case MULTI_CHUNK:
                int index = bytes.length - 1;
                while(index > 0) {
                    int size = Ints.fromBytes(bytes[index - 3], bytes[index - 2], bytes[index - 1], bytes[index]);
                    chunk = Chunk.deserialize(bytes, index - size + 1, index);
                    set.chunks.add(chunk);
                    index -= size;
                }
                Collections.reverse(set.chunks);
                break;
        }
        return set;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public Iterator<ByteArray> iterator() {
        return new FullIterator();
    }

    /**
     * Merges the fronting set values into the chunks, potentially creating new chunks, merging them together and/or
     * splitting them apart.
     */
    protected void merge() {
        if(frontingSet.size() == 0) return;
        List<Chunk> newChunks = new ArrayList<>();
        Chunk chunk = new Chunk(Chunk.MAX_CHUNK_SIZE);
        Iterator<ByteArray> frontIter = frontingSet.iterator();
        ChunksIterator chunksIter = new ChunksIterator();
        newChunks.add(chunk);
        ByteArray frontBA = null;
        ByteArray chunksBA = null;

        while(frontBA != null || frontIter.hasNext() || chunksBA != null || chunksIter.hasNext()) {
            if(frontBA == null && frontIter.hasNext()) frontBA = frontIter.next();
            while(chunksBA == null && chunksIter.hasNext()) chunksBA = chunksIter.next();
            if(frontBA != null && chunksBA != null) {
                if (frontBA.compareTo(chunksBA) < 0) {
                    if (chunk.size + 1 + frontBA.size() > Chunk.MAX_CHUNK_SIZE) {
                        chunk = new Chunk(Chunk.MAX_CHUNK_SIZE);
                        newChunks.add(chunk);
                    }
                    chunk.add(frontBA);
                    frontBA = null;
                } else {
                    if (chunk.size + 1 + chunksBA.size() > Chunk.MAX_CHUNK_SIZE) {
                        chunk = new Chunk(Chunk.MAX_CHUNK_SIZE);
                        newChunks.add(chunk);
                    }
                    chunk.add(chunksBA);
                    chunksBA = null;
                }
            } else if(frontBA != null) {
                if (chunk.size + 1 + frontBA.size() > Chunk.MAX_CHUNK_SIZE) {
                    chunk = new Chunk(Chunk.MAX_CHUNK_SIZE);
                    newChunks.add(chunk);
                }
                chunk.add(frontBA);
                frontBA = null;
            } else if(chunksBA != null) {
                if (chunk.size + 1 + chunksBA.size() > Chunk.MAX_CHUNK_SIZE) {
                    chunk = new Chunk(Chunk.MAX_CHUNK_SIZE);
                    newChunks.add(chunk);
                }
                chunk.add(chunksBA);
                chunksBA = null;
            }
        }
        chunks = newChunks;
        frontingSet.clear();
    }

    @Override
    public boolean remove(Object o) {
        Preconditions.checkArgument(o instanceof ByteArray);
        if(frontingSet.contains(o)) {
            return frontingSet.remove(o);
        } else {
            for(Chunk chunk: chunks) {
                if(chunk.remove((ByteArray) o)) return true;
            }
        }
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean retVal = false;
        for(Object o: c) {
            retVal = remove(o) || retVal;
        }
        return retVal;
    }

    /**
     * Serializes this set into a byte array representation
     * @return The byte array representation of this instance
     */
    public byte[] serialize() {
        merge();
        ByteBuffer buffer;
        switch(chunks.size()) {
            case 0:
                return EMPTY_SET_BYTES;
            case 1:
                Chunk singleChunk = chunks.get(0);
                switch(singleChunk.entries) {
                    case 0:
                        return EMPTY_SET_BYTES;
                    case 1:
                        buffer = ByteBuffer.allocate(1 + singleChunk.min.getBytes().length);
                        buffer.put(FORMAT.SINGLE_VALUE.getValue());
                        buffer.put(singleChunk.min.getBytes());
                        return buffer.array();
                    default:
                        buffer = ByteBuffer.allocate(1 + singleChunk.size);
                        buffer.put(FORMAT.SINGLE_CHUNK.getValue());
                        buffer.put(Arrays.copyOfRange(singleChunk.bytes, 0, singleChunk.size));
                        return buffer.array();
                }
            default:
                List<byte[]> chunkBytes = new ArrayList<>();
                int totalSize = 0;
                for(Chunk chunk: chunks) {
                    byte[] bytes = chunk.serialize();
                    totalSize += bytes.length;
                    chunkBytes.add(bytes);
                }
                buffer = ByteBuffer.allocate(1 + totalSize);
                buffer.put(FORMAT.MULTI_CHUNK.getValue());
                for(byte[] bytes: chunkBytes) {
                    buffer.put(bytes);
                }
                return buffer.array();
        }
    }

    @Override
    public int size() {
        int size = frontingSet.size();
        for(Chunk chunk: chunks) {
            size += chunk.entries;
        }
        return size;
    }

    // I don't care about these methods
    @Override
    public boolean retainAll(Collection<?> c) {
        throw new NotImplementedException();
    }

    @Override
    public boolean equals(Object object) {
        if (object == null || !(object instanceof ByteArraySet)) {
            return false;
        }

        if (size() != ((ByteArraySet) object).size()) {
            return false;
        }
        for (ByteArray ba: (ByteArraySet) object) {
            if (!contains(ba)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public ByteArray[] toArray() {
        return toArray(new ByteArray[size()]);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] array) {
        if(!(array instanceof ByteArray[])) {
            throw new IllegalArgumentException("Supplied array must be of type ByteArray[]");
        } else if(array.length != size()) {
            throw new IllegalArgumentException("Supplied array must equal the size of this set");
        }
        int index = 0;
        for(ByteArray byteArray: this) {
            if(byteArray != null && byteArray.size() > 0) {
                array[index] = (T) byteArray;
                index++;
            }
        }
        return array;
    }
}
