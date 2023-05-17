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
package com.jwplayer.southpaw;

import com.jwplayer.southpaw.state.BaseState;
import com.jwplayer.southpaw.util.ByteArray;
import org.apache.commons.lang.NotImplementedException;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

public class MockState extends BaseState {
    public static class Iterator extends BaseState.Iterator {
        java.util.Iterator<Map.Entry<com.jwplayer.southpaw.util.ByteArray,byte[]>> innerIter;

        public Iterator(java.util.Iterator<Map.Entry<com.jwplayer.southpaw.util.ByteArray,byte[]>> innerIter) {
            this.innerIter = innerIter;
        }

        @Override
        public void close() {
        }

        @Override
        public boolean hasNext() {
            return innerIter.hasNext();
        }

        @Override
        public AbstractMap.SimpleEntry<byte[], byte[]> next() {
            Map.Entry<ByteArray, byte[]> nextValue = innerIter.next();
            return new AbstractMap.SimpleEntry<>(nextValue.getKey().getBytes(), nextValue.getValue());
        }
    }

    private final Map<ByteArray, Map<ByteArray, byte[]>> dataBatches = new HashMap<>();

    @Override
    public void backup() {
        throw new NotImplementedException();
    }

    @Override
    public void configure(Map<String, Object> config) {

    }

    @Override
    public void open() {
        super.open();
    }

    @Override
    public void createKeySpace(String keySpace) {
        dataBatches.putIfAbsent(new ByteArray(keySpace), new HashMap<>());
    }

    @Override
    public void delete() {
        dataBatches.clear();
    }

    @Override
    public void delete(String keySpace, byte[] key) {
        dataBatches.get(new ByteArray(keySpace)).remove(new ByteArray(key));
    }

    @Override
    public void deleteBackups() {
        throw new NotImplementedException();
    }

    @Override
    public void flush() {
    }

    @Override
    public void flush(String keySpace) {
    }

    @Override
    public byte[] get(String keySpace, byte[] key) {
       return dataBatches.get(new ByteArray(keySpace)).get(new ByteArray(key));
    }

    @Override
    public Iterator iterate(String keySpace) {
        return new Iterator(dataBatches.get(new ByteArray(keySpace)).entrySet().iterator());
    }

    @Override
    public void put(String keySpace, byte[] key, byte[] value) {
        Map<ByteArray, byte[]> dataBatch = dataBatches.get(new ByteArray(keySpace));
        dataBatch.put(new ByteArray(key), value);
    }

    @Override
    public void restore() {
        throw new NotImplementedException();
    }
}
