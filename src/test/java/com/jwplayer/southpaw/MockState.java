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

import java.util.HashMap;
import java.util.Map;

public class MockState extends BaseState {

    private Map<ByteArray, Map<ByteArray, byte[]>> dataBatches;

    @Override
    public void backup() {
        throw new NotImplementedException();
    }

    @Override
    public void open(Map<String, Object> config) {
        dataBatches = new HashMap<>();
        super.open(config);
    }

    @Override
    public void createKeySpace(String keySpace) {
        dataBatches.put(new ByteArray(keySpace), new HashMap<>());
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
        return null;
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
