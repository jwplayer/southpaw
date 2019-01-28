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
package com.jwplayer.southpaw.record;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;


/**
 * Wrapper record for a simple map object
 */
public class MapRecord extends BaseRecord {
    Map<String, ?> internalRecord;

    public MapRecord(Map<String, ?> internalRecord) {
        this.internalRecord = internalRecord;
    }

    @Override
    public Object get(String fieldName) {
        if(internalRecord == null) return null;
        return internalRecord.get(fieldName);
    }

    @Override
    public Map<String, Class> getSchema() {
        Map<String, Class> schema = new HashMap<>();
        if(internalRecord == null) return schema;

        for(Map.Entry<String, ?> entry: internalRecord.entrySet()) {
            schema.put(entry.getKey(), entry.getValue().getClass());
        }

        return schema;
    }

    @Override
    public boolean isEmpty() {
        return internalRecord == null || internalRecord.size() == 0;
    }

    @Override
    public Map<String, ?> toMap() {
        if(internalRecord == null) return ImmutableMap.of();
        return ImmutableMap.copyOf(internalRecord);
    }
}
