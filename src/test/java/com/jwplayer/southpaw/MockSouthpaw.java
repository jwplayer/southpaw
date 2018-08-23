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

import com.jwplayer.southpaw.index.BaseIndex;
import com.jwplayer.southpaw.json.*;
import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.topic.BaseTopic;
import com.jwplayer.southpaw.util.ByteArray;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * 'Mock' Southpaw class that gives us access to its protected methods for testing.
 */
public class MockSouthpaw extends Southpaw {
    public MockSouthpaw(Map<String, Object> config, List<URI> uris)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, IOException, URISyntaxException {
        super(config, uris);
    }

    public void createDenormalizedRecords(
            Relation root,
            Set<ByteArray> rootRecordPKs) {
        super.createDenormalizedRecords(root, rootRecordPKs);
    }

    public Record createInternalRecord(BaseRecord inputRecord) {
        return super.createInternalRecord(inputRecord);
    }

    public Map<String, Object> createTopicConfig(String configName) {
        return super.createTopicConfig(configName);
    }

    public AbstractMap.SimpleEntry<Relation, Relation> getRelation(Relation relation, String entity) {
        return super.getRelation(relation, entity);
    }

    /**
     * Accessor for the FK indices used by Southpaw
     * @return Southpaw's FK indices
     */
    public Map<String, BaseIndex<BaseRecord, BaseRecord, Set<ByteArray>>> getFkIndices() {
        return fkIndices;
    }

    /**
     * Accessor for the normalized topics used by Southpaw
     * @return Southpaw's normalized topics
     */
    public Map<String, BaseTopic<BaseRecord, BaseRecord>> getNormalizedTopics() {
        return inputTopics;
    }
}
