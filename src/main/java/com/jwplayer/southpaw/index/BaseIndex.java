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
package com.jwplayer.southpaw.index;

import com.google.common.base.Preconditions;
import com.jwplayer.southpaw.state.BaseState;
import com.jwplayer.southpaw.topic.BaseTopic;
import com.jwplayer.southpaw.util.ByteArray;

import java.util.*;


/**
 * This class indexes entities stored in topics. These indexes are always a key (as a byte array, which can be
 * composite) to an offset object. This index can also be used to read those entities by key, in addition to
 * standard operations of adding new entries, adding new offsets to existing entries, deleting entries, and
 * deleting offsets from existing entries. A single index topic can store multiple indices.
 * @param <K> - The type of the key stored in the indexed topic
 * @param <V> - The type of the value stored in the indexed topic
 * @param <O> - The type of the offset object stored in the index topic
 */
public abstract class BaseIndex<K, V, O> {
    /**
     * The name of the index. Only used internally to generate the index hash that IDs entries for this index in
     * the index topic.
     */
    protected String indexName;
    /**
     * The topic that is indexed by this index.
     */
    protected BaseTopic<K, V> indexedTopic;
    /**
     * State for storing the indices and other data
     */
    protected BaseState state;

    public BaseIndex() { }

    /**
     * Adds or updates an index entry for the given foreign key with the given record PK to the indexed topic.
     * @param foreignKey - The foreign key to add/update an index entry for
     * @param primaryKey - The PK to the record
     */
    public abstract void add(ByteArray foreignKey, ByteArray primaryKey);

    /**
     * Configure the index
     * @param indexName - The name of the index
     * @param config - Configuration object containing index config
     * @param state - State used to store the index
     * @param indexedTopic - The indexed topic
     */
    public void configure(
            String indexName,
            Map<String, Object> config,
            BaseState state,
            BaseTopic<K, V> indexedTopic) {
        this.indexName = Preconditions.checkNotNull(indexName);
        this.indexedTopic = Preconditions.checkNotNull(indexedTopic);
        this.state = state;
        this.state.createKeySpace(indexName);
    }

    /**
     * Accessor for the topic that is being indexed
     * @return The indexed topic
     */
    public BaseTopic<K, V> getIndexedTopic() {
        return indexedTopic;
    }

    /**
     * Accessor for the index entry
     * @param foreignKey - The key of the index entry
     * @return The index entry for the given key
     */
    public abstract O getIndexEntry(ByteArray foreignKey);

    /**
     * Flushes any changes to the topic. Useful for efficiently batching together writes when you need to make
     * a bunch of changes to the index.
     */
    public abstract void flush();

    /**
     * Reads the indexed records corresponding to the given key.
     * @param foreignKey - The foreign key to lookup in the index.
     * @return The indexed records, or null if no entries exists for the given key.
     */
    public abstract Iterator<AbstractMap.SimpleEntry<ByteArray, V>> readRecords(ByteArray foreignKey);

    /**
     * Removes the index entry with the given key.
     * @param foreignKey - The foreign key of the index entry to remove.
     * @return The removed entry value
     */
    public abstract O remove(ByteArray foreignKey);

    /**
     * Removes the given primary key from the given foreign key entry, assuming both exist.
     * @param foreignKey - The key of the index entry.
     * @param primaryKey - The primary key to remove.
     */
    public abstract boolean remove(ByteArray foreignKey, ByteArray primaryKey);

    /**
     * Gives a nicely formatted string representation of this object. Useful for the Intellij debugger.
     * @return Formatted string representation of this object
     */
    public String toString() {
        return String.format(
                "{indexName=%s,indexedTopic=%s}",
                indexName,
                indexedTopic.getShortName()
        );
    }
}
