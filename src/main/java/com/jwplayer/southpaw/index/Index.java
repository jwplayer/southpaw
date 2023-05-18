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
import com.jwplayer.southpaw.metric.Metrics;
import com.jwplayer.southpaw.state.BaseState;
import com.jwplayer.southpaw.util.ByteArray;
import com.jwplayer.southpaw.util.ByteArraySet;
import org.apache.commons.collections4.map.LRUMap;

import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Simple index class that lets you store multiple primary keys (in a Set) per foreign key. This index
 * also has 'reverse index' functionality where you can get the foreign keys for a given primary key.
 */
public class Index {
    /**
     * Size of the LRU cache for storing the index entries containing more than one key
     */
    public static final String INDEX_LRU_CACHE_SIZE = "index.lru.cache.size";
    /**
     * Size of pending writes for index entries held in memory. Keeps pressure of the state since puts to the state
     * are not guaranteed to be immediately gettable without flushing.
     */
    public static final String INDEX_WRITE_BATCH_SIZE = "index.write.batch.size";
    /**
     * Logger
     */
    private static final Logger LOGGER =  LoggerFactory.getLogger(Index.class);
    /**
     * The size threshold for an index entry to be put into the LRU cache.
     */
    public static final int LRU_CACHE_THRESHOLD = 10;

    protected LRUMap<ByteArray, ByteArraySet> entryCache;
    protected LRUMap<ByteArray, ByteArraySet> entryRICache;
    /**
     * Size of the LRU cache for storing the index entries containing more than one key
     */
    protected int indexLRUCacheSize;
    /**
     * The name of the index. Only used internally to generate the index hash that IDs entries for this index in
     * the index topic.
     */
    protected String indexName;
    /**
     * Size of pending writes for index entries held in memory. Keeps pressure of the state since puts to the state
     * are not guaranteed to be immediately gettable without flushing.
     */
    protected int indexWriteBatchSize;
    /**
     * Metrics class that contains index related metrics
     */
    protected Metrics metrics;
    protected Map<ByteArray, ByteArraySet> pendingRIWrites = new HashMap<>();
    protected Map<ByteArray, ByteArraySet> pendingWrites = new HashMap<>();
    protected String reverseIndexName;
    /**
     * State for storing the indices and other data
     */
    protected BaseState state;

    /**
     * Adds or updates an index entry for the given foreign key with the given record PK to the indexed topic.
     * @param foreignKey - The foreign key to add/update an index entry for
     * @param primaryKey - The PK to the record
     */
    public void add(ByteArray foreignKey, ByteArray primaryKey) {
        Preconditions.checkNotNull(foreignKey);
        ByteArraySet pks = getIndexEntry(foreignKey);
        if(pks == null) {
            pks = new ByteArraySet();
        }
        addRI(foreignKey, primaryKey);
        if(pks.add(primaryKey)) {
            metrics.indexEntriesSize.get(indexName).update(pks.size());
            putToState(foreignKey, pks);
        }
    }

    /**
     * Adds the given foreign key and primary key to the reverse index
     * @param foreignKey - The foreign key to add
     * @param primaryKey - The primary key to add
     */
    protected void addRI(ByteArray foreignKey, ByteArray primaryKey) {
        ByteArraySet keys = getForeignKeys(primaryKey);
        if(keys == null) {
            keys = new ByteArraySet();
        }
        if(keys.add(foreignKey)) {
            metrics.indexReverseEntriesSize.get(indexName).update(keys.size());
            putRIToState(primaryKey, keys);
        }
    }

    /**
     * Configure the index
     * @param indexName - The name of the index
     * @param config - Configuration object containing index config
     * @param metrics - Metrics object for reporting index based metrics
     * @param state - State used to store the index
     */
    public void configure(String indexName, Map<String, Object> config, Metrics metrics, BaseState state) {
        this.indexLRUCacheSize = (int) Preconditions.checkNotNull(config.get(INDEX_LRU_CACHE_SIZE));
        this.entryCache = new LRUMap<>(this.indexLRUCacheSize);
        this.entryRICache = new LRUMap<>(this.indexLRUCacheSize);
        this.indexName = Preconditions.checkNotNull(indexName);
        this.indexWriteBatchSize = (int) Preconditions.checkNotNull(config.get(INDEX_WRITE_BATCH_SIZE));
        this.metrics = metrics;
        this.metrics.registerIndex(this.indexName);
        this.reverseIndexName = indexName + "-reverse";
        this.state = state;
        this.state.createKeySpace(this.indexName);
        this.state.createKeySpace(this.reverseIndexName);
    }

    /**
     * Flushes any changes to the topic. Useful for efficiently batching together writes when you need to make
     * a bunch of changes to the index.
     */
    public void flush() {
        for(Map.Entry<ByteArray, ByteArraySet> entry: pendingWrites.entrySet()) {
            writeToState(entry.getKey(), entry.getValue());
        }
        for(Map.Entry<ByteArray, ByteArraySet> entry: pendingRIWrites.entrySet()) {
            writeRIToState(entry.getKey(), entry.getValue());
        }
        pendingWrites.clear();
        pendingRIWrites.clear();
        state.flush(indexName);
        state.flush(reverseIndexName);
    }

    /**
     * Get the foreign keys for the given primary key.
     * @param primaryKey - The primary key to lookup
     * @return The keys for the given primary key or null if no corresponding entry exists
     */
    public ByteArraySet getForeignKeys(ByteArray primaryKey) {
        ByteArraySet retVal;
        if(entryRICache.containsKey(primaryKey)) {
            retVal = entryRICache.get(primaryKey);
        } else if(pendingRIWrites.containsKey(primaryKey)) {
            retVal = pendingRIWrites.get(primaryKey);
        } else {
            byte[] bytes = state.get(reverseIndexName, primaryKey.getBytes());
            if (bytes == null) {
                retVal = null;
            } else {
                ByteArraySet set = ByteArraySet.deserialize(bytes);
                if(set.size() > LRU_CACHE_THRESHOLD) entryRICache.put(primaryKey, set);
                retVal = set;
            }
        }
        if(retVal == null) {
            metrics.indexReverseEntriesSize.get(indexName).update(0);
        } else {
            metrics.indexReverseEntriesSize.get(indexName).update(retVal.size());
        }
        return retVal;
    }

    /**
     * Accessor for the index entry
     * @param foreignKey - The key of the index entry
     * @return The index entry for the given key
     */
    public ByteArraySet getIndexEntry(ByteArray foreignKey) {
        Preconditions.checkNotNull(foreignKey);
        ByteArraySet retVal;
        if(entryCache.containsKey(foreignKey)) {
            retVal = entryCache.get(foreignKey);
        } else if(pendingWrites.containsKey(foreignKey)) {
            retVal = pendingWrites.get(foreignKey);
        } else {
            byte[] bytes = state.get(indexName, foreignKey.getBytes());
            if (bytes == null) {
                retVal = null;
            } else {
                ByteArraySet set = ByteArraySet.deserialize(bytes);
                if(set.size() > LRU_CACHE_THRESHOLD) entryCache.put(foreignKey, set);
                retVal = set;
            }
        }
        if(retVal == null) {
            metrics.indexEntriesSize.get(indexName).update(0);
        } else {
            metrics.indexEntriesSize.get(indexName).update(retVal.size());
        }
        return retVal;
    }

    /**
     * Method for keeping pending writes manageable by auto-flushing once it reaches a certain size.
     */
    protected void putToState(ByteArray key, ByteArraySet value) {
        if(value.size() > LRU_CACHE_THRESHOLD) entryCache.put(key, value);
        pendingWrites.put(key, value);
        if(pendingWrites.size() > indexWriteBatchSize) {
            for(Map.Entry<ByteArray, ByteArraySet> entry: pendingWrites.entrySet()) {
                writeToState(entry.getKey(), entry.getValue());
            }
            pendingWrites.clear();
        }
    }

    /**
     * Method for keeping RI pending writes manageable by auto-flushing once it reaches a certain size.
     */
    protected void putRIToState(ByteArray key, ByteArraySet value) {
        if(value.size() > LRU_CACHE_THRESHOLD) entryRICache.put(key, value);
        pendingRIWrites.put(key, value);
        if(pendingRIWrites.size() > indexWriteBatchSize) {
            for (Map.Entry<ByteArray, ByteArraySet> entry : pendingRIWrites.entrySet()) {
                writeRIToState(entry.getKey(), entry.getValue());
            }
            pendingRIWrites.clear();
        }
    }

    /**
     * Removes the given foreign key from the reverse index entry corresponding to the given primary key
     * @param foreignKey - The foreign key to remove
     * @param primaryKey - The primary key of the entry
     */
    protected void removeRI(ByteArray foreignKey, ByteArray primaryKey) {
        ByteArraySet foreignKeys = getForeignKeys(primaryKey);
        if(foreignKeys != null) {
            foreignKeys.remove(foreignKey);
            metrics.indexReverseEntriesSize.get(indexName).update(foreignKeys.size());
            if(foreignKeys.size() == 0) {
                state.delete(reverseIndexName, primaryKey.getBytes());
                entryRICache.remove(primaryKey);
                pendingRIWrites.remove(primaryKey);
            } else {
                putRIToState(primaryKey, foreignKeys);
            }
        } else {
            metrics.indexReverseEntriesSize.get(indexName).update(0);
        }
    }

    /**
     * Removes the given primary key from the given foreign key entry, assuming both exist.
     * @param foreignKey - The key of the index entry.
     * @param primaryKey - The primary key to remove.
     */
    public boolean remove(ByteArray foreignKey, ByteArray primaryKey) {
        Preconditions.checkNotNull(foreignKey);
        removeRI(foreignKey, primaryKey);
        ByteArraySet primaryKeys = getIndexEntry(foreignKey);
        if(primaryKeys != null) {
            if(primaryKeys.remove(primaryKey)) {
                metrics.indexEntriesSize.get(indexName).update(primaryKeys.size());
                if(primaryKeys.size() == 0) {
                    state.delete(indexName, foreignKey.getBytes());
                    entryCache.remove(foreignKey);
                    pendingWrites.remove(foreignKey);
                } else {
                    putToState(foreignKey, primaryKeys);
                }
                return true;
            } else {
                metrics.indexEntriesSize.get(indexName).update(primaryKeys.size());
                return false;
            }
        } else {
            metrics.indexEntriesSize.get(indexName).update(0);
            return false;
        }
    }

    /**
     * Gives a nicely formatted string representation of this object. Useful for the Intellij debugger.
     * @return Formatted string representation of this object
     */
    public String toString() {
        return String.format("{indexName=%s}", indexName);
    }

    /**
     * Writes the reverse index entry to the state.
     * @param primaryKey - The primary key of the reverse index entry
     * @param foreignKeys - The foreign keys to record
     */
    protected void writeRIToState(ByteArray primaryKey, ByteArraySet foreignKeys) {
        Preconditions.checkNotNull(primaryKey);
        if(foreignKeys == null || foreignKeys.size() == 0) {
            state.delete(reverseIndexName, primaryKey.getBytes());
        } else {
            state.put(reverseIndexName, primaryKey.getBytes(), foreignKeys.serialize());
        }
    }

    /**
     * Writes the index entry to the state.
     * @param foreignKey - The foreign key of the index entry
     * @param primaryKeys - The primary keys to record
     */
    protected void writeToState(ByteArray foreignKey, ByteArraySet primaryKeys) {
        Preconditions.checkNotNull(foreignKey);
        if(primaryKeys == null || primaryKeys.size() == 0) {
            state.delete(indexName, foreignKey.getBytes());
        } else {
            state.put(indexName, foreignKey.getBytes(), primaryKeys.serialize());
        }
    }

    /**
     * Iterates through each entry in the reverse index and verifies the symmetric entries exist in the regular index
     * and correctly point to the reverse index
     * @return A string representation set of reverse index entry keys that are missing from the regular index.
     */
    public Set<String> verifyIndexState() {
        LOGGER.info("Verifying reverse index: " + reverseIndexName + " against index: " + indexName);
        Set<String> missingKeys = new HashSet<>();
        BaseState.Iterator iter = state.iterate(reverseIndexName);
        while (iter.hasNext()) {
            AbstractMap.SimpleEntry<byte[], byte[]> pair = iter.next();
            ByteArray revIndexPrimaryKey = new ByteArray(pair.getKey());
            ByteArraySet revIndexForeignKeySet = ByteArraySet.deserialize(pair.getValue());
            for (ByteArray indexPrimaryKey : revIndexForeignKeySet) {
                ByteArraySet indexForeignKeySet = getIndexEntry(indexPrimaryKey);

                if(indexForeignKeySet != null) {
                    if(indexForeignKeySet.contains(revIndexPrimaryKey)) {
                        continue;
                    }
                }
                missingKeys.add(revIndexPrimaryKey.toString());
            }
        }
        return missingKeys;
    }

    /**
     * Iterates through each entry in the regular index and verifies the symmetric entries exist in the reverse index
     * and correctly point to the regular index
     * @return A string representation set of regular index entry keys that are missing from the reverse index.
     */
    public Set<String> verifyReverseIndexState() {
        LOGGER.info("Verifying index: " + indexName + " against reverse index: " + reverseIndexName);
        Set<String> missingKeys = new HashSet<>();
        BaseState.Iterator iter = state.iterate(indexName);
        while (iter.hasNext()) {
            AbstractMap.SimpleEntry<byte[], byte[]> pair = iter.next();
            ByteArray indexPrimaryKey = new ByteArray(pair.getKey());
            ByteArraySet indexForeignKeySet = ByteArraySet.deserialize(pair.getValue());
            for (ByteArray revIndexPrimaryKey : indexForeignKeySet) {
                ByteArraySet revIndexForeignKeySet = getForeignKeys(revIndexPrimaryKey);
                if(revIndexForeignKeySet != null) {
                    if(revIndexForeignKeySet.contains(indexPrimaryKey)) {
                        continue;
                    }
                }
                missingKeys.add(indexPrimaryKey.toString());
            }
        }
        return missingKeys;
    }
}
