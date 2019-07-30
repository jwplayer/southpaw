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
import com.jwplayer.southpaw.util.ByteArraySet;
import org.apache.commons.collections4.map.LRUMap;

import java.util.*;


/**
 * Simple Index class that lets you store multiple primary keys (in a Set) per foreign key. This index
 * also has 'reverse index' functionality where you can get the foreign keys for a given primary key.
 * @param <K> - The type of the key stored in the indexed topic
 * @param <V> - The type of the value stored in the indexed topic
 */
public class MultiIndex<K, V> extends BaseIndex<K, V, Set<ByteArray>> implements Reversible {
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
     * The size threshold for an index entry to be put into the LRU cache.
     */
    public static final int LRU_CACHE_THRESHOLD = 10;

    /**
     * Size of the LRU cache for storing the index entries containing more than one key
     */
    protected int indexLRUCacheSize;
    /**
     * Size of pending writes for index entries held in memory. Keeps pressure of the state since puts to the state
     * are not guaranteed to be immediately gettable without flushing.
     */
    protected int indexWriteBatchSize;
    protected LRUMap<ByteArray, ByteArraySet> entryCache;
    protected LRUMap<ByteArray, ByteArraySet> entryRICache;
    protected Map<ByteArray, ByteArraySet> pendingWrites = new HashMap<>();
    protected Map<ByteArray, ByteArraySet> pendingRIWrites = new HashMap<>();
    protected String reverseIndexName;

    @Override
    public void add(ByteArray foreignKey, ByteArray primaryKey) {
        Preconditions.checkNotNull(foreignKey);
        ByteArraySet pks = getIndexEntry(foreignKey);
        if(pks == null) {
            pks = new ByteArraySet();
        }
        if(pks.add(primaryKey)) {
            addRI(foreignKey, primaryKey);
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
            putRIToState(primaryKey, keys);
        }
    }

    @Override
    public void configure(
            String indexName,
            Map<String, Object> config,
            BaseState state,
            BaseTopic<K, V> indexedTopic) {
        super.configure(indexName, config, state, indexedTopic);
        this.indexLRUCacheSize = (int) Preconditions.checkNotNull(config.get(INDEX_LRU_CACHE_SIZE));
        this.entryCache = new LRUMap<>(this.indexLRUCacheSize);
        this.entryRICache = new LRUMap<>(this.indexLRUCacheSize);
        this.indexWriteBatchSize = (int) Preconditions.checkNotNull(config.get(INDEX_WRITE_BATCH_SIZE));
        reverseIndexName = indexName + "-reverse";
        state.createKeySpace(reverseIndexName);
    }

    @Override
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

    @Override
    public ByteArraySet getForeignKeys(ByteArray primaryKey) {
        if(entryRICache.containsKey(primaryKey)) {
            return entryRICache.get(primaryKey);
        } else if(pendingRIWrites.containsKey(primaryKey)) {
            return pendingRIWrites.get(primaryKey);
        } else {
            byte[] bytes = state.get(reverseIndexName, primaryKey.getBytes());
            if (bytes == null) {
                return null;
            } else {
                ByteArraySet set = ByteArraySet.deserialize(bytes);
                if(set.size() > LRU_CACHE_THRESHOLD) entryRICache.put(primaryKey, set);
                return set;
            }
        }
    }

    @Override
    public ByteArraySet getIndexEntry(ByteArray foreignKey) {
        Preconditions.checkNotNull(foreignKey);
        if(entryCache.containsKey(foreignKey)) {
            return entryCache.get(foreignKey);
        } else if(pendingWrites.containsKey(foreignKey)) {
            return pendingWrites.get(foreignKey);
        } else {
            byte[] bytes = state.get(indexName, foreignKey.getBytes());
            if (bytes == null) {
                return null;
            } else {
                ByteArraySet set = ByteArraySet.deserialize(bytes);
                if(set.size() > LRU_CACHE_THRESHOLD) entryCache.put(foreignKey, set);
                return set;
            }
        }
    }

    /**
     * Method for keeping pending writes manageable by auto-flushing once it reaches a certain size.
     */
    public void putToState(ByteArray key, ByteArraySet value) {
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
    public void putRIToState(ByteArray key, ByteArraySet value) {
        if(value.size() > LRU_CACHE_THRESHOLD) entryRICache.put(key, value);
        pendingRIWrites.put(key, value);
        if(pendingRIWrites.size() > indexWriteBatchSize) {
            for (Map.Entry<ByteArray, ByteArraySet> entry : pendingRIWrites.entrySet()) {
                writeRIToState(entry.getKey(), entry.getValue());
            }
            pendingRIWrites.clear();
        }
    }

    @Override
    public Iterator<AbstractMap.SimpleEntry<ByteArray, V>> readRecords(ByteArray foreignKey) {
        Preconditions.checkNotNull(foreignKey);
        ByteArraySet primaryKeys = getIndexEntry(foreignKey);
        if(primaryKeys != null) {
            List<AbstractMap.SimpleEntry<ByteArray, V>> records = new ArrayList<>(primaryKeys.size());
            for(ByteArray primaryKey: primaryKeys) {
                records.add(new AbstractMap.SimpleEntry<>(primaryKey, indexedTopic.readByPK(primaryKey)));
            }
            return records.iterator();
        }

        return Collections.emptyListIterator();
    }

    @Override
    public ByteArraySet remove(ByteArray foreignKey) {
        Preconditions.checkNotNull(foreignKey);
        ByteArraySet primaryKeys = getIndexEntry(foreignKey);
        if(primaryKeys != null) {
            state.delete(indexName, foreignKey.getBytes());
            if(entryCache.containsKey(foreignKey)) {
                entryCache.remove(foreignKey);
            }
            if(pendingWrites.containsKey(foreignKey)) {
                pendingWrites.remove(foreignKey);
            }
            for(ByteArray primaryKey: primaryKeys) {
                removeRI(foreignKey, primaryKey);
            }
        }
        return primaryKeys;
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
            if(foreignKeys.size() == 0) {
                state.delete(reverseIndexName, primaryKey.getBytes());
                if(entryRICache.containsKey(primaryKey)) {
                    entryRICache.remove(primaryKey);
                }
                if(pendingRIWrites.containsKey(primaryKey)) {
                    pendingRIWrites.remove(primaryKey);
                }
            } else {
                putRIToState(primaryKey, foreignKeys);
            }
        }
    }

    @Override
    public boolean remove(ByteArray foreignKey, ByteArray primaryKey) {
        Preconditions.checkNotNull(foreignKey);
        ByteArraySet primaryKeys = getIndexEntry(foreignKey);
        if(primaryKeys != null) {
            if(primaryKeys.remove(primaryKey)) {
                if(primaryKeys.size() == 0) {
                    state.delete(indexName, foreignKey.getBytes());
                    if(entryCache.containsKey(foreignKey)) {
                        entryCache.remove(foreignKey);
                    }
                    pendingWrites.remove(foreignKey);
                } else {
                    putToState(foreignKey, primaryKeys);
                }
                removeRI(foreignKey, primaryKey);
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
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
        System.out.println("Verifying reverse index: " + reverseIndexName + " against index: " + indexName);
        Set<String> missingKeys = new HashSet<>();
        BaseState.Iterator iter = state.iterate(reverseIndexName);
        while (iter.hasNext()) {
            AbstractMap.SimpleEntry<byte[], byte[]> pair = iter.next();
            ByteArray revIndexPrimaryKey = new ByteArray(pair.getKey());
            ByteArraySet revIndexForeignKeySet = ByteArraySet.deserialize(pair.getValue());
            for (ByteArray indexPrimaryKey : revIndexForeignKeySet.toArray()) {
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
        System.out.println("Verifying index: " + indexName + " against reverse index: " + reverseIndexName);
        Set<String> missingKeys = new HashSet<>();
        BaseState.Iterator iter = state.iterate(indexName);
        while (iter.hasNext()) {
            AbstractMap.SimpleEntry<byte[], byte[]> pair = iter.next();
            ByteArray indexPrimaryKey = new ByteArray(pair.getKey());
            ByteArraySet indexForeignKeySet = ByteArraySet.deserialize(pair.getValue());
            for (ByteArray revIndexPrimaryKey : indexForeignKeySet.toArray()) {
                ByteArraySet revIndexforeignKeySet = getForeignKeys(revIndexPrimaryKey);

                if(revIndexforeignKeySet != null) {
                    if(revIndexforeignKeySet.contains(indexPrimaryKey)) {
                        continue;
                    }
                }
                missingKeys.add(indexPrimaryKey.toString());
            }
        }
        return missingKeys;
    }
}
