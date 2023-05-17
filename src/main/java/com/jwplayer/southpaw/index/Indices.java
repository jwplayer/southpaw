package com.jwplayer.southpaw.index;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.jwplayer.southpaw.json.Relation;
import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.state.BaseState;
import com.jwplayer.southpaw.util.ByteArray;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Class for containing the various kinds of indices used by Southpaw, as well as code for interacting with these
 * indices.
 */
public class Indices {
    /**
     * Join key, the key in the child record used in joins (PaK == JK)
     */
    public static final String JK = "JK";
    /**
     * Parent key, the key in the parent record used in joins (PaK == JK)
     */
    public static final String PaK = "PaK";
    /**
     * Separator used by constructor keys and other things
     */
    public static final String SEP = "|";
    private static final Logger logger =  LoggerFactory.getLogger(Indices.class);

    /**
     * Configuration for the indices
     */
    protected final Map<String, Object> config;
    /**
     * All indices representing the relationship between an entity's primary and join keys
     */
    protected final Map<String, Index> joinIndices = new HashMap<>();
    /**
     * All indices representing the relationship between a child entity's join keys and the root entity's primary keys
     */
    protected final Map<String, Index> parentIndices = new HashMap<>();
    /**
     * State for storing the indices
     */
    protected BaseState state;

    /**
     * Der Konstruktor
     * @param config - Config for the indices
     * @param state - State for storing the indices
     */
    public Indices(Map<String, Object> config, BaseState state) {
        this.config = config;
        this.state = state;
    }

    /**
     * Create all indices for the given child relation and its children.
     * @param root - The root relation to create the indices for
     * @param parent - The parent relation to create the indices for
     * @param child - The child relation to create the indices for
     */
    protected void createChildIndices(Relation root, Relation parent, Relation child) {
        // Create this child's indices
        String joinIndexName = createJoinIndexName(child);
        joinIndices.put(joinIndexName, createIndex(joinIndexName));
        String parentIndexName = createParentIndexName(root, parent, child);
        parentIndices.put(parentIndexName, createIndex(parentIndexName));

        // Add its children's indices
        if(child.getChildren() != null) {
            for(Relation grandchild: child.getChildren()) {
                createChildIndices(root, child, grandchild);
            }
        }
    }

    /**
     * Simple class for creating a FK multi index
     * @param indexName - The name of the index to create
     * @return A brand new, shiny index
     */
    protected Index createIndex(String indexName) {
        Index index = new Index();
        index.configure(indexName, config, state);
        return index;
    }

    /**
     * Creates all indices for all relations provided to Southpaw. Note: indices to the input records
     * can be shared between top level relations.
     */
    public void createIndices(Relation[] relations) {
        for(Relation root: relations) {
            for(Relation child: root.getChildren()) {
                createChildIndices(root, root, child);
            }
        }
    }

    /**
     * Creates the join index name from the child relation
     * @param child - The child relation to create the join index name for
     * @return The join index name
     */
    protected static String createJoinIndexName(Relation child) {
        return String.join(SEP, JK, child.getEntity(), child.getJoinKey());
    }

    /**
     * Creates the parent index name from the given relations
     * @param root - The root relation to create the join index name for
     * @param parent - The parent relation to create the join index name for
     * @param child - The child relation to create the join index name for
     * @return The join index name
     */
    protected static String createParentIndexName(Relation root, Relation parent, Relation child) {
        return String.join(SEP, PaK, root.getEntity(), parent.getEntity(), child.getParentKey());
    }

    /**
     * Flushes all indices
     */
    public void flush() {
        for(Map.Entry<String, Index> index: joinIndices.entrySet()) {
            index.getValue().flush();
        }
        for(Map.Entry<String, Index> index: parentIndices.entrySet()) {
            index.getValue().flush();
        }
    }

    /**
     * Get the join keys for the given primary key from the join index for the given relation
     * @param relation - The relation of the join index to check against
     * @param primaryKey - The primary key to lookup in the index
     * @return The join keys found in the index
     */
    public Set<ByteArray> getRelationJKs(Relation relation, ByteArray primaryKey) {
        Index joinIndex = joinIndices.get(createJoinIndexName(relation));
        return joinIndex.getForeignKeys(primaryKey);
    }

    /**
     * Get the primary keys for the given join key from the join index for the given relation
     * @param relation - The relation of the join index to check against
     * @param joinKey - The join key to lookup in the index
     * @return The primary keys found in the index
     */
    public Set<ByteArray> getRelationPKs(Relation relation, ByteArray joinKey) {
        Index joinIndex = joinIndices.get(createJoinIndexName(relation));
        return joinIndex.getIndexEntry(joinKey);
    }

    /**
     * Get the primary keys for the given join key from the parent index for the given root, parent, and child relations
     * @param root - The root relation of the parent index to check against
     * @param parent - The parent relation of the parent index to check against
     * @param child - The child relation of the parent index to check against
     * @param joinKey - The join key to lookup in the index
     * @return The primary keys found in the index
     */
    public Set<ByteArray> getRootPKs(Relation root, Relation parent, Relation child, ByteArray joinKey) {
        Index parentIndex = parentIndices.get(createParentIndexName(root, parent, child));
        return parentIndex.getIndexEntry(joinKey);
    }

    /**
     * Scrubs the parent indices of the given root primary key starting at the given relation. This is needed when a
     * tombstone record is seen for the root so that we remove all references to the now defunct root PK so we no
     * longer try to create (empty) records for it.
     * @param root - The root relation of the parent relation
     * @param parent  - The parent relation of the parent index to scrub
     * @param rootPrimaryKey - The primary key of the root record prior to the tombstone triggering this scrubbing
     */
    public void scrubParentIndices(Relation root, Relation parent, ByteArray rootPrimaryKey) {
        Preconditions.checkNotNull(root);
        Preconditions.checkNotNull(parent);

        if(parent.getChildren() != null && rootPrimaryKey != null) {
            for(Relation child: parent.getChildren()) {
                Index parentIndex = parentIndices.get(createParentIndexName(root, parent, child));
                Set<ByteArray> oldForeignKeys = parentIndex.getForeignKeys(rootPrimaryKey);
                if(oldForeignKeys != null) {
                    for(ByteArray oldForeignKey: ImmutableSet.copyOf(oldForeignKeys)) {
                        parentIndex.remove(oldForeignKey, rootPrimaryKey);
                    }
                }
                scrubParentIndices(root, child, rootPrimaryKey);
            }
        }
    }

    /**
     * Updates the join index for the given child relation using the new record and the old PK index entry.
     * @param relation - The child relation of the join index
     * @param newRecord - The new version of the child record. May technically not be the latest version of a
     *                  record, but that is ok, since the index will eventually be updated with the latest
     *                  record.
     */
    public void updateJoinIndex(Relation relation, ConsumerRecord<BaseRecord, BaseRecord> newRecord) {
        Preconditions.checkNotNull(relation.getJoinKey());
        Preconditions.checkNotNull(newRecord);
        ByteArray newRecordPK = newRecord.key().toByteArray();
        Index joinIndex = joinIndices.get(createJoinIndexName(relation));
        Set<ByteArray> oldJoinKeys = joinIndex.getForeignKeys(newRecordPK);
        ByteArray newJoinKey = null;
        if(newRecord.value() != null) {
            newJoinKey = ByteArray.toByteArray(newRecord.value().get(relation.getJoinKey()));
        }
        if (oldJoinKeys != null && oldJoinKeys.size() > 0) {
            for(ByteArray oldJoinKey: oldJoinKeys) {
                if(!oldJoinKey.equals(newJoinKey)) {
                    joinIndex.remove(oldJoinKey, newRecordPK);
                }
            }
        }
        if (newJoinKey != null) {
            joinIndex.add(newJoinKey, newRecordPK);
        }
    }

    /**
     * Updates the parent index of the given relations.
     * @param root - The root relation of the parent relation
     * @param parent - The parent relation of the parent index
     * @param child - The child relation of the parent index
     * @param rootPrimaryKey - The primary key of the new root record
     * @param newParentKey - The new parent key (may or may not differ from the old one)
     */
    public void updateParentIndex(
            Relation root,
            Relation parent,
            Relation child,
            ByteArray rootPrimaryKey,
            ByteArray newParentKey
    ) {
        Preconditions.checkNotNull(root);
        Preconditions.checkNotNull(parent);
        Preconditions.checkNotNull(child);
        Preconditions.checkNotNull(rootPrimaryKey);

        Index parentIndex = parentIndices.get(createParentIndexName(root, parent, child));
        if (newParentKey != null) parentIndex.add(newParentKey, rootPrimaryKey);
    }

    /**
     * Verifies and logs the given index
     * @param index - The index to verify
     */
    public void verifyIndex(Index index) {
        Set<String> missingIndexKeys = index.verifyIndexState();
        logger.info("Verifying forward index");
        if(missingIndexKeys.isEmpty()){
            logger.info("Forward index check complete");
        } else {
            logger.error("Forward index check failed for the following " + missingIndexKeys.size() + " keys:");
            logger.error(missingIndexKeys.toString());
        }

        logger.info("Verifying reverse index");
        Set<String> missingReverseIndexKeys = index.verifyReverseIndexState();
        if(missingReverseIndexKeys.isEmpty()){
            logger.info("Reverse index check complete");
        } else {
            logger.error("Reverse index check failed for the following " + missingReverseIndexKeys.size() + " keys:");
            logger.error(missingReverseIndexKeys.toString());
        }
    }

    /**
     * Utility command to verify all indices and reverse indices in the State are in sync with each other. Keys that
     * are not set properly in the index and reverse index are logged to error.
     * <b>Note: This requires a full scan of each index dataset. This could be an expensive operation on larger
     * datasets.</b>
     */
    public void verifyIndices() {
        logger.info("Verifying join indices");
        for(Map.Entry<String, Index> index: joinIndices.entrySet()) {
            logger.info("Verifying join index: " + index.getKey());
            verifyIndex(index.getValue());
        }
        logger.info("Verifying parent indices");
        for(Map.Entry<String, Index> index: parentIndices.entrySet()) {
            logger.info("Verifying parent index: " + index.getKey());
            verifyIndex(index.getValue());
        }
    }
}
