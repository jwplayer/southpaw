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

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.jwplayer.southpaw.filter.BaseFilter;
import com.jwplayer.southpaw.index.BaseIndex;
import com.jwplayer.southpaw.index.MultiIndex;
import com.jwplayer.southpaw.index.Reversible;
import com.jwplayer.southpaw.json.*;
import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.serde.BaseSerde;
import com.jwplayer.southpaw.state.BaseState;
import com.jwplayer.southpaw.state.RocksDBState;
import com.jwplayer.southpaw.topic.BaseTopic;
import com.jwplayer.southpaw.util.ByteArray;
import com.jwplayer.southpaw.util.ByteArraySet;
import com.jwplayer.southpaw.util.FileHelper;
import com.jwplayer.southpaw.metric.Metrics;
import com.jwplayer.southpaw.metric.StaticGauge;
import com.jwplayer.southpaw.topic.TopicConfig;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * A class that creates denormalized records from input records based on hierarchical relationships. These
 * relationships are similar a LEFT OUTER JOIN defined by the following SQL statement:
 *     SELECT ...
 *     FROM table_a LEFT OUTER JOIN table_b on a_key = b_key
 * In this case 'table_b' is a child relationship of 'table_a.' 'a_key' is equivalent to the parent key and 'b_key'
 * is equivalent to the join key in a child relation. Ultimately, one 'table' is the root relation. The topic key
 * for each record in all input and denormalized records is treated as the primary key, which is used by the
 * various indices and within the denormalized records themselves.
 */
public class Southpaw {
    /**
     * Join key, the key in the child record used in joins (PaK == JK)
     */
    public static final String JK = "JK";
    /**
     * The name of the state keyspace for Southpaw's metadata
     */
    public static final String METADATA_KEYSPACE = "__southpaw.metadata";
    /**
     * Parent key, the key in the parent record used in joins (PaK == JK)
     */
    public static final String PaK = "PaK";
    /**
     * Primary key
     */
    public static final String PK = "PK";
    /**
     * Separator used by constructor keys and other things
     */
    public static final String SEP = "|";
    /**
     * Le Logger
     */
    private static final Logger logger = Logger.getLogger(Southpaw.class);
    /**
     * Used for doing object <-> JSON mappings
     */
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Parsed Southpaw config
     */
    protected final Config config;
    /**
     * The PKs of the denormalized records yet to be created
     */
    protected Map<Relation, ByteArraySet> dePKsByType = new HashMap<>();
    /**
     * A map of foreign key indices needed by Southpaw. This includes parent indices (points at the root
     * records) and join indices (points at the child records). The key is the index name. Multiple offsets
     * can be stored per key.
     */
    protected final Map<String, BaseIndex<BaseRecord, BaseRecord, Set<ByteArray>>> fkIndices = new HashMap<>();
    /**
     * A map of all input topics needed by Southpaw. The key is the short name of the topic.
     */
    protected Map<String, BaseTopic<BaseRecord, BaseRecord>> inputTopics;
    /**
     * Simple metrics class for Southpaw
     */
    protected final Metrics metrics = new Metrics();
    /**
     * A map of the output topics needed where the denormalized records are written. The key is the short name of
     * the topic.
     */
    protected Map<String, BaseTopic<byte[], DenormalizedRecord>> outputTopics;
    /**
     * Tells the run() method to process records. If this is set to false, it will stop.
     */
    protected boolean processRecords = true;
    /**
     * The configuration for Southpaw. Mostly Kafka and topic configuration. See
     * test/test-resources/config.sample.yaml for an example.
     */
    protected final Map<String, Object> rawConfig;
    /**
     * The top level relations that instruct Southpaw how to construct denormalized records. See
     * test-resources/relations.sample.json for an example.
     */
    protected final Relation[] relations;
    /**
     * Did Southpaw start up successfully?
     */
    protected boolean startedSuccessfully = false;
    /**
     * State for Southpaw
     */
    protected BaseState state;

    /**
     * Base Southpaw config
     */
    protected static class Config {
        public static final String BACKUP_ON_SHUTDOWN_CONFIG = "backup.on.shutdown";
        public static final boolean BACKUP_ON_SHUTDOWN_DEFAULT = false;
        public static final String BACKUP_ON_SHUTDOWN_DOC = "Instructs Southpaw to backup on shutdown (or not)";
        public static final String BACKUP_TIME_S_CONFIG = "backup.time.s";
        public static final int BACKUP_TIME_S_DEFAULT = 1800;
        public static final String BACKUP_TIME_S_DOC = "Time interval (roughly) between backups";
        public static final String COMMIT_TIME_S_CONFIG = "commit.time.s";
        public static final int COMMIT_TIME_S_DEFAULT = 0;
        public static final String COMMIT_TIME_S_DOC = "Time interval (roughly) between commits";
        public static final String CREATE_RECORDS_TRIGGER_CONFIG = "create.records.trigger";
        public static final int CREATE_RECORDS_TRIGGER_DEFAULT = 250000;
        public static final String CREATE_RECORDS_TRIGGER_DOC =
                "Config for when to create denormalized records once the number of records to create has exceeded " +
                        "a certain amount";
        public static final String LOG_LEVEL_CONFIG = "log.level";
        public static final String LOG_LEVEL_DEFAULT = "INFO";
        public static final String LOG_LEVEL_DOC = "Log level config for log4j";
        public static final String TOPIC_LAG_TRIGGER_CONFIG = "topic.lag.trigger";
        public static final String TOPIC_LAG_TRIGGER_DEFAULT = "1000";
        public static final String TOPIC_LAG_TRIGGER_DOC =
                "Config for when to switch from one topic to the next (or to stop processing a topic entirely), " +
                        "when lag drops below this value.";

        public boolean backupOnShutdown;
        public int backupTimeS;
        public int commitTimeS;
        public int createRecordsTrigger;
        public String logLevel;
        public int topicLagTrigger;

        public Config(Map<String, Object> rawConfig) throws ClassNotFoundException {
            this.backupOnShutdown = (boolean) rawConfig.getOrDefault(BACKUP_ON_SHUTDOWN_CONFIG, BACKUP_ON_SHUTDOWN_DEFAULT);
            this.backupTimeS = (int) rawConfig.getOrDefault(BACKUP_TIME_S_CONFIG, BACKUP_TIME_S_DEFAULT);
            this.commitTimeS = (int) rawConfig.getOrDefault(COMMIT_TIME_S_CONFIG, COMMIT_TIME_S_DEFAULT);
            this.createRecordsTrigger = (int) rawConfig.getOrDefault(CREATE_RECORDS_TRIGGER_CONFIG, CREATE_RECORDS_TRIGGER_DEFAULT);
            this.logLevel = rawConfig.getOrDefault(LOG_LEVEL_CONFIG, LOG_LEVEL_DEFAULT).toString();
            this.topicLagTrigger = (int) rawConfig.getOrDefault(TOPIC_LAG_TRIGGER_CONFIG, TOPIC_LAG_TRIGGER_DEFAULT);
        }
    }

    protected static class RestoreMode {
        static final String ALWAYS = "always";
        static final String NEVER = "never";
        static final String WHEN_NEEDED = "when_needed";
    }

    /**
     * Constructor
     * @param rawConfig - Southpaw configuration
     * @param relations - URIs to files containing the top level relations that define the denormalized
     *            objects to construct
     * @throws IOException -
     * @throws URISyntaxException -
     */
    public Southpaw(Map<String, Object> rawConfig, List<URI> relations)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, IOException, URISyntaxException {
        this(rawConfig, loadRelations(Preconditions.checkNotNull(relations)));
    }

    /**
     * Constructor
     * @param rawConfig - Southpaw configuration
     * @param relations - The top level relations that define the denormalized objects to construct
     */
    public Southpaw(Map<String, Object> rawConfig, Relation[] relations)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        validateRootRelations(relations);

        this.rawConfig = Preconditions.checkNotNull(rawConfig);
        this.config = new Config(rawConfig);
        logger.setLevel(Level.toLevel(config.logLevel, Level.INFO));
        this.relations = Preconditions.checkNotNull(relations);
        this.state = new RocksDBState();
        this.state.configure(rawConfig);
        this.state.createKeySpace(METADATA_KEYSPACE);
        this.inputTopics = new HashMap<>();
        this.outputTopics = new HashMap<>();
        for(Relation root: this.relations) {
            this.inputTopics.putAll(createInputTopics(root));
            this.outputTopics.put(root.getDenormalizedName(), createOutputTopic(root.getDenormalizedName()));
            this.metrics.registerOutputTopic(root.getDenormalizedName());
        }
        for(Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>> entry: this.inputTopics.entrySet()) {
            this.metrics.registerInputTopic(entry.getKey());
        }
        createIndices();

        // Load any previous denormalized record PKs that have yet to be created
        for (Relation root : relations) {
            byte[] bytes = state.get(METADATA_KEYSPACE, createDePKEntryName(root).getBytes());
            dePKsByType.put(root, ByteArraySet.deserialize(bytes));
        }

        /* Make sure we backup, and close before exiting. Also, prevents a scenario where Southpaw:
        * - Starts up
        * - Attempts to restore from backups
        * - Restore fails, potentially due to a transient S3 error
        * - Backup on shutdown is set to true
        * - The new empty DB is backed up, overwriting the existing backups
        * */
        if(config.backupOnShutdown && startedSuccessfully) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Backing up before shutting down");
                this.state.backup();
                this.close();
                logger.info("Shutting down");
            }));
        }
    }

    /**
     * Reads batches of new records from each of the input topics and creates the appropriate denormalized
     * records according to the top level relations. Performs a full commit and backup before returning.
     * @param runTimeS - Sets an amount of time in seconds for this method to run. The method will not run this amount
     *                of time exactly, but will stop after processing the latest batch of records. If set to 0,
     *                it will run until interrupted. Probably most useful for testing.
     */
    protected void build(int runTimeS) {
        logger.info("Building denormalized records");
        StopWatch backupWatch = new StopWatch();
        backupWatch.start();
        StopWatch runWatch = new StopWatch();
        runWatch.start();
        StopWatch commitWatch = new StopWatch();
        commitWatch.start();

        while(processRecords) {
            // Loop through each input topic and read a batch of records
            List<Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>>> topics =
                    new ArrayList<>(inputTopics.entrySet());
            List<String> rootEntities = Arrays.stream(relations).map(Relation::getEntity).collect(Collectors.toList());
            topics.sort((x, y) -> Boolean.compare(rootEntities.contains(x.getKey()), rootEntities.contains(y.getKey())));
            for (Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>> entry : topics) {
                String entity = entry.getKey();
                BaseTopic<BaseRecord, BaseRecord> inputTopic = entry.getValue();

                long topicLag;
                calculateRecordsToCreate();
                calculateTotalLag();

                do {
                    Iterator<ConsumerRecord<BaseRecord, BaseRecord>> records = inputTopic.readNext();
                    // Loop through each record and process it
                    while (records.hasNext()) {
                        ConsumerRecord<BaseRecord, BaseRecord> newRecord = records.next();
                        ByteArray primaryKey = newRecord.key().toByteArray();
                        for (Relation root : relations) {
                            Set<ByteArray> dePrimaryKeys = dePKsByType.get(root);
                            if (root.getEntity().equals(entity)) {
                                // The top level relation is the relation of the input record
                                dePrimaryKeys.add(primaryKey);
                            } else {
                                // Check the child relations instead
                                AbstractMap.SimpleEntry<Relation, Relation> child = getRelation(root, entity);
                                if (child != null && child.getValue() != null) {
                                    BaseIndex<BaseRecord, BaseRecord, Set<ByteArray>> parentIndex =
                                            fkIndices.get(createParentIndexName(root, child.getKey(), child.getValue()));
                                    ByteArray newParentKey = null;
                                    Set<ByteArray> oldParentKeys;
                                    if (newRecord.value() != null) {
                                        newParentKey = ByteArray.toByteArray(newRecord.value().get(child.getValue().getJoinKey()));
                                    }
                                    BaseIndex<BaseRecord, BaseRecord, Set<ByteArray>> joinIndex =
                                            fkIndices.get(createJoinIndexName(child.getValue()));
                                    oldParentKeys = ((Reversible) joinIndex).getForeignKeys(primaryKey);

                                    // Create the denormalized records
                                    if (oldParentKeys != null) {
                                        for (ByteArray oldParentKey : oldParentKeys) {
                                            if (!ObjectUtils.equals(oldParentKey, newParentKey)) {
                                                Set<ByteArray> primaryKeys = parentIndex.getIndexEntry(oldParentKey);
                                                if (primaryKeys != null) {
                                                    dePrimaryKeys.addAll(primaryKeys);
                                                }
                                            }
                                        }
                                    }
                                    if (newParentKey != null) {
                                        Set<ByteArray> primaryKeys = parentIndex.getIndexEntry(newParentKey);
                                        if (primaryKeys != null) {
                                            dePrimaryKeys.addAll(primaryKeys);
                                        }
                                    }
                                    // Update the join index
                                    updateJoinIndex(child.getValue(), primaryKey, newRecord);
                                }
                            }
                            int size = dePrimaryKeys.size();
                            if(size > config.createRecordsTrigger) {
                                createDenormalizedRecords(root, dePrimaryKeys);
                                dePrimaryKeys.clear();
                            }
                            metrics.denormalizedRecordsToCreateByTopic.get(root.getDenormalizedName()).update((long) size);
                        }
                        metrics.recordsConsumed.mark(1);
                        metrics.recordsConsumedByTopic.get(entity).mark(1);
                    }

                    topicLag = inputTopic.getLag();
                    metrics.topicLagByTopic.get(entity).update(topicLag);
                    reportRecordsToCreate();
                    reportTotalLag();

                    if(
                            (config.backupTimeS > 0 && backupWatch.getTime() > config.backupTimeS * 1000)
                            || (runWatch.getTime() > runTimeS * 1000 && runTimeS > 0)) {
                        try(Timer.Context context = metrics.backupsCreated.time()) {
                            logger.info("Performing a backup after a full commit");
                            calculateRecordsToCreate();
                            calculateTotalLag();
                            commit();
                            state.backup();
                            backupWatch.reset();
                            backupWatch.start();
                            if (runWatch.getTime() > runTimeS * 1000 && runTimeS > 0) return;
                        }
                    } else if(config.commitTimeS > 0 && commitWatch.getTime() > config.commitTimeS * 1000) {
                        try(Timer.Context context = metrics.stateCommitted.time()) {
                            logger.info("Performing a full commit");
                            calculateRecordsToCreate();
                            calculateTotalLag();
                            commit();
                            commitWatch.reset();
                            commitWatch.start();
                        }
                    }
                } while (topicLag > config.topicLagTrigger);
            }

            // Create the denormalized records that have been queued up
            for(Map.Entry<Relation, ByteArraySet> entry: dePKsByType.entrySet()) {
                createDenormalizedRecords(entry.getKey(), entry.getValue());
                entry.getValue().clear();
            }
        }
        commit();
    }

    /**
     * Calculates and reports the total number of denormalized records to create
     */
    public void calculateRecordsToCreate() {
        long totalRecords = 0;
        for(Map.Entry<Relation, ByteArraySet> entry: dePKsByType.entrySet()) {
            long records = entry.getValue().size();
            totalRecords += records;
            metrics.denormalizedRecordsToCreateByTopic.get(entry.getKey().getDenormalizedName()).update(records);
        }
        metrics.denormalizedRecordsToCreate.update(totalRecords);
    }

    /**
     * Calculates and reports the total lag for all input topics
     */
    public void calculateTotalLag() {
        long topicLag;
        long totalLag = 0;
        for(Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>> entry: inputTopics.entrySet()) {
            topicLag = entry.getValue().getLag();
            totalLag += topicLag;
            metrics.topicLagByTopic.get(entry.getKey()).update(topicLag);
        }
        metrics.topicLag.update(totalLag);
    }

    /**
     * Cleans up and closes anything used by Southpaw.
     */
    public void close() {
        metrics.close();
        state.close();
    }

    /**
     * Commit / flush offsets and data for the normalized topics and indices
     */
    public void commit() {
        // Commit / flush changes
        for(Map.Entry<String, BaseTopic<byte[], DenormalizedRecord>> topic: outputTopics.entrySet()) {
            topic.getValue().flush();
        }
        for(Map.Entry<String, BaseIndex<BaseRecord, BaseRecord, Set<ByteArray>>> index: fkIndices.entrySet()) {
            index.getValue().flush();
        }
        for(Map.Entry<Relation, ByteArraySet> entry: dePKsByType.entrySet()) {
            state.put(METADATA_KEYSPACE, createDePKEntryName(entry.getKey()).getBytes(), entry.getValue().serialize());
        }
        for(Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>> entry: inputTopics.entrySet()) {
            entry.getValue().commit();
        }
        state.flush();
    }

    /**
     * Create all indices for the given child relation and its children.
     * @param root - The root relation to create the indices for
     * @param parent - The parent relation to create the indices for
     * @param child - The child relation to create the indices for
     */
    protected void createChildIndices(
            Relation root,
            Relation parent,
            Relation child) {
        // Create this child's indices
        String joinIndexName = createJoinIndexName(child);
        fkIndices.put(joinIndexName, createFkIndex(joinIndexName, child.getEntity()));
        String parentIndexName = createParentIndexName(root, parent, child);
        fkIndices.put(parentIndexName, createFkIndex(parentIndexName, root.getEntity()));

        // Add its children's indices
        if(child.getChildren() != null) {
            for(Relation grandchild: child.getChildren()) {
                createChildIndices(root, child, grandchild);
            }
        }
    }

    /**
     * Recursively create a new denormalized record based on its relation definition, its parent's input record,
     * and its primary key.
     * @param root - The root relation of the denormalized record
     * @param relation - The current relation of the denormalized record to build
     * @param rootPrimaryKey - The PK of the root / denormalized record
     * @param relationPrimaryKey - The PK of the record
     * @return A fully created denormalized object
     */
    protected DenormalizedRecord createDenormalizedRecord(
            Relation root,
            Relation relation,
            ByteArray rootPrimaryKey,
            ByteArray relationPrimaryKey) {
        DenormalizedRecord denormalizedRecord = null;
        BaseTopic<BaseRecord, BaseRecord> relationTopic = inputTopics.get(relation.getEntity());
        BaseRecord relationRecord = relationTopic.readByPK(relationPrimaryKey);

        if(!(relationRecord == null || relationRecord.isEmpty())) {
            denormalizedRecord = new DenormalizedRecord();
            denormalizedRecord.setRecord(createInternalRecord(relationRecord));
            ChildRecords childRecords = new ChildRecords();
            denormalizedRecord.setChildren(childRecords);
            for (Relation child : relation.getChildren()) {
                ByteArray newParentKey = ByteArray.toByteArray(relationRecord.get(child.getParentKey()));
                updateParentIndex(root, relation, child, rootPrimaryKey, newParentKey);
                Map<ByteArray, DenormalizedRecord> records = new TreeMap<>();
                if (newParentKey != null) {
                    BaseIndex<BaseRecord, BaseRecord, Set<ByteArray>> joinIndex = fkIndices.get(createJoinIndexName(child));
                    Set<ByteArray> childPKs = joinIndex.getIndexEntry(newParentKey);
                    if (childPKs != null) {
                        for (ByteArray childPK : childPKs) {
                            DenormalizedRecord deChildRecord = createDenormalizedRecord(root, child, rootPrimaryKey, childPK);
                            if (deChildRecord != null) records.put(childPK, deChildRecord);
                        }
                    }
                    childRecords.setAdditionalProperty(child.getEntity(), new ArrayList<>(records.values()));
                }
            }
        }

        return denormalizedRecord;
    }

    /**
     * Creates a set of denormalized records and writes them to the appropriate output topic.
     * @param root - The top level relation defining the structure and relations of the denormalized records to create
     * @param rootRecordPKs - The primary keys of the root input records to create denormalized records for
     */
    protected void createDenormalizedRecords(
            Relation root,
            Set<ByteArray> rootRecordPKs) {
        for(ByteArray dePrimaryKey: rootRecordPKs) {
            if(dePrimaryKey != null) {
                BaseTopic<byte[], DenormalizedRecord> outputTopic = outputTopics.get(root.getDenormalizedName());
                scrubParentIndices(root, root, dePrimaryKey);
                DenormalizedRecord newDeRecord = createDenormalizedRecord(root, root, dePrimaryKey, dePrimaryKey);
                if(logger.getLevel().equals(Level.DEBUG)) {
                    try {
                        logger.debug(
                                String.format(
                                        "Root Entity: %s / Primary Key: %s",
                                        root.getEntity(), dePrimaryKey.toString()
                                )
                        );
                        logger.debug(mapper.writeValueAsString(newDeRecord));
                    } catch (Exception ex) {
                        // noop
                    }
                }

                outputTopic.write(
                        dePrimaryKey.getBytes(),
                        newDeRecord
                );
            }
            metrics.denormalizedRecordsCreated.mark(1);
            metrics.denormalizedRecordsCreatedByTopic.get(root.getDenormalizedName()).mark(1);
            metrics.denormalizedRecordsToCreate.update(metrics.denormalizedRecordsToCreate.getValue() - 1);
            metrics.denormalizedRecordsToCreateByTopic.get(root.getDenormalizedName())
                    .update(metrics.denormalizedRecordsToCreateByTopic.get(root.getDenormalizedName()).getValue() - 1);
        }
    }

    /**
     * Create the entry name for the denormalized PKs yet to be created
     * @return - The entry name
     */
    protected String createDePKEntryName(Relation root) {
        return String.join(SEP, PK, root.getDenormalizedName());
    }

    /**
     * Simple class for creating a FK multi index
     * @param indexName - The name of the index to create
     * @param indexedTopicName - The name of the indexed topic
     * @return A brand new, shiny index
     */
    protected BaseIndex<BaseRecord, BaseRecord, Set<ByteArray>> createFkIndex(
            String indexName,
            String indexedTopicName) {
        MultiIndex<BaseRecord, BaseRecord> index = new MultiIndex<>();
        index.configure(indexName, rawConfig, state, inputTopics.get(indexedTopicName));
        return index;
    }

    /**
     * Creates all indices for all relations provided to Southpaw. Note: indices to the input records
     * can be shared between top level relations.
     */
    protected void createIndices() {
        for(Relation root: relations) {
            // Children - PK, parent key and join key indices for the input topics
            for(Relation child: root.getChildren()) {
                createChildIndices(root, root, child);
            }
        }
    }

    /**
     * Creates an internal record for a denormalized record based on the input record
     * @param inputRecord - The input record used to generate the internal record
     * @return The internal record of the denormalized record that contains the actual values for the input record
     */
    protected Record createInternalRecord(BaseRecord inputRecord) {
        Record internalRecord = new Record();

        for(Map.Entry<String, ?> entry: inputRecord.toMap().entrySet()) {
            internalRecord.setAdditionalProperty(entry.getKey(), entry.getValue());
        }

        return internalRecord;
    }

    /**
     * Creates the join index name from the child relation
     * @param child - The child relation to create the join index name for
     * @return The join index name
     */
    protected String createJoinIndexName(Relation child) {
        return String.join(SEP, JK, child.getEntity(), child.getJoinKey());
    }

    /**
     * Creates all input topics for this relation and its children.
     * @param relation - The relation to create topics for
     * @return A map of topics
     */
    protected Map<String, BaseTopic<BaseRecord, BaseRecord>> createInputTopics(Relation relation)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Map<String, BaseTopic<BaseRecord, BaseRecord>> topics = new HashMap<>();

        topics.put(relation.getEntity(), createTopic(relation.getEntity()));

        if(relation.getChildren() != null) {
            for (Relation child : relation.getChildren()) {
                topics.putAll(createInputTopics(child));
            }
        }

        return topics;
    }

    /**
     * Creates an output topic for writing the created denormalized records to.
     * @param shortName - The short name of the topic to create
     * @return A shiny new topic
     * @throws ClassNotFoundException -
     * @throws IllegalAccessException -
     * @throws InstantiationException -
     */
    @SuppressWarnings("unchecked")
    protected BaseTopic<byte[], DenormalizedRecord> createOutputTopic(String shortName)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Map<String, Object> topicConfig = createTopicConfig(shortName);
        Class keySerdeClass = Class.forName(Preconditions.checkNotNull(topicConfig.get(BaseTopic.KEY_SERDE_CLASS_CONFIG).toString()));
        Class valueSerdeClass = Class.forName(Preconditions.checkNotNull(topicConfig.get(BaseTopic.VALUE_SERDE_CLASS_CONFIG).toString()));
        Serde<byte[]> keySerde = (Serde<byte[]>) keySerdeClass.newInstance();
        Serde<DenormalizedRecord> valueSerde = (Serde<DenormalizedRecord>) valueSerdeClass.newInstance();
        return createTopic(
                shortName,
                topicConfig,
                keySerde,
                valueSerde,
                new BaseFilter(),
                metrics
        );
    }

    /**
     * Creates the parent index name from the parent and child relations
     * @param root - The root relation to create the join index name for
     * @param parent - The parent relation to create the join index name for
     * @param child - The child relation to create the join index name for
     * @return The join index name
     */
    protected String createParentIndexName(Relation root, Relation parent, Relation child) {
        return String.join(SEP, PaK, root.getEntity(), parent.getEntity(), child.getParentKey());
    }

    /**
     * Creates a new topic with the given short name. Pulls the key and value serde classes from the configuration,
     * which should be subclasses of BaseSerde.
     * @param shortName - The short name of the topic, used to construct it's configuration by combining the specific
     *                  configuration based on this short name and the default configuration.
     * @return A shiny, new topic
     */
    @SuppressWarnings("unchecked")
    protected <K extends BaseRecord, V extends BaseRecord> BaseTopic<K, V> createTopic(String shortName)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Map<String, Object> topicConfig = createTopicConfig(shortName);
        Class keySerdeClass = Class.forName(Preconditions.checkNotNull(topicConfig.get(BaseTopic.KEY_SERDE_CLASS_CONFIG).toString()));
        Class valueSerdeClass = Class.forName(Preconditions.checkNotNull(topicConfig.get(BaseTopic.VALUE_SERDE_CLASS_CONFIG).toString()));
        Class filterClass = Class.forName(topicConfig.getOrDefault(BaseTopic.FILTER_CLASS_CONFIG, BaseTopic.FILTER_CLASS_DEFAULT).toString());
        BaseSerde<K> keySerde = (BaseSerde<K>) keySerdeClass.newInstance();
        BaseSerde<V> valueSerde = (BaseSerde<V>) valueSerdeClass.newInstance();
        BaseFilter filter = (BaseFilter) filterClass.newInstance();
        return createTopic(
                shortName,
                topicConfig,
                keySerde,
                valueSerde,
                filter,
                metrics
        );
    }

    /**
     * Creates a new topic with the given parameters. Also useful for overriding for testing purposes.
     * @param shortName - The short name of the topic
     * @param southpawConfig - The topic configuration
     * @param keySerde - The serde used to (de)serialize the key bytes
     * @param valueSerde - The serde used to (de)serialize the value bytes
     * @param filter - The filter used to filter out consumed records, treating them like a tombstone
     * @param <K> - The key type. Usually a primitive type or a type deriving from BaseRecord
     * @param <V> - The value type. Usually a primitive type or a type deriving from BaseRecord
     * @return A shiny, new topic
     */
    @SuppressWarnings("unchecked")
    protected <K, V> BaseTopic<K, V> createTopic(
            String shortName,
            Map<String, Object> southpawConfig,
            Serde<K> keySerde,
            Serde<V> valueSerde,
            BaseFilter filter,
            Metrics metrics) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class topicClass = Class.forName(Preconditions.checkNotNull(southpawConfig.get(BaseTopic.TOPIC_CLASS_CONFIG).toString()));
        BaseTopic<K, V> topic = (BaseTopic<K, V>) topicClass.newInstance();
        keySerde.configure(southpawConfig, true);
        valueSerde.configure(southpawConfig, false);
        filter.configure(southpawConfig);

        topic.configure(new TopicConfig<K, V>()
            .setShortName(shortName)
            .setSouthpawConfig(southpawConfig)
            .setState(state)
            .setKeySerde(keySerde)
            .setValueSerde(valueSerde)
            .setFilter(filter)
            .setMetrics(metrics));
        
        return topic;
    }

    /**
     * Creates a new map containing the topic config for the given config name. This is a merging of the default config
     * and the specific config for the given config name, if it exists.
     * @param configName - The name of the specific config to use
     * @return A map of configuration settings for a topic
     */
    @SuppressWarnings("unchecked")
    protected Map<String, Object> createTopicConfig(String configName) {
        Map<String, Object> topicsConfig = (Map<String, Object>) Preconditions.checkNotNull(rawConfig.get("topics"));
        Map<String, Object> defaultConfig = new HashMap<>(Preconditions.checkNotNull((Map<String, Object>) topicsConfig.get("default")));
        Map<String, Object> topicConfig = new HashMap<>(Preconditions.checkNotNull((Map<String, Object>) topicsConfig.get(configName)));
        defaultConfig.putAll(topicConfig);
        return defaultConfig;
    }

    /**
     * Deletes the backups for Southpaw state. Be very careful calling this! Unlike deleteState(), does not
     * require creating a new instance to continue processing.
     */
    public void deleteBackups() {
        logger.warn("Deleting backups!!!");
        state.deleteBackups();
        metrics.backupsDeleted.mark();
    }

    /**
     * Resets Southpaw by deleting it's state. Denormalized records written to output topics are not deleted.
     * You must create a new Southpaw object to keep processing.
     */
    public void deleteState() {
        logger.warn("Deleting state!!!");
        state.delete();
        metrics.statesDeleted.mark();
    }

    /**
     * Searches for the relation for the given child entity.
     * @param relation - The relation (and its children) to search
     * @param childEntity - The child entity to search for
     * @return The relation for the given child entity and it's parent, or null if it doesn't exist. Returned as a
     * Pair<Parent, Child> object. If the child entity found is the root entity, the Parent is null.
     */
    protected AbstractMap.SimpleEntry<Relation, Relation> getRelation(Relation relation, String childEntity) {
        Preconditions.checkNotNull(relation);
        if(relation.getEntity().equals(childEntity)) return new AbstractMap.SimpleEntry<>(null, relation);
        if(relation.getChildren() == null) return null;
        for(Relation child: relation.getChildren()) {
            if(child.getEntity().equals(childEntity)) return new AbstractMap.SimpleEntry<>(relation, child);
            AbstractMap.SimpleEntry<Relation, Relation> retVal = getRelation(child, childEntity);
            if(retVal != null) return retVal;
        }
        return null;
    }

    /**
     * Loads all top level relations from the given URIs. Needs to fit the relations JSON schema.
     * @param uris - The URIs to load
     * @return Top level relations from the given URIs
     * @throws IOException -
     * @throws URISyntaxException -
     */
    protected static Relation[] loadRelations(List<URI> uris) throws IOException, URISyntaxException {
        List<Relation> retVal = new ArrayList<>();
        for(URI uri: uris) {
            retVal.addAll(Arrays.asList(mapper.readValue(FileHelper.loadFileAsString(uri), Relation[].class)));
        }
        return retVal.toArray(new Relation[retVal.size()]);
    }

    public static void main(String args[]) throws Exception {
        String BUILD = "build";
        String CONFIG = "config";
        String DEBUG = "debug";
        String DELETE_BACKUP = "delete-backup";
        String DELETE_STATE = "delete-state";
        String HELP = "help";
        String RELATIONS = "relations";
        String RESTORE = "restore";
        String RESTORE_MODE = "restore-mode";

        OptionParser parser = new OptionParser() {
            {
                accepts(CONFIG, "Path to the Southpaw config file")
                        .withRequiredArg()
                        .required();
                accepts(RELATIONS, "Paths to one or more files containing input record relations")
                        .withRequiredArg()
                        .required();
                accepts(BUILD, "Builds denormalized records using an existing state.");
                accepts(DELETE_BACKUP, "Deletes existing backups specified in the config file. BE VERY CAREFUL WITH THIS!!!");
                accepts(DELETE_STATE, "Deletes the existing state specified in the config file. BE VERY CAREFUL WITH THIS!!!");
                accepts(RESTORE, "DEPRECATED: Restores the state from existing backups.");
                accepts(RESTORE_MODE, "Specifies when state restores should run. One of: never|always|when_needed Defaults to: never")
                        .withOptionalArg()
                        .ofType(String.class)
                        .defaultsTo(RestoreMode.NEVER);
                accepts(DEBUG, "Sets logging to DEBUG.")
                        .withOptionalArg();
                accepts(HELP, "Since you are seeing this, you probably know what this is for. :)")
                        .forHelp();
            }
        };
        OptionSet options = parser.parse(args);

        if (options.has(HELP)) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }
        if(options.has(DEBUG)) Logger.getRootLogger().setLevel(Level.DEBUG);

        Yaml yaml = new Yaml();
        Map<String, Object> config = yaml.load(FileHelper.getInputStream(new URI(options.valueOf(CONFIG).toString())));
        List<?> relations = options.valuesOf(RELATIONS);
        List<URI> relURIs = new ArrayList<>();
        for(Object relation: relations) relURIs.add(new URI(relation.toString()));
        Southpaw southpaw = new Southpaw(config, relURIs);

        if(options.has(DELETE_BACKUP)) {
            southpaw.deleteBackups();
        }
        if(options.has(DELETE_STATE)) {
            southpaw.deleteState();
        }

        if(options.has(RESTORE)) {
            logger.warn("The --restore flag is deprecated and will be removed in a future version. Please use --restore-mode.");
            southpaw.restore(RestoreMode.ALWAYS);
        } else {
            southpaw.restore(options.valueOf(RESTORE_MODE).toString());
        }

        southpaw.startedSuccessfully = true;
        if(options.has(BUILD)) {
            southpaw.run(0);
        }
    }

    /**
     * Reports the number of denormalized records that are queued to be created
     */
    protected void reportRecordsToCreate() {
        long totalRecords = 0;
        for(Map.Entry<String, StaticGauge<Long>> entry: metrics.denormalizedRecordsToCreateByTopic.entrySet()) {
            long records = entry.getValue().getValue();
            totalRecords += records;
            metrics.denormalizedRecordsToCreateByTopic.get(entry.getKey()).update(records);
        }
        metrics.denormalizedRecordsToCreate.update(totalRecords);
    }

    /**
     * Sums up and reports the total lag for all input topics
     */
    protected void reportTotalLag() {
        long topicLag;
        long totalLag = 0;
        for(Map.Entry<String, StaticGauge<Long>> entry: metrics.topicLagByTopic.entrySet()) {
            topicLag = entry.getValue().getValue();
            totalLag += topicLag;
            metrics.topicLagByTopic.get(entry.getKey()).update(topicLag);
        }
        metrics.topicLag.update(totalLag);
    }

    /**
     * Restores Southpaw state from the latest backup.
     */
    public void restore() throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        try(Timer.Context context = metrics.backupsRestored.time()) {
            logger.info("Restoring state from backups");
            state.restore();
            inputTopics = new HashMap<>(inputTopics.size());
            for(Relation root: relations) {
                inputTopics.putAll(createInputTopics(root));
            }
        }
    }

    public void restore(String mode) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        switch (mode) {
            case RestoreMode.ALWAYS:
                restore();
            case RestoreMode.WHEN_NEEDED:
                if (state.needsRestore()) {
                    logger.info("State not found locally. Attempting to restore state from backups");
                    restore();
                } else {
                    logger.info("State already exists. Skipping restore");
                }
                break;
            case RestoreMode.NEVER:
                break;
            default:
                throw new RuntimeException("Unsupported restore mode: " + mode);
        }
    }

    /**
     * Main method to call for reading input records and building denormalized records. Appropriately
     * switches between buildChildIndices and build to most efficiently build the records.
     * @param runTimeS - Sets an amount of time in seconds for this method to run. The method will not run this amount
     *                of time exactly, but will stop after processing the latest batch of records. If set to 0,
     *                it will run until interrupted. Probably most useful for testing.
     */
    public void run(int runTimeS) {
        build(runTimeS);
    }

    /**
     * Scrubs the parent indices of the given root primary key starting at the given relation. This is needed when a
     * tombstone record is seen for the root so that we remove all references to the now defunct root PK so we no
     * longer try to create (empty) records for it.
     * @param root - The root relation of the parent relation
     * @param parent  - The parent relation of the parent index to scrub
     * @param rootPrimaryKey - The primary key of the root record prior to the tombstone triggering this scrubbing
     */
    protected void scrubParentIndices(Relation root, Relation parent, ByteArray rootPrimaryKey) {
        Preconditions.checkNotNull(root);
        Preconditions.checkNotNull(parent);

        if(parent.getChildren() != null && rootPrimaryKey != null) {
            for(Relation child: parent.getChildren()) {
                BaseIndex<BaseRecord, BaseRecord, Set<ByteArray>> parentIndex =
                        fkIndices.get(createParentIndexName(root, parent, child));
                Set<ByteArray> oldForeignKeys = ((Reversible) parentIndex).getForeignKeys(rootPrimaryKey);
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
     * @param primaryKey - The primary key of the child record.
     * @param newRecord - The new version of the child record. May technically not be the latest version of a
     *                  record, but that is ok, since the index will eventually be updated with the latest
     *                  record.
     */
    protected void updateJoinIndex(
            Relation relation,
            ByteArray primaryKey,
            ConsumerRecord<BaseRecord, BaseRecord> newRecord) {
        Preconditions.checkNotNull(relation.getJoinKey());
        Preconditions.checkNotNull(newRecord);
        BaseIndex<BaseRecord, BaseRecord, Set<ByteArray>> joinIndex = fkIndices.get(createJoinIndexName(relation));
        Set<ByteArray> oldJoinKeys = ((Reversible) joinIndex).getForeignKeys(primaryKey);
        ByteArray newJoinKey = null;
        if(newRecord.value() != null) {
            newJoinKey = ByteArray.toByteArray(newRecord.value().get(relation.getJoinKey()));
        }
        boolean addNewJoinKey = true;
        if (oldJoinKeys != null && oldJoinKeys.size() > 0) {
            for(ByteArray oldJoinKey: oldJoinKeys) {
                if(!oldJoinKey.equals(newJoinKey)) {
                    joinIndex.remove(oldJoinKey, primaryKey);
                } else {
                    addNewJoinKey = false;
                }
            }
        }
        if (newJoinKey != null && addNewJoinKey) {
            joinIndex.add(newJoinKey, primaryKey);
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
    protected void updateParentIndex(
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

        BaseIndex<BaseRecord, BaseRecord, Set<ByteArray>> parentIndex =
                fkIndices.get(createParentIndexName(root, parent, child));
        if (newParentKey != null) parentIndex.add(newParentKey, rootPrimaryKey);
    }

    /**
     * Validates the given child relation.
     * @param relation - The child relation to validate
     */
    protected static void validateChildRelation(Relation relation) {
        Preconditions.checkNotNull(
                relation.getEntity(),
                "A child relation must correspond to an input record"
        );
        Preconditions.checkNotNull(
                relation.getJoinKey(),
                String.format("Child relation '%s' must have a join key", relation.getEntity())
        );
        Preconditions.checkNotNull(
                relation.getParentKey(),
                String.format("Child relation '%s' must have a parent key", relation.getEntity())
        );
    }

    /**
     * Validates that the given root relations are properly constructed.
     * @param relations - The relations to validate
     */
    protected static void validateRootRelations(Relation[] relations) {
        for(Relation relation: relations) {
            Preconditions.checkNotNull(
                    relation.getDenormalizedName(),
                    "A root relation must have a denormalized object name"
            );
            Preconditions.checkNotNull(
                    relation.getEntity(),
                    String.format("Top level relation '%s' must correspond to an input record type", relation.getDenormalizedName())
            );
            Preconditions.checkNotNull(
                    relation.getChildren(),
                    String.format("Top level relation '%s' must have children", relation.getDenormalizedName())
            );

            for(Relation child: relation.getChildren()) {
                validateChildRelation(child);
            }
        }
    }
}
