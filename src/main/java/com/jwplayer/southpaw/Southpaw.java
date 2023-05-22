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
import com.jwplayer.southpaw.index.Indices;
import com.jwplayer.southpaw.json.*;
import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.state.BaseState;
import com.jwplayer.southpaw.state.RocksDBState;
import com.jwplayer.southpaw.strategy.QueueingStrategy;
import com.jwplayer.southpaw.topic.BaseTopic;
import com.jwplayer.southpaw.topic.Topics;
import com.jwplayer.southpaw.util.ByteArray;
import com.jwplayer.southpaw.util.ByteArraySet;
import com.jwplayer.southpaw.util.FileHelper;
import com.jwplayer.southpaw.metric.Metrics;
import com.jwplayer.southpaw.util.RelationHelper;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.lang.time.StopWatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
     * The name of the state keyspace for Southpaw's metadata
     */
    public static final String METADATA_KEYSPACE = "__southpaw.metadata";
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
    private static final Logger logger =  LoggerFactory.getLogger(Southpaw.class);
    /**
     * Used for doing object <-> JSON mappings
     */
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Timer used to track when to back up
     */
    protected final StopWatch backupWatch = new StopWatch();
    /**
     * Timer used to track when to commit
     */
    protected final StopWatch commitWatch = new StopWatch();
    /**
     * Parsed Southpaw config
     */
    protected final Config config;
    /**
     * The indices used by Southpaw
     */
    protected Indices indices;
    /**
     * Simple metrics class for Southpaw
     */
    protected final Metrics metrics = new Metrics();
    /**
     * Timer used to track when to calculate and report certain metrics
     */
    protected final StopWatch metricsWatch = new StopWatch();
    /**
     * Tells the run() method to process records. If this is set to false, it will stop.
     */
    protected boolean processRecords = true;
    /**
     * The queueing strategy for records to be created
     */
    protected QueueingStrategy queueingStrategy;
    /**
     * The configuration for Southpaw. Mostly Kafka and topic configuration. See
     * test/test-resources/config.sample.yaml for an example.
     */
    protected final Map<String, Object> rawConfig;
    /**
     * The PKs of the denormalized records yet to be created
     */
    protected Map<Relation, Map<QueueingStrategy.Priority, ByteArraySet>> recordsToBeCreated = new HashMap<>();
    /**
     * The top level relations that instruct Southpaw how to construct denormalized records. See
     * test-resources/relations.sample.json for an example.
     */
    protected final Relation[] relations;
    /**
     * State for Southpaw
     */
    protected BaseState state;
    /**
     * The input and output topics used by Southpaw
     */
    protected Topics topics;

    /**
     * Base Southpaw config
     */
    protected static class Config {
        public static final String BACKUP_TIME_S_CONFIG = "backup.time.s";
        public static final int BACKUP_TIME_S_DEFAULT = 1800;
        public static final String COMMIT_TIME_S_CONFIG = "commit.time.s";
        public static final int COMMIT_TIME_S_DEFAULT = 0;
        public static final String CREATE_RECORDS_TIME_S_CONFIG = "create.records.time.s";
        public static final int CREATE_RECORDS_TIME_S_DEFAULT = 10;
        public static final String CREATE_RECORDS_TRIGGER_CONFIG = "create.records.trigger";
        public static final int CREATE_RECORDS_TRIGGER_DEFAULT = 250000;
        public static final String METRICS_REPORT_TIME_S_CONFIG = "metrics.report.time.s";
        public static final int METRICS_REPORT_TIME_S_DEFAULT = 30;
        public static final String QUEUEING_STRATEGY_CLASS_CONFIG = "queueing.strategy.class";
        public static final String QUEUEING_STRATEGY_CLASS_DEFAULT = "com.jwplayer.southpaw.strategy.QueueingStrategy";
        public static final String TOPIC_LAG_TRIGGER_CONFIG = "topic.lag.trigger";
        public static final String TOPIC_LAG_TRIGGER_DEFAULT = "1000";

        /**
         * Time interval (roughly) between backups
         */
        public int backupTimeS;
        /**
         * Time interval (roughly) between commits
         */
        public int commitTimeS;
        /**
         * Time interval (roughly) that controls how long denormalized records will be created before Southpaw will
         * stop to perform other actions such as metrics reporting and committing state.
         */
        public int createRecordsTimeS;
        /**
         * Config for when to create denormalized records once the number of records to create has exceeded a certain amount
         */
        public int createRecordsTrigger;
        /**
         * Time interval (roughly) that controls how often certain metrics are calculated and reported. These are
         * metrics that require calculations that we don't want to constantly perform. For example, topic lag.
         */
        public int metricsReportTimeS;
        /**
         * The class name for the queueing strategy
         */
        public String queueingStrategyClass;
        /**
         * Config for when to switch from one topic to the next (or to stop processing a topic entirely), when lag drops below this value
         */
        public int topicLagTrigger;

        public Config(Map<String, Object> rawConfig) {
            this.backupTimeS = (int) rawConfig.getOrDefault(BACKUP_TIME_S_CONFIG, BACKUP_TIME_S_DEFAULT);
            this.commitTimeS = (int) rawConfig.getOrDefault(COMMIT_TIME_S_CONFIG, COMMIT_TIME_S_DEFAULT);
            this.createRecordsTimeS = (int) rawConfig.getOrDefault(CREATE_RECORDS_TIME_S_CONFIG, CREATE_RECORDS_TIME_S_DEFAULT);
            this.createRecordsTrigger = (int) rawConfig.getOrDefault(CREATE_RECORDS_TRIGGER_CONFIG, CREATE_RECORDS_TRIGGER_DEFAULT);
            this.metricsReportTimeS = (int) rawConfig.getOrDefault(METRICS_REPORT_TIME_S_CONFIG, METRICS_REPORT_TIME_S_DEFAULT);
            this.queueingStrategyClass = (String) rawConfig.getOrDefault(QUEUEING_STRATEGY_CLASS_CONFIG, QUEUEING_STRATEGY_CLASS_DEFAULT);
            this.topicLagTrigger = (int) rawConfig.getOrDefault(TOPIC_LAG_TRIGGER_CONFIG, TOPIC_LAG_TRIGGER_DEFAULT);
        }
    }

    /**
     * Constructor
     * @param rawConfig - Southpaw configuration
     * @param relations - URIs to files containing the top level relations that define the denormalized
     *            objects to construct
     * @throws IOException -
     * @throws URISyntaxException -
     */
    public Southpaw(Map<String, Object> rawConfig, List<URI> relations) throws Exception {
        this(rawConfig, RelationHelper.loadRelations(Preconditions.checkNotNull(relations)));
    }

    /**
     * Constructor
     * @param rawConfig - Southpaw configuration
     * @param relations - The top level relations that define the denormalized objects to construct
     */
    public Southpaw(Map<String, Object> rawConfig, Relation[] relations) throws Exception {
        RelationHelper.validateRootRelations(relations);

        this.rawConfig = Preconditions.checkNotNull(rawConfig);
        this.config = new Config(rawConfig);
        this.relations = Preconditions.checkNotNull(relations);
        this.state = new RocksDBState(rawConfig);
        this.state.open();
        this.state.createKeySpace(METADATA_KEYSPACE);
        this.indices = new Indices(this.rawConfig, this.metrics, this.state, this.relations);
        Class<?> queueingStrategyClass = Class.forName(config.queueingStrategyClass);
        this.queueingStrategy = (QueueingStrategy) queueingStrategyClass.getDeclaredConstructor().newInstance();
        this.queueingStrategy.configure(this.rawConfig);
        this.topics = new Topics(this.rawConfig, this.metrics, this.state, this.relations);

        // Load any previous denormalized record PKs that have yet to be created
        for (Relation root : relations) {
            this.recordsToBeCreated.put(root, new HashMap<>());
            for (QueueingStrategy.Priority priority: QueueingStrategy.Priority.values()) {
                if (priority != QueueingStrategy.Priority.NONE) {
                    byte[] bytes = this.state.get(
                            METADATA_KEYSPACE,
                            createRecordsToBeCreatedKeyspace(root, priority).getBytes());
                    this.recordsToBeCreated.get(root).put(priority, ByteArraySet.deserialize(bytes));
                }
            }
        }

        // Start the global timers
        this.backupWatch.start();
        this.commitWatch.start();
        this.metricsWatch.start();
    }

    /**
     * Reads batches of new records from each of the input topics and creates the appropriate denormalized
     * records according to the top level relations. Performs a full commit and backup before returning.
     * @param runTimeMS - Sets an amount of time in milliseconds for this method to run. The method will not run this
     *                amount of time exactly, but will stop after processing the latest batch of records. If set to 0,
     *                it will run until interrupted. Useful for testing.
     */
    protected void build(long runTimeMS) {
        logger.info("Building denormalized records");
        StopWatch runWatch = new StopWatch();
        runWatch.start();

        List<String> entities = getEntities(relations);

        while(processRecords) {
            for (String entity: entities) {
                BaseTopic<BaseRecord, BaseRecord> inputTopic = topics.getInputTopic(entity);
                do {
                    Iterator<ConsumerRecord<BaseRecord, BaseRecord>> records = inputTopic.readNext();
                    while (records.hasNext()) {
                        ConsumerRecord<BaseRecord, BaseRecord> inputRecord = records.next();
                        for (Relation root : relations) {
                            Set<ByteArray> newDePKs = processInputRecord(root, entity, inputRecord);
                            queueDenormalizedPKs(root, entity, newDePKs);
                            handleTimers();
                        }
                    }
                } while (inputTopic.getLag() > config.topicLagTrigger);
            }

            processDenormalizedPKs();
            handleTimers();
            if (runTimeMS > 0 && runWatch.getTime() > runTimeMS) {
                logger.info("Stopping due to run time");
                break;
            }
        }
        commit();
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
        try(Timer.Context ignored = metrics.stateCommitted.time()) {
            logger.info("Performing a full commit");
            topics.flushOutputTopics();
            indices.flush();
            for(Map.Entry<Relation, Map<QueueingStrategy.Priority, ByteArraySet>> relationQueues: recordsToBeCreated.entrySet()) {
                for(Map.Entry<QueueingStrategy.Priority, ByteArraySet> priorityQueue: relationQueues.getValue().entrySet()){
                    state.put(METADATA_KEYSPACE,
                            createRecordsToBeCreatedKeyspace(relationQueues.getKey(), priorityQueue.getKey()).getBytes(),
                            priorityQueue.getValue().serialize());
                }
            }
            state.flush(METADATA_KEYSPACE);
            topics.commitInputTopics();
            state.flush();
            commitWatch.reset();
            commitWatch.start();
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
        BaseTopic<BaseRecord, BaseRecord> relationTopic = topics.getInputTopic(relation.getEntity());
        BaseRecord relationRecord = relationTopic.readByPK(relationPrimaryKey);

        if(!(relationRecord == null || relationRecord.isEmpty())) {
            denormalizedRecord = new DenormalizedRecord();
            denormalizedRecord.setRecord(createInternalRecord(relationRecord));
            ChildRecords childRecords = new ChildRecords();
            denormalizedRecord.setChildren(childRecords);
            for (Relation child : relation.getChildren()) {
                ByteArray joinKey = ByteArray.toByteArray(relationRecord.get(child.getParentKey()));
                indices.updateParentIndex(root, relation, child, rootPrimaryKey, joinKey);
                Map<ByteArray, DenormalizedRecord> records = new TreeMap<>();
                if (joinKey != null) {
                    Set<ByteArray> childPKs = indices.getRelationPKs(child, joinKey);
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
     * Creates a set of denormalized records and writes them to the appropriate output topic. Not all records are
     * guaranteed to be created because this method will only run for config.createRecordsTimeS seconds. Any records
     * that were created will be removed from the input set of PKs.
     * @param root - The top level relation defining the structure and relations of the denormalized records to create
     * @param priority - The priority of the queue the primary keys belong to
     * @param rootRecordPKs - The primary keys of the root input records to create denormalized records for
     */
    protected void createDenormalizedRecords(
            Relation root,
            QueueingStrategy.Priority priority,
            Set<ByteArray> rootRecordPKs) {
        Set<ByteArray> createdDePKs = new ByteArraySet();
        StopWatch timer = new StopWatch();
        timer.start();
        for(ByteArray dePrimaryKey: rootRecordPKs) {
            if(dePrimaryKey != null) {
                BaseTopic<byte[], DenormalizedRecord> outputTopic = topics.getOutputTopic(root.getDenormalizedName());
                indices.scrubParentIndices(root, root, dePrimaryKey);
                DenormalizedRecord newDeRecord = createDenormalizedRecord(root, root, dePrimaryKey, dePrimaryKey);
                if (logger.isDebugEnabled()) {
                    try {
                        logger.debug(String.format(
                                "Root Entity: %s / Primary Key: %s", root.getEntity(), dePrimaryKey));
                        logger.debug(mapper.writeValueAsString(newDeRecord));
                    } catch (Exception ex) {
                        // noop
                    }
                }

                outputTopic.write(dePrimaryKey.getBytes(), newDeRecord);
                metrics.denormalizedRecordsCreated.mark(1);
                metrics.denormalizedRecordsCreatedByTopic.get(root.getDenormalizedName()).mark(1);
                metrics.denormalizedRecordsCreatedByTopicAndPriority.get(root.getDenormalizedName()).get(priority).mark(1);
                createdDePKs.add(dePrimaryKey);
            }
            if(config.createRecordsTimeS > 0 && timer.getTime() > config.createRecordsTimeS * 1000L) {
                break;
            }
        }
        if(createdDePKs.size() == rootRecordPKs.size()) {
            rootRecordPKs.clear();
        } else {
            rootRecordPKs.removeAll(createdDePKs);
        }
        handleTimers();
    }

    /**
     * Create the keyspace for the primary keys of the denormalized records yet to be created. Only low and high
     * priorities have the priority in the keyspace for backwards compatibility.
     * @param root - The root relation
     * @param priority - The priority of the records to be created
     * @return - The name of the keyspace
     */
    protected static String createRecordsToBeCreatedKeyspace(Relation root, QueueingStrategy.Priority priority) {
        if(priority == QueueingStrategy.Priority.MEDIUM) {
            return String.join(SEP, PK, root.getDenormalizedName());
        } else {
            return String.join(SEP, PK, priority.name().toLowerCase(), root.getDenormalizedName());
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
     * Deletes the backups for Southpaw state. Be very careful calling this! Unlike deleteState(), does not
     * require creating a new instance to continue processing.
     */
    public static void deleteBackups(Map<String, Object> config) {
        BaseState state = null;
        try {
            state =  new RocksDBState(config);
            state.deleteBackups();
        } finally {
            if (state != null) {
                state.close();
            }
        }
    }

    /**
     * Resets Southpaw by deleting it's state. Denormalized records written to output topics are not deleted.
     * You must create a new Southpaw object to keep processing.
     */
    public static void deleteState(Map<String, Object> config) {
        BaseState state =  new RocksDBState(config);
        state.delete();
    }

    /**
     * Gets a list of all entities from all relations, sorted so that the root entities are last
     * @param relations - The relations to extract the entities from
     * @return A list of all entities contained in the given relations
     */
    protected static List<String> getEntities(Relation[] relations) {
        Set<String> rootEntities = Arrays.stream(relations).map(Relation::getEntity).collect(Collectors.toSet());
        List<String> entities = new ArrayList<>(RelationHelper.getEntities(relations));
        entities.sort((x, y) -> Boolean.compare(rootEntities.contains(x), rootEntities.contains(y)));
        return entities;
    }

    /**
     * Handles the various timers for backups, commits, etc.
     */
    protected void handleTimers() {
        metrics.timeSinceLastBackup.update(backupWatch.getTime());

        if(config.backupTimeS > 0 && backupWatch.getTime() > config.backupTimeS * 1000L) {
            try(Timer.Context ignored = metrics.backupsCreated.time()) {
                logger.info("Performing a backup after a full commit");
                commit();
                state.backup();
                backupWatch.reset();
                backupWatch.start();
            }
        }
        if(config.commitTimeS > 0 && commitWatch.getTime() > config.commitTimeS * 1000L) {
            commit();
        }
        if(config.metricsReportTimeS > 0 && metricsWatch.getTime() > config.metricsReportTimeS * 1000L) {
            reportMetrics();
            metricsWatch.reset();
            metricsWatch.start();
        }
    }

    public static void main(String[] args) throws Exception {
        String BUILD = "build";
        String CONFIG = "config";
        String DELETE_BACKUP = "delete-backup";
        String DELETE_STATE = "delete-state";
        String HELP = "help";
        String RELATIONS = "relations";
        String RESTORE = "restore";
        String VERIFY_STATE = "verify-state";

        OptionParser parser = new OptionParser() {
            {
                accepts(CONFIG, "Path to the Southpaw config file").withRequiredArg().required();
                accepts(RELATIONS, "Paths to one or more files containing input record relations").withRequiredArg().required();
                accepts(BUILD, "Builds denormalized records using an existing state.");
                accepts(DELETE_BACKUP, "Deletes existing backups specified in the config file. BE VERY CAREFUL WITH THIS!!!");
                accepts(DELETE_STATE, "Deletes the existing state specified in the config file. BE VERY CAREFUL WITH THIS!!!");
                accepts(RESTORE, "Restores the state from existing backups.");
                accepts(HELP, "Since you are seeing this, you probably know what this is for. :)").forHelp();
                accepts(VERIFY_STATE, "Verifies that the Southpaw state indices and reverse indices are in sync");
            }
        };
        OptionSet options = parser.parse(args);

        if (options.has(HELP)) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Yaml yaml = new Yaml();
        Map<String, Object> config = yaml.load(FileHelper.getInputStream(new URI(options.valueOf(CONFIG).toString())));
        List<?> relations = options.valuesOf(RELATIONS);
        List<URI> relURIs = new ArrayList<>();
        for(Object relation: relations) relURIs.add(new URI(relation.toString()));

        if(options.has(VERIFY_STATE)) {
            Southpaw southpaw = new Southpaw(config, relURIs);
            try{
                southpaw.indices.verifyIndices();
            } finally {
                southpaw.close();
            }
            System.exit(0);
        }

        if(options.has(DELETE_BACKUP)) {
            Southpaw.deleteBackups(config);
        }
        if(options.has(DELETE_STATE)) {
            Southpaw.deleteState(config);
        }
        if(options.has(RESTORE)) {
            Southpaw.restore(config);
        }

        if(options.has(BUILD)) {
            Southpaw southpaw = new Southpaw(config, relURIs);
            try{
                southpaw.run(0L);
            } finally {
                southpaw.close();
            }
        }
    }

    /**
     * Processes a new input record by looking up all PKs of the denormalized records that need to be (re)created
     * based on the root relation and the entity name. Additionally, it updates the join index.
     * @param root - The root relation
     * @param entity - The entity that belongs to the input record
     * @param inputRecord - The input record to process
     * @return A set of PKs of the denormalized records that need to be (re)created
     */
    protected Set<ByteArray> processInputRecord(
            Relation root,
            String entity,
            ConsumerRecord<BaseRecord, BaseRecord> inputRecord) {
        ByteArray newRecordPK = inputRecord.key().toByteArray();
        Set<ByteArray> newDePKs = new ByteArraySet();
        if (root.getEntity().equals(entity)) {
            // The top level relation is the relation of the input record
            newDePKs.add(newRecordPK);
        } else {
            // Check the child relations instead
            AbstractMap.SimpleEntry<Relation, Relation> child = RelationHelper.getRelation(root, entity);
            if (child != null && child.getValue() != null) {
                // Get join keys
                Set<ByteArray> joinKeys = indices.getRelationJKs(child.getValue(), newRecordPK);
                if(joinKeys == null) joinKeys = new ByteArraySet();
                if (inputRecord.value() != null) {
                    joinKeys.add(ByteArray.toByteArray(inputRecord.value().get(child.getValue().getJoinKey())));
                }
                // Get the PKs for the denormalized records to create
                for (ByteArray joinKey : joinKeys) {
                    Set<ByteArray> primaryKeys = indices.getRootPKs(root, child.getKey(), child.getValue(), joinKey);
                    if (primaryKeys != null) {
                        newDePKs.addAll(primaryKeys);
                    }
                }
                // Update the join index
                indices.updateJoinIndex(child.getValue(), inputRecord);
            }
        }
        return newDePKs;
    }

    /**
     * Processes previously queued denormalized PKs. High and medium queues are completely processed, while low
     * priority queues only get a single pass.
     */
    protected void processDenormalizedPKs() {
        for(Map.Entry<Relation, Map<QueueingStrategy.Priority, ByteArraySet>> relationQueues: recordsToBeCreated.entrySet()) {
            for(Map.Entry<QueueingStrategy.Priority, ByteArraySet> priorityQueue: relationQueues.getValue().entrySet()) {
                do {
                    createDenormalizedRecords(relationQueues.getKey(), priorityQueue.getKey(), priorityQueue.getValue());
                } while (priorityQueue.getValue().size() > 0 && priorityQueue.getKey() != QueueingStrategy.Priority.LOW);
            }
        }
    }

    /**
     * Queues new denormalized PKs, and if too many have been queued, creates denormalized records until the queue
     * sizes are below the configured amount. Additionally, all records for the high priority queue are created,
     * even if the queue is below the configured amount.
     * @param root - The root relation of these PKs
     * @param entity - The entity that triggered these records to be created
     * @param newDePKs - The PKs of the denormalized records to create
     */
    protected void queueDenormalizedPKs(Relation root, String entity, Set<ByteArray> newDePKs) {
        if(newDePKs.size() == 0) return;
        QueueingStrategy.Priority priority = queueingStrategy.getPriority(root.getDenormalizedName(), entity, newDePKs);
        if(priority == QueueingStrategy.Priority.NONE) {
            metrics.denormalizedRecordsDropped.mark(newDePKs.size());
            metrics.denormalizedRecordsDroppedByTopic.get(root.getDenormalizedName()).mark(newDePKs.size());
            return;
        }
        ByteArraySet queue = recordsToBeCreated.get(root).get(priority);
        queue.addAll(newDePKs);
        if(priority == QueueingStrategy.Priority.HIGH) {
            do {
                createDenormalizedRecords(root, priority, queue);
            } while (queue.size() > 0);
        } else {
            while(queue.size() > config.createRecordsTrigger) {
                createDenormalizedRecords(root, priority, queue);
            }
        }
    }

    /**
     * Calculates and reports metrics
     */
    protected void reportMetrics() {
        // Denormalized records to create
        long totalRecords = 0;
        for(Map.Entry<Relation, Map<QueueingStrategy.Priority, ByteArraySet>> relationQueues: recordsToBeCreated.entrySet()) {
            long topicRecords = 0;
            for(Map.Entry<QueueingStrategy.Priority, ByteArraySet> priorityQueue: relationQueues.getValue().entrySet()) {
                long records = priorityQueue.getValue().size();
                topicRecords += records;
                totalRecords += records;
                metrics.denormalizedRecordsToCreateByTopicAndPriority
                        .get(relationQueues.getKey().getDenormalizedName())
                        .get(priorityQueue.getKey())
                        .update(records);
            }
            metrics.denormalizedRecordsToCreateByTopic
                    .get(relationQueues.getKey().getDenormalizedName())
                    .update(topicRecords);
        }
        metrics.denormalizedRecordsToCreate.update(totalRecords);
        topics.reportMetrics();
    }

    /**
     * Restores Southpaw state from the latest backup.
     */
    public static void restore(Map<String, Object> config){
        RocksDBState state = new RocksDBState(config);
        state.restore();
    }

    /**
     * Main method to call for reading input records and building denormalized records. Appropriately
     * switches between buildChildIndices and build to most efficiently build the records.
     * @param runTimeMS - Sets an amount of time in milliseconds for this method to run. The method will not run this
     *                amount of time exactly, but will stop after processing the latest batch of records. If set to 0,
     *                it will run until interrupted. Useful for testing.
     */
    public void run(long runTimeMS) {
        build(runTimeMS);
    }
}
