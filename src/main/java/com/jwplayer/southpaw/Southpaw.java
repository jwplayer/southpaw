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
import com.jwplayer.southpaw.filter.BaseFilter;
import com.jwplayer.southpaw.index.Indices;
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
import com.jwplayer.southpaw.topic.TopicConfig;
import com.jwplayer.southpaw.util.RelationHelper;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.lang.time.StopWatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
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
     * The PKs of the denormalized records yet to be created
     */
    protected Map<Relation, ByteArraySet> dePKsByType = new HashMap<>();
    /**
     * The indices used by Southpaw
     */
    protected Indices indices;
    /**
     * A map of all input topics needed by Southpaw. The key is the short name of the topic.
     */
    protected Map<String, BaseTopic<BaseRecord, BaseRecord>> inputTopics;
    /**
     * Simple metrics class for Southpaw
     */
    protected final Metrics metrics = new Metrics();
    /**
     * Timer used to track when to calculate and report certain metrics
     */
    protected final StopWatch metricsWatch = new StopWatch();
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
     * State for Southpaw
     */
    protected BaseState state;

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
         * Config for when to switch from one topic to the next (or to stop processing a topic entirely), when lag drops below this value
         */
        public int topicLagTrigger;

        public Config(Map<String, Object> rawConfig) {
            this.backupTimeS = (int) rawConfig.getOrDefault(BACKUP_TIME_S_CONFIG, BACKUP_TIME_S_DEFAULT);
            this.commitTimeS = (int) rawConfig.getOrDefault(COMMIT_TIME_S_CONFIG, COMMIT_TIME_S_DEFAULT);
            this.createRecordsTimeS = (int) rawConfig.getOrDefault(CREATE_RECORDS_TIME_S_CONFIG, CREATE_RECORDS_TIME_S_DEFAULT);
            this.createRecordsTrigger = (int) rawConfig.getOrDefault(CREATE_RECORDS_TRIGGER_CONFIG, CREATE_RECORDS_TRIGGER_DEFAULT);
            this.metricsReportTimeS = (int) rawConfig.getOrDefault(METRICS_REPORT_TIME_S_CONFIG, METRICS_REPORT_TIME_S_DEFAULT);
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
    public Southpaw(Map<String, Object> rawConfig, List<URI> relations)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, IOException, URISyntaxException, NoSuchMethodException, InvocationTargetException {
        this(rawConfig, RelationHelper.loadRelations(Preconditions.checkNotNull(relations)));
    }

    /**
     * Constructor
     * @param rawConfig - Southpaw configuration
     * @param relations - The top level relations that define the denormalized objects to construct
     */
    public Southpaw(Map<String, Object> rawConfig, Relation[] relations)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        RelationHelper.validateRootRelations(relations);

        this.rawConfig = Preconditions.checkNotNull(rawConfig);
        this.config = new Config(rawConfig);
        this.relations = Preconditions.checkNotNull(relations);
        this.state = new RocksDBState(rawConfig);
        this.state.open();
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
        this.indices = new Indices(rawConfig, this.state);
        this.indices.createIndices(relations);

        // Load any previous denormalized record PKs that have yet to be created
        for (Relation root : relations) {
            byte[] bytes = this.state.get(METADATA_KEYSPACE, createDePKEntryName(root).getBytes());
            this.dePKsByType.put(root, ByteArraySet.deserialize(bytes));
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

        List<Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>>> topics = new ArrayList<>(inputTopics.entrySet());
        List<String> rootEntities = Arrays.stream(relations).map(Relation::getEntity).collect(Collectors.toList());
        topics.sort((x, y) -> Boolean.compare(rootEntities.contains(x.getKey()), rootEntities.contains(y.getKey())));

        while(processRecords) {
            for (Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>> entry : topics) {
                BaseTopic<BaseRecord, BaseRecord> inputTopic = entry.getValue();
                do {
                    String entity = entry.getKey();
                    Iterator<ConsumerRecord<BaseRecord, BaseRecord>> records = inputTopic.readNext();
                    while (records.hasNext()) {
                        ConsumerRecord<BaseRecord, BaseRecord> inputRecord = records.next();
                        for (Relation root : relations) {
                            Set<ByteArray> newDePKs = processInputRecord(root, entity, inputRecord);
                            queueDenormalizedPKs(root, newDePKs);
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
            for(Map.Entry<String, BaseTopic<byte[], DenormalizedRecord>> topic: outputTopics.entrySet()) {
                topic.getValue().flush();
            }
            indices.flush();
            for(Map.Entry<Relation, ByteArraySet> entry: dePKsByType.entrySet()) {
                state.put(METADATA_KEYSPACE, createDePKEntryName(entry.getKey()).getBytes(), entry.getValue().serialize());
                state.flush(METADATA_KEYSPACE);
            }
            for(Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>> entry: inputTopics.entrySet()) {
                entry.getValue().commit();
            }
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
        BaseTopic<BaseRecord, BaseRecord> relationTopic = inputTopics.get(relation.getEntity());
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
     * @param rootRecordPKs - The primary keys of the root input records to create denormalized records for
     */
    protected void createDenormalizedRecords(
            Relation root,
            Set<ByteArray> rootRecordPKs) {
        Set<ByteArray> createdDePKs = new ByteArraySet();
        StopWatch timer = new StopWatch();
        timer.start();
        for(ByteArray dePrimaryKey: rootRecordPKs) {
            if(dePrimaryKey != null) {
                BaseTopic<byte[], DenormalizedRecord> outputTopic = outputTopics.get(root.getDenormalizedName());
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
    }

    /**
     * Create the entry name for the denormalized PKs yet to be created
     * @return - The entry name
     */
    protected String createDePKEntryName(Relation root) {
        return String.join(SEP, PK, root.getDenormalizedName());
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
     * Creates all input topics for this relation and its children.
     * @param relation - The relation to create topics for
     * @return A map of topics
     */
    protected Map<String, BaseTopic<BaseRecord, BaseRecord>> createInputTopics(Relation relation)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
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
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        Map<String, Object> topicConfig = createTopicConfig(shortName);
        Class<?> keySerdeClass = Class.forName(Preconditions.checkNotNull(topicConfig.get(BaseTopic.KEY_SERDE_CLASS_CONFIG).toString()));
        Class<?> valueSerdeClass = Class.forName(Preconditions.checkNotNull(topicConfig.get(BaseTopic.VALUE_SERDE_CLASS_CONFIG).toString()));
        Serde<byte[]> keySerde = (Serde<byte[]>) keySerdeClass.getDeclaredConstructor().newInstance();
        Serde<DenormalizedRecord> valueSerde = (Serde<DenormalizedRecord>) valueSerdeClass.getDeclaredConstructor().newInstance();
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
     * Creates a new topic with the given short name. Pulls the key and value serde classes from the configuration,
     * which should be subclasses of BaseSerde.
     * @param shortName - The short name of the topic, used to construct its configuration by combining the specific
     *                  configuration based on this short name and the default configuration.
     * @return A shiny, new topic
     */
    @SuppressWarnings("unchecked")
    protected <K extends BaseRecord, V extends BaseRecord> BaseTopic<K, V> createTopic(String shortName)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        Map<String, Object> topicConfig = createTopicConfig(shortName);
        Class<?> keySerdeClass = Class.forName(Preconditions.checkNotNull(topicConfig.get(BaseTopic.KEY_SERDE_CLASS_CONFIG).toString()));
        Class<?> valueSerdeClass = Class.forName(Preconditions.checkNotNull(topicConfig.get(BaseTopic.VALUE_SERDE_CLASS_CONFIG).toString()));
        Class<?> filterClass = Class.forName(topicConfig.getOrDefault(BaseTopic.FILTER_CLASS_CONFIG, BaseTopic.FILTER_CLASS_DEFAULT).toString());
        BaseSerde<K> keySerde = (BaseSerde<K>) keySerdeClass.getDeclaredConstructor().newInstance();
        BaseSerde<V> valueSerde = (BaseSerde<V>) valueSerdeClass.getDeclaredConstructor().newInstance();
        BaseFilter filter = (BaseFilter) filterClass.getDeclaredConstructor().newInstance();
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
     * Creates a new topic with the given parameters. Also, useful for overriding for testing purposes.
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
            Metrics metrics) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        Class<?> topicClass = Class.forName(Preconditions.checkNotNull(southpawConfig.get(BaseTopic.TOPIC_CLASS_CONFIG).toString()));
        BaseTopic<K, V> topic = (BaseTopic<K, V>) topicClass.getDeclaredConstructor().newInstance();
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
     * Processes previously queued denormalized PKs.
     */
    protected void processDenormalizedPKs() {
        for(Map.Entry<Relation, ByteArraySet> entry: dePKsByType.entrySet()) {
            do {
                createDenormalizedRecords(entry.getKey(), entry.getValue());
                handleTimers();
            } while (entry.getValue().size() > 0);
        }
    }

    /**
     * Queues new denormalized PKs, and if too many have been queued, creates denormalized records until the queue
     * size is below the configured amount.
     * @param root - The root relation of these PKs
     * @param newDePKs - The PKs of the denormalized records to create
     */
    protected void queueDenormalizedPKs(Relation root, Set<ByteArray> newDePKs) {
        Set<ByteArray> dePKsToCreate = dePKsByType.get(root);
        dePKsToCreate.addAll(newDePKs);
        while(dePKsToCreate.size() > config.createRecordsTrigger) {
            createDenormalizedRecords(root, dePKsToCreate);
            handleTimers();
        }
    }

    /**
     * Calculates and reports metrics
     */
    protected void reportMetrics() {
        // Denormalized records to create
        long totalRecords = 0;
        for(Map.Entry<Relation, ByteArraySet> entry: dePKsByType.entrySet()) {
            long records = entry.getValue().size();
            totalRecords += records;
            metrics.denormalizedRecordsToCreateByTopic.get(entry.getKey().getDenormalizedName()).update(records);
        }
        metrics.denormalizedRecordsToCreate.update(totalRecords);
        // Topic lag
        long totalLag = 0;
        for(Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>> entry: inputTopics.entrySet()) {
            long topicLag = entry.getValue().getLag();
            totalLag += topicLag;
            metrics.topicLagByTopic.get(entry.getKey()).update(topicLag);
        }
        metrics.topicLag.update(totalLag);
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
