package com.jwplayer.southpaw.topic;

import com.google.common.base.Preconditions;
import com.jwplayer.southpaw.filter.BaseFilter;
import com.jwplayer.southpaw.json.DenormalizedRecord;
import com.jwplayer.southpaw.json.Relation;
import com.jwplayer.southpaw.metric.Metrics;
import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.serde.BaseSerde;
import com.jwplayer.southpaw.state.BaseState;
import org.apache.kafka.common.serialization.Serde;

import java.util.HashMap;
import java.util.Map;

/**
 * Class for containing the various kinds of topics used by Southpaw, as well as code for interacting with these
 * topics.
 */
public class Topics {
    /**
     * Configuration for the topics
     */
    protected final Map<String, Object> config;
    /**
     * A map of all input topics needed by Southpaw. The key is the short name of the topic.
     */
    protected Map<String, BaseTopic<BaseRecord, BaseRecord>> inputTopics = new HashMap<>();
    /**
     * Metrics class that contains topic related metrics
     */
    protected final Metrics metrics;
    /**
     * A map of the output topics needed where the denormalized records are written. The key is the short name of
     * the topic.
     */
    protected Map<String, BaseTopic<byte[], DenormalizedRecord>> outputTopics = new HashMap<>();
    /**
     * State for Southpaw
     */
    protected BaseState state;

    public Topics(Map<String, Object> config, Metrics metrics, BaseState state, Relation[] relations) throws Exception {
        this.config = config;
        this.metrics = metrics;
        this.state = state;
        for(Relation root: relations) {
            this.inputTopics.putAll(createInputTopics(root));
            this.outputTopics.put(root.getDenormalizedName(), createOutputTopic(root.getDenormalizedName()));
            this.metrics.registerOutputTopic(root.getDenormalizedName());
        }
        for(Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>> entry: this.inputTopics.entrySet()) {
            this.metrics.registerInputTopic(entry.getKey());
        }
    }

    /**
     * Commits the state for the input topics
     */
    public void commitInputTopics() {
        for(Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>> entry: inputTopics.entrySet()) {
            entry.getValue().commit();
        }
    }

    /**
     * Creates all input topics for this relation and its children.
     * @param relation - The relation to create topics for
     * @return A map of topics
     */
    protected Map<String, BaseTopic<BaseRecord, BaseRecord>> createInputTopics(Relation relation)
            throws Exception {
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
            throws Exception {
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
            throws Exception {
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
            Metrics metrics) throws Exception {
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
        Map<String, Object> topicsConfig = (Map<String, Object>) Preconditions.checkNotNull(config.get("topics"));
        Map<String, Object> defaultConfig = new HashMap<>(Preconditions.checkNotNull((Map<String, Object>) topicsConfig.get("default")));
        Map<String, Object> topicConfig = new HashMap<>(Preconditions.checkNotNull((Map<String, Object>) topicsConfig.get(configName)));
        defaultConfig.putAll(topicConfig);
        return defaultConfig;
    }

    /**
     * Flushes records written to the output topics
     */
    public void flushOutputTopics() {
        for(Map.Entry<String, BaseTopic<byte[], DenormalizedRecord>> topic: outputTopics.entrySet()) {
            topic.getValue().flush();
        }
    }

    /**
     * Gets the input topic based on the entity name
     * @param entity - The entity of the topic to get
     * @return The input topic
     */
    public BaseTopic<BaseRecord, BaseRecord> getInputTopic(String entity) {
        return inputTopics.get(entity);
    }

    /**
     * Gets the output topic based on the denormalized name
     * @param denormalizedName - The denormalized name of the topic to get
     * @return The output topic
     */
    public BaseTopic<byte[], DenormalizedRecord> getOutputTopic(String denormalizedName) {
        return outputTopics.get(denormalizedName);
    }

    public void reportMetrics() {
        // Topic lag
        long totalLag = 0;
        for(Map.Entry<String, BaseTopic<BaseRecord, BaseRecord>> entry: inputTopics.entrySet()) {
            long topicLag = entry.getValue().getLag();
            totalLag += topicLag;
            metrics.topicLagByTopic.get(entry.getKey()).update(topicLag);
        }
        metrics.topicLag.update(totalLag);
    }
}
