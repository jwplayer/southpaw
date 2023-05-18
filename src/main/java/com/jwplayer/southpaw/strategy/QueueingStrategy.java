package com.jwplayer.southpaw.strategy;

import com.jwplayer.southpaw.util.ByteArray;

import java.util.Map;
import java.util.Set;

/**
 * Class for denormalized record queueing strategies that determines which queue a set of denormalized record primary
 * keys get put in. This class is the default implementation that always returns a medium priority.
 */
public class QueueingStrategy {
    public enum Priority { HIGH, LOW, MEDIUM, NONE };

    /**
     * Configures this class with a given config map
     * @param config - The map of config options to configure this class with
     */
    public void configure(Map<String, Object> config) {
    }

    /**
     * Gets the priority based on the given denormalized name, entity, and PKs to be created
     * @param denormalizedName - The name of the denormalized record
     * @param entity - The entity of the record that triggered these PKs to be queued
     * @param primaryKeys - The new primary keys that will be queued
     * @return The priority of the queue for these primary keys
     */
    public Priority getPriority(String denormalizedName, String entity, Set<ByteArray> primaryKeys) {
        return Priority.MEDIUM;
    }
}
