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
package com.jwplayer.southpaw.state;

import java.util.AbstractMap;
import java.util.Map;


/**
 * Base state class for permanently storing indices and data
 */
public abstract class BaseState {

    private boolean isOpen = false;

    /**
     * The set of predefined RestoreMode options for {@link BaseState} restores.
     */
    public enum RestoreMode {
        /**
         * Always attempt to perform a state restore. If a state backup exists this will restore
         * and overwrite any existing state that might exist.
         */
        ALWAYS ("always"),

        /**
         * Only attempt to perform a state restore if local state doesn't exist
         */
        WHEN_NEEDED ("when_needed"),

        /**
         * Never attempt to perform a state restore.
         */
        NEVER ("never");

        private final String value;

        RestoreMode(String value) {
            this.value = value;
        }

        public String getValue() {
            return this.value;
        }

        /**
         * Determine if the supplied value is one of the predefined state restore modes.
         * @param value the configuration property value; may not be null
         * @return the matching mode, or null if match is not found
         */
        public static RestoreMode parse(String value) {
            if (value == null) {
                return null;
            }

            value = value.trim();
            for (RestoreMode option : RestoreMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }
    }

    public BaseState() { }

    public BaseState(Map<String, Object> config) {
        configure(config);
    }

    /**
     * An iterator for keys in the state.
     */
    public abstract static class Iterator implements java.util.Iterator<AbstractMap.SimpleEntry<byte[], byte[]>> {
        /**
         * Close the iterator after use.
         */
        public abstract void close();

        /**
         * Reset the iterator so it may be reused.
         */
        public abstract void reset();
    }

    /**
     * Backup this state
     */
    public abstract void backup();

    /**
     * Close the state
     */
    public void close() {
        this.isOpen = false;
    }

    /**
     * Configure the state. Should be called after instantiation, but before opening the state.
     * @param config - Configuration for the state
     */
    public abstract void configure(Map<String, Object> config);

    /**
     * Create a new key space
     * @param keySpace - The key space to create
     */
    public abstract void createKeySpace(String keySpace);

    /**
     * Delete the state
     */
    public abstract void delete();

    /**
     * Delete the given key
     * @param keySpace - The key space where the key is stored
     * @param key - The key to delete
     */
    public abstract void delete(String keySpace, byte[] key);

    /**
     * Deletes any backups for this state. Obviously, be very careful using this.
     */
    public abstract void deleteBackups();

    /**
     * Flush all pending puts for all key spaces
     */
    public abstract void flush();

    /**
     * Flush all pending puts for the given key space
     * @param keySpace - The key space to flush
     */
    public abstract void flush(String keySpace);

    /**
     * Get the value for the given key from the given key space.
     * @param keySpace - The key space where the value is stored
     * @param key - The key of the value to get
     * @return The value for the given key in the given key space
     */
    public abstract byte[] get(String keySpace, byte[] key);

    /**
     * Get an iterator for all keys in the given key space
     * @param keySpace - The key space to iterate over
     * @return An iterator for all keys in the key space
     */
    public abstract Iterator iterate(String keySpace);

    /**
     * Checks if the state is open.
     * @return True if the state is open; False if the state is closed.
     */
    public boolean isOpen() {
        return this.isOpen;
    }

    /**
     * Open the state. Should be called after instantiation, but before using the state.
     */
    public void open() {
        this.isOpen = true;
    };

    /**
     * Put a new value into the state for the given key and key space. Puts may be batched and the values may not be
     * available until flush() is called.
     * @param keySpace - The key space to store the key and value in
     * @param key - The key to store
     * @param value - The value to store
     */
    public abstract void put(String keySpace, byte[] key, byte[] value);

    /**
     * Restore the state from a previous backup.
     */
    public abstract void restore();
}
