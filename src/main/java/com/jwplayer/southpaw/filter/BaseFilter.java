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
package com.jwplayer.southpaw.filter;

import com.jwplayer.southpaw.record.BaseRecord;

import java.util.Map;


/**
 * Base class for filtering input records. These records are not recorded in the state and are not included
 * in any denormalized records. They are effectively treated as a tombstone.
 */
public abstract class BaseFilter {
    /**
     * Configure this filter using the global configuration
     * @param config - The global config
     */
    public void configure(Map<String, Object> config) {
        // Do nothing by default
    }

    /**
     * Determines if the given record should be filtered based on its entity
     * @param entity - The entity of the given record
     * @param record - The record to filter
     * @return True if the record should be filtered, else false
     */
    public abstract boolean isFiltered(String entity, BaseRecord record);
}
