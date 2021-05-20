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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.jwplayer.southpaw.record.BaseRecord;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * Base class for filtering input records. These records are not recorded in the state and are not included
 * in any denormalized records. They are effectively treated as a tombstone.
 */
public class BaseFilter {
  protected class Metrics {
    public static final String FILTER_DELETES = "filter.deletes";
    public static final String FILTER_SKIPS = "filter.skips";
    public static final String FILTER_UPDATES = "filter.updates";

    /**
     * Registry where metrics are registered
     */
    protected final MetricRegistry registry = new MetricRegistry();
    /**
     * Send the metrics to JMX.
     */
    protected final JmxReporter reporter =
        JmxReporter.forRegistry(registry).inDomain(com.jwplayer.southpaw.metric.Metrics.PREFIX).build();

    public Metrics() {
      reporter.start();
    }

    public Meter getMeter(String entity, FilterMode mode) {
      String meterName;

      switch (mode) {
        case DELETE:
          meterName = String.join(".", FILTER_DELETES, entity);
          break;
        case SKIP:
          meterName = String.join(".", FILTER_SKIPS, entity);
          break;
        case UPDATE:
          meterName = String.join(".", FILTER_UPDATES, entity);
          break;
        default:
          throw new RuntimeException(String.format("Mode %s not found!", mode.toString()));
      }

      if (!registry.getMetrics().containsKey(meterName)) {
        return registry.meter(meterName);
      } else {
        return (Meter) registry.getMetrics().get(meterName);
      }
    }
  }

  protected Metrics metrics = new Metrics();

  public BaseFilter() {
  }

  /**
   * Used to inform how a record should be handled:
   * <p>
   * UPDATE: Do not filter (no op) by advancing offset and updating state, output record produced.
   * SKIP: Skip record and advance offset, don't update state, output record not produced.
   * DELETE: Delete record by advancing offset and updating state, output record produced.
   */
  public enum FilterMode {UPDATE, SKIP, DELETE}

  /**
   * Configure this filter using the global configuration
   *
   * @param config - The global config
   */
  public void configure(Map<String, Object> config) {
    // Do nothing by default
  }

  /**
   * Default 'custom' filter
   *
   * @param entity    - The entity of the given record
   * @param record    - The record to filter
   * @param oldRecord - The previously seen record state (may be null)
   * @return FilterMode - Describes how to handle the input record
   */
  protected FilterMode customFilter(String entity, BaseRecord record, BaseRecord oldRecord) {
    return FilterMode.UPDATE;
  }

  /**
   * Useful function for custom filters to test equality between two records
   *
   * @param record        - The record to filter
   * @param oldRecord     - The previously seen record state (may be null)
   * @param ignoredFields - Fields to ignore when testing if two records are equal
   * @return Returns true if the two records are equal (ignoring the specified fields), otherwise false
   */
  protected boolean isEqual(BaseRecord record, BaseRecord oldRecord, List<String> ignoredFields) {
    if ((record == null && oldRecord != null) || (record != null && oldRecord == null)) {
      return false;
    } else if (record != null) {
      Map<String, ?> newMap = record.toMap();
      Map<String, ?> oldMap = oldRecord.toMap();

      if (newMap.size() != oldMap.size()) {
        return false;
      } else {
        for (Map.Entry<String, ?> entry : newMap.entrySet()) {
          if (!ignoredFields.contains(entry.getKey()) && oldMap.containsKey(entry.getKey())) {
            if (!Objects.equals(entry.getValue(), oldMap.get(entry.getKey()))) {
              return false;
            }
          }
        }
      }
    }

    return true;
  }

  /**
   * Determines if the given record should be filtered based on its entity and previous entity state.
   *
   * @param entity    - The entity of the given record
   * @param record    - The record to filter
   * @param oldRecord - The previously seen record state (may be null)
   * @return FilterMode - Describes how to handle the input record
   */
  public FilterMode filter(String entity, BaseRecord record, BaseRecord oldRecord) {
    FilterMode mode;
    if (record == null || record.isEmpty()) {
      mode = FilterMode.DELETE;
    } else {
      mode = customFilter(entity, record, oldRecord);
    }

    metrics.getMeter(entity, mode).mark(1);

    return mode;
  }
}
