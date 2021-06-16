package com.jwplayer.southpaw.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.record.MapRecord;
import java.util.Collections;
import org.junit.Test;


public class BaseFilterTest {
  @Test
  public void testEqualRecords() {
    BaseFilter filter = new BaseFilter();
    BaseRecord record1 = new MapRecord(ImmutableMap.of("A", 1, "B", 2, "C", 3));
    BaseRecord record2 = new MapRecord(ImmutableMap.of("A", 1, "B", 2, "C", 3));
    Boolean isEqual = filter.isEqual(record1, record2, Collections.emptyList());

    assertTrue(isEqual);
  }

  @Test
  public void testEqualRecordsWithIgnoredFields() {
    BaseFilter filter = new BaseFilter();
    BaseRecord record1 = new MapRecord(ImmutableMap.of("A", 1, "B", 2, "C", 3));
    BaseRecord record2 = new MapRecord(ImmutableMap.of("A", 1, "B", 5, "C", 3));
    Boolean isEqual = filter.isEqual(record1, record2, ImmutableList.of("B"));

    assertTrue(isEqual);
  }

  @Test
  public void testFilterWithEmptyRecord() {
    BaseFilter filter = new BaseFilter();
    BaseRecord record = new MapRecord(Collections.emptyMap());
    BaseFilter.FilterMode mode = filter.filter(null, record, null);

    assertEquals(BaseFilter.FilterMode.DELETE, mode);
  }

  @Test
  public void testFilterWithNullRecord() {
    BaseFilter filter = new BaseFilter();
    BaseRecord record = new MapRecord(null);
    BaseFilter.FilterMode mode = filter.filter(null, record, null);

    assertEquals(BaseFilter.FilterMode.DELETE, mode);
  }

  @Test
  public void testFilterWithSimpleRecord() {
    BaseFilter filter = new BaseFilter();
    BaseRecord record = new MapRecord(ImmutableMap.of("A", 1));
    BaseFilter.FilterMode mode = filter.filter(null, record, null);

    assertEquals(BaseFilter.FilterMode.UPDATE, mode);
  }

  @Test
  public void testUnequalRecords() {
    BaseFilter filter = new BaseFilter();
    BaseRecord record1 = new MapRecord(ImmutableMap.of("A", 1, "B", 2, "C", 3));
    BaseRecord record2 = new MapRecord(ImmutableMap.of("A", 1, "B", 5, "C", 3));
    Boolean isEqual = filter.isEqual(record1, record2, Collections.emptyList());

    assertFalse(isEqual);
  }
}
