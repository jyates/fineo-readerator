package io.fineo.read.drill.exec.store.rel.recombinator.physical.batch.impl;

import io.fineo.read.drill.exec.store.FineoCommon;
import io.fineo.schema.store.StoreClerk;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Manages the conversion of fields from upstream (e.g. actual drill tables) sources to the
 * actual field names.
 */
public class AliasFieldNameManager {

  private final Map<String, String> map;
  private final Pattern partitionPattern;
  private final boolean radio;

  public AliasFieldNameManager(StoreClerk.Metric metric, String partitionDesignator,
    boolean radio) {
    this.radio = radio;
    this.map = getFieldMap(metric);
    partitionPattern = Pattern.compile(String.format("%s[0-9]+", partitionDesignator));
  }

  private Map<String, String> getFieldMap(StoreClerk.Metric metric) {
    Map<String, String> map = new HashMap<>();
    // build list of fields that we need to add
    // required fields
    for (String field : FineoCommon.REQUIRED_FIELDS) {
      map.put(field, field);
    }

    if (radio) {
      map.put(FineoCommon.MAP_FIELD, FineoCommon.MAP_FIELD);
    }

    for (StoreClerk.Field field : metric.getUserVisibleFields()) {
      map.put(field.getName(), field.getName());
      for (String alias : field.getAliases()) {
        map.put(alias, field.getName());
      }
      // canonical name mapping
      map.put(field.getCname(), field.getName());
    }
    return map;
  }

  public String getOutputName(String upstreamFieldName) {
    return this.map.get(upstreamFieldName);
  }

  public boolean shouldSkip(String outputName, boolean dynamic) {
    // dynamic fields should come first
    if (dynamic) {
      // for fields we know about (have mappings) we will get them in the non-dynamic fields.
      if (radio) {
        // Otherwise, we should not skip it because its an unknown field and needs to go into _fm
        return !outputName.equals(FineoCommon.MAP_FIELD) && getOutputName(outputName) != null;
      }
      return true;
    }

    return isDirectory(outputName);
  }

  public boolean isDirectory(String outputName) {
    return partitionPattern.matcher(outputName).matches();
  }
}
