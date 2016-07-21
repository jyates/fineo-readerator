package io.fineo.read.drill.exec.store.rel.recombinator.physical.batch;

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

  public AliasFieldNameManager(StoreClerk.Metric clerk, String partitionDesignator) {
    this.map = getFieldMap(clerk);
    partitionPattern = Pattern.compile(String.format("%s[0-9]+", partitionDesignator));
  }

  private Map<String, String> getFieldMap(StoreClerk.Metric metric) {
    Map<String, String> map = new HashMap<>();
    // build list of fields that we need to add
    // required fields
    for (String field : FineoCommon.REQUIRED_FIELDS) {
      map.put(field, field);
    }

    map.put(FineoCommon.MAP_FIELD, FineoCommon.MAP_FIELD);

    for (StoreClerk.Field field : metric.getUserVisibleFields()) {
      map.put(field.getName(), field.getName());
      for (String alias : field.getAliases()) {
        map.put(alias, field.getName());
      }
    }

    // directory names
    for (int i = 0; i < 20; i++) {
      String name = "dir" + i;
      map.put(name, name);
    }
    return map;
  }

  public String getOutputName(String upstreamFieldName) {
    return this.map.get(upstreamFieldName);
  }

  public boolean shouldSkip(String outputName, boolean dynamic) {
    // dynamic fields should come first
    if (dynamic) {
      return !outputName.equals(FineoCommon.MAP_FIELD) && getOutputName(outputName) != null;
    }
    return isDirectory(outputName);
  }

  public boolean isDirectory(String outputName) {
    return partitionPattern.matcher(outputName).matches();
  }
}
