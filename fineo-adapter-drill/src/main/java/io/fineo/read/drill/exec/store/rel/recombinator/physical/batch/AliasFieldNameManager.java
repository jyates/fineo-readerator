package io.fineo.read.drill.exec.store.rel.recombinator.physical.batch;

import io.fineo.read.drill.exec.store.FineoCommon;
import io.fineo.schema.store.StoreClerk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Manages the conversion of fields from upstream (e.g. actual drill tables) sources to the
 * actual field names.
 */
public class AliasFieldNameManager {

  private final Map<String, String> map;
  private final List<String> directories = new ArrayList<>();
  private final Pattern partitionPattern;
  private boolean seenField = false;

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
    return map;
  }

  public String getOutputName(String upstreamFieldName) {
    return this.map.get(upstreamFieldName);
  }

  public boolean shouldSkip(String outputName, boolean dynamic) {
    // dynamic fields should come first
    if (dynamic) {
      // counts on directories showing up before actual fields in the * lookup
      if (!seenField && partitionPattern.matcher(outputName).matches()) {
        // there is an unknown dynamic field named dir_N_
        if (directories.contains(outputName)) {
          seenField = true;
          return false;
        }
        directories.add(outputName);
        return true;
      }
      seenField = true;
      return !outputName.equals(FineoCommon.MAP_FIELD) && getOutputName(outputName) != null;
    }

    seenField = true;
    return isDirectory(outputName);
  }

  public void reset() {
    seenField = false;
    directories.clear();
  }

  public boolean isDirectory(String outputName) {
    return directories.remove(outputName);
  }
}
