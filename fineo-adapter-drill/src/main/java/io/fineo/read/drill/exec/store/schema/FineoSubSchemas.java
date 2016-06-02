package io.fineo.read.drill.exec.store.schema;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Map;

public class FineoSubSchemas {

  private Multimap<String, String> schemaAndTables = ArrayListMultimap.create();

  public FineoSubSchemas(Map<String, List<String>> schemaToTableMap) {
    for (String schema : schemaToTableMap.keySet()) {
      this.schemaAndTables.putAll(schema, schemaToTableMap.get(schema));
    }
  }

  public void scan(LogicalScanBuilder builder) {
    for (String schema : schemaAndTables.keySet()) {
      for (String table : schemaAndTables.get(schema)) {
        builder.scan(schema, table);
      }
    }
  }
}
