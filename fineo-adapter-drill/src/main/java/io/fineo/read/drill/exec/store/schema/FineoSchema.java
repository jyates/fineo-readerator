package io.fineo.read.drill.exec.store.schema;

import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * Manages the actual schema for Fineo tables
 */
public class FineoSchema extends AbstractSchema {
  private static final String EVENT_TABLE_NAME = "events";
  private final FineoStoragePlugin plugin;
  private final String name;
  private final FineoSubSchemas subSchemas;
  private SchemaStore store;

  public FineoSchema(String schemaName, FineoStoragePlugin plugin, FineoSubSchemas subSchemas,
    SchemaStore store) {
    this.name = schemaName;
    this.plugin = plugin;
    this.subSchemas = subSchemas;
    this.store = store;
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  protected Map<String, Table> getTableMap() {
    HashMap<String, Table> tables = new HashMap<>();
    tables.put(EVENT_TABLE_NAME, new FineoTable(plugin, name, null, null, subSchemas, this.store));
    return tables;
  }
}
