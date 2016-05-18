package io.fineo.read.calcite;

import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * Manages the actual schema for Fineo tables
 */
public class FineoSchema extends AbstractSchema {
  private static final String EVENT_TABLE_NAME = "events";
  private final SchemaStore schema;

  public FineoSchema(SchemaStore store) {
    this.schema = store;
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  protected Map<String, Table> getTableMap() {
    HashMap<String, Table> tables = new HashMap<>();
    tables.put(EVENT_TABLE_NAME, new FineoTable(schema));
    return tables;
  }
}
