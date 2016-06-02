package io.fineo.read.drill.exec.store.schema;

import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.Map;

import static io.fineo.read.drill.exec.store.schema.FineoSchemaFactory.DYNAMO_SCHEMA_NAME;

/**
 * Manages the actual schema for Fineo tables
 */
public class FineoSchema extends AbstractSchema {
  private static final String EVENT_TABLE_NAME = "events";
  private final SchemaStore schema;
  private final SchemaPlus calciteSchema;
  private final Schema dynamoSchema;
  private final FineoStoragePlugin plugin;
  private final String name;

  public FineoSchema(SchemaPlus parentSchema, String schemaName, FineoStoragePlugin plugin, SchemaStore store,
    Schema dynamoSchema) {
    this.calciteSchema = parentSchema;
    this.name = schemaName;
    this.plugin = plugin;
    this.schema = store;
    this.dynamoSchema = dynamoSchema;
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  protected Map<String, Table> getTableMap() {
    HashMap<String, Table> tables = new HashMap<>();
    tables.put(EVENT_TABLE_NAME, new FineoTable(plugin, name, null, null, schema));
    return tables;
  }

  @Override
  protected Map<String, Schema> getSubSchemaMap() {
    Map<String, Schema> subschemas = new HashMap<>(1);
    subschemas.put(DYNAMO_SCHEMA_NAME, calciteSchema.getSubSchema(DYNAMO_SCHEMA_NAME));
    return subschemas;
  }
}