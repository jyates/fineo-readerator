package io.fineo.read.drill.exec.store.schema;

import com.google.common.collect.ImmutableList;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.store.AbstractSchema;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;

/**
 * Manages the actual schema for Fineo tables
 */
public class FineoSchema extends AbstractSchema {
  private static final String EVENT_TABLE_NAME = "events";
  private final FineoStoragePlugin plugin;
  private final FineoSubSchemas subSchemas;
  private SchemaStore store;

  public FineoSchema(List<String> parentPath, String name, FineoStoragePlugin plugin,
    FineoSubSchemas subSchemas,
    SchemaStore store) {
    super(parentPath, name);
    this.plugin = plugin;
    this.subSchemas = subSchemas;
    this.store = store;
  }

  public FineoSchema(String schemaName, FineoStoragePlugin plugin, FineoSubSchemas subSchemas,
    SchemaStore store) {
    this(ImmutableList.of(), schemaName, plugin, subSchemas, store);
  }

  @Override
  public String getTypeName() {
    return "FINEO";
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public Set<String> getTableNames() {
    return newHashSet(EVENT_TABLE_NAME, EVENT_TABLE_NAME+2);
  }

  @Override
  public Table getTable(String name) {
//    assert name.equals(EVENT_TABLE_NAME);
    return new FineoTable(plugin, name, null, null, subSchemas, this.store);
  }
}
