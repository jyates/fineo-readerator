package io.fineo.read.drill.exec.store.schema;

import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.store.AbstractSchema;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Manages the actual schema for Fineo tables
 */
public class FineoSchema extends FineoBaseSchema {
  private final FineoStoragePlugin plugin;
  private final FineoSubSchemas subSchemas;
  private final StoreClerk clerk;

  public FineoSchema(List<String> parentPath, String name, FineoStoragePlugin plugin,
    FineoSubSchemas subSchemas, SchemaStore store) {
    super(parentPath, name);
    this.plugin = plugin;
    this.subSchemas = subSchemas;
    this.clerk = new StoreClerk(store, name);
  }

  @Override
  public Set<String> getTableNames() {
    Set<String> names = new HashSet<>();
    for (StoreClerk.Metric metric : clerk.getMetrics()) {
      names.add(metric.getUserName());
    }
    return names;
  }

  @Override
  public Table getTable(String name) {
    try {
      StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(name);
      return new FineoTable(plugin, name, subSchemas, metric);
    } catch (SchemaNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
