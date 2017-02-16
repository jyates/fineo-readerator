package io.fineo.read.drill.exec.store.schema;

import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import org.apache.calcite.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Manages the actual schema for Fineo tables
 */
public class FineoSchema extends FineoBaseSchema {
  private static final Logger LOG = LoggerFactory.getLogger(FineoSchema.class);

  private final FineoStoragePlugin plugin;
  private final SubTableScanBuilder scanner;
  private final StoreClerk clerk;

  public FineoSchema(List<String> parentPath, String orgAsSchemaName, FineoStoragePlugin plugin,
    SubTableScanBuilder subSchemas, SchemaStore store) {
    super(parentPath, orgAsSchemaName);
    this.plugin = plugin;
    this.scanner = subSchemas;
    this.clerk = new StoreClerk(store, orgAsSchemaName);
  }

  @Override
  public Set<String> getTableNames() {
    Set<String> names = new HashSet<>();
    for (StoreClerk.Metric metric : clerk.getMetrics()) {
      names.add(metric.getUserName());
    }
    LOG.debug("Got table names {}: {}", this.getName(), names);
    return names;
  }

  @Override
  public Table getTable(String tableName) {
    try {
      StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(tableName);
      return new FineoTable(plugin, tableName, scanner, metric);
    } catch (SchemaNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
