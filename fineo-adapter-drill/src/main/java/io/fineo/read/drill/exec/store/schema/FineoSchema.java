package io.fineo.read.drill.exec.store.schema;

import com.google.common.base.Stopwatch;
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
import java.util.concurrent.TimeUnit;

/**
 * Manages the actual schema for Fineo tables
 */
public class FineoSchema extends FineoBaseSchema {
  private static final Logger LOG = LoggerFactory.getLogger(FineoSchema.class);

  private final FineoStoragePlugin plugin;
  private final SubTableScanBuilder scanner;
  private final SchemaStore store;
  private final String org;

  public FineoSchema(List<String> parentPath, String orgAsSchemaName, FineoStoragePlugin plugin,
    SubTableScanBuilder subSchemas, SchemaStore store) {
    super(parentPath, orgAsSchemaName);
    this.plugin = plugin;
    this.scanner = subSchemas;
    this.store = store;
    this.org = orgAsSchemaName;
  }

  @Override
  public Set<String> getTableNames() {
    Stopwatch timer = Stopwatch.createStarted();
    Set<String> names = new HashSet<>();
    for (StoreClerk.Metric metric : getClerk().getMetrics()) {
      names.add(metric.getUserName());
    }
    LOG.debug("({}ms) Got table names {}: {}", timer.elapsed(TimeUnit.MILLISECONDS), this.getName(),
      names);
    return names;
  }

  @Override
  public Table getTable(String tableName) {
    Stopwatch timer = Stopwatch.createStarted();
    try {
      StoreClerk.Metric metric = getClerk().getMetricForUserNameOrAlias(tableName);
      return new FineoTable(plugin, tableName, scanner, metric);
    } catch (SchemaNotFoundException e) {
      throw new IllegalArgumentException(e);
    } finally {
      LOG.debug("({}ms) finished getting table: {}", tableName);
    }
  }

  /**
   * A new clerk. This forces the clerk to re-lookup the org metadata, which is necessary because
   * the tables may have changed for the tenant
   */
  private StoreClerk getClerk() {
    return new StoreClerk(store, org);
  }
}
