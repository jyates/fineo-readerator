package io.fineo.read.drill.exec.store.schema;

import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePluginConfig;
import io.fineo.read.drill.exec.store.rel.FineoRecombinatorMarkerRel;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;

import java.util.List;
import java.util.Map;

/**
 * Base access for a logical Fineo table. This actually delegates to a series of unions to
 * underlying dynamo and/or spark tables, depending on the time range we are querying
 */
public class FineoTable extends DynamicDrillTable implements TranslatableTable {

  private final FineoSubSchemas schemas;
  private final SchemaStore store;

  public FineoTable(FineoStoragePlugin plugin, String storageEngineName, String userName,
    Object selection, FineoSubSchemas schemas, SchemaStore store) {
    super(plugin, storageEngineName, userName, selection);
    this.schemas = schemas;
    this.store = store;
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    LogicalScanBuilder builder = new LogicalScanBuilder(context, relOptTable);
    schemas.scan(builder);
    return builder.buildMarker(this.store);
  }
}
