package io.fineo.read.drill.exec.store.schema;

import io.fineo.read.drill.exec.store.FineoCommon;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;

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
    addBaseFields(relOptTable.getRowType());

    LogicalScanBuilder builder = new LogicalScanBuilder(context, relOptTable);
    schemas.scan(builder);
//    return builder.buildMarker(this.store);
    return builder.getFirstScan();
  }

  /**
   * Add the expected columns to the type - required fields, unknown map. This only works in
   * cases where the type is dynamic
   */
  public static void addBaseFields(RelDataType type) {
    addBaseFields(type, (field, expectedName) -> {
    });
  }

  public static void addBaseFields(RelDataType type, FieldVerifier verifier) {
    for (String field : FineoCommon.REQUIRED_FIELDS) {
      verifier.verify(type.getField(field, false, false), field);
    }
    verifier.verify(type.getField(FineoCommon.MAP_FIELD, false, false), FineoCommon.MAP_FIELD);
  }

  @FunctionalInterface
  public interface FieldVerifier {
    void verify(RelDataTypeField field, String expectedName);
  }
}
