package io.fineo.read.drill.exec.store.schema;

import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.exec.planner.logical.DrillTable;

/**
 * Base access for a logical Fineo table. This actually delegates to a series of unions to
 * underlying dynamo and/or spark tables, depending on the time range we are querying
 */
public class FineoTable extends DrillTable implements TranslatableTable {

  private final FineoSubSchemas schemas;
  private final SchemaStore store;

  public FineoTable(FineoStoragePlugin plugin, String storageEngineName, String userName,
    Object selection, FineoSubSchemas schemas, SchemaStore store) {
    super(storageEngineName, plugin, userName, selection);
    this.schemas = schemas;
    this.store = store;
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    LogicalScanBuilder builder = new LogicalScanBuilder(context, relOptTable)
      // placeholders for values specified on table creation
      .withOrgId("orgid1")
      .withMetricType("metricid1");
    schemas.scan(builder);
    return builder.buildMarker(this.store);
//    return builder.getFirstScan();
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    //TODO replace this with the real row type from the schema store
    return typeFactory.builder()
                      .add("timestamp", SqlTypeName.BIGINT)
                      .add("field1", SqlTypeName.BOOLEAN)
                      .add("_fm", SqlTypeName.MAP)
                      .build();
  }

  @FunctionalInterface
  public interface FieldVerifier {
    void verify(RelDataTypeField field, String expectedName);
  }
}
