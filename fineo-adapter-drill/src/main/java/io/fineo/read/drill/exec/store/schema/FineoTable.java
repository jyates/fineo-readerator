package io.fineo.read.drill.exec.store.schema;

import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.schema.store.StoreClerk;
import org.apache.avro.Schema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.exec.planner.logical.DrillTable;

import java.util.function.Function;

import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

/**
 * Base access for a logical Fineo table. This actually delegates to a series of unions to
 * underlying dynamo and/or spark tables, depending on the time range we are querying
 */
public class FineoTable extends DrillTable implements TranslatableTable {

  private enum BaseField {
    TIMESTAMP("timestamp", tf -> tf.createSqlType(BIGINT)),
    RADIO("_fm", tf -> tf.createMapType(tf.createSqlType(VARCHAR), tf.createSqlType(ANY)));
    private final String name;
    private final Function<RelDataTypeFactory, RelDataType> func;

    BaseField(String name, Function<RelDataTypeFactory, RelDataType> func) {
      this.name = name;
      this.func = func;
    }

    public RelDataTypeFactory.FieldInfoBuilder add(RelDataTypeFactory.FieldInfoBuilder builder,
      RelDataTypeFactory factory) {
      return builder.add(name, func.apply(factory));
    }
  }

  private final FineoSubSchemas schemas;
  private final StoreClerk.Metric metric;

  public FineoTable(FineoStoragePlugin plugin, String storageEngineName,
    FineoSubSchemas schemas, StoreClerk.Metric metric) {
//    super(plugin, storageEngineName, null, null);
    super(storageEngineName, plugin, null, null);
    this.schemas = schemas;
    this.metric = metric;
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    LogicalScanBuilder builder = new LogicalScanBuilder(context, relOptTable)
      // placeholders for values specified on table creation
      .withOrgId(metric.getOrgId())
      .withMetricType(metric.getUserName());
    schemas.scan(builder);
//    return builder.getFirstScan();
    return builder.buildMarker(this.metric);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
    // base fields
    for (BaseField field : BaseField.values()) {
      field.add(builder, typeFactory);
    }

    // add all the user visible fields
    for (StoreClerk.Field field : this.metric.getUserVisibleFields()) {
      SqlTypeName type = getSqlType(field.getType());
      builder.add(field.getName(), type);
    }
    return builder.build();
  }

  private SqlTypeName getSqlType(Schema.Type type) {
    switch (type) {
      case STRING:
        return SqlTypeName.VARCHAR;
      case BOOLEAN:
        return SqlTypeName.BOOLEAN;
      case BYTES:
        return SqlTypeName.BINARY;
      case INT:
        return SqlTypeName.INTEGER;
      case LONG:
        return SqlTypeName.BIGINT;
      case FLOAT:
        return SqlTypeName.FLOAT;
      case DOUBLE:
        return SqlTypeName.DOUBLE;
      default:
        throw new IllegalArgumentException("We cannot type avro type: " + type);
    }
  }
}
