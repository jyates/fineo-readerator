package io.fineo.read.calcite;

import com.google.common.base.Strings;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ExtensibleTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

import static io.fineo.schema.avro.AvroSchemaEncoder.*;

/**
 * Base access for a logical Fineo table. This actually delegates to a series of unions to
 * underlying dynamo and/or spark tables, depending on the time range we are querying
 */
public class FineoTable extends AbstractTable implements TranslatableTable,
                                                         ExtensibleTable {
  private final SchemaStore schema;
  private List<RelDataTypeField> extensionFields = new ArrayList<>();

  public FineoTable(SchemaStore schema) {
    this.schema = schema;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();

    // add the default extensionFields that we expect from all users
    builder.add(ORG_ID_KEY.toUpperCase(), typeFactory.createSqlType(SqlTypeName.VARCHAR));
    builder.add(ORG_METRIC_TYPE_KEY.toUpperCase(), typeFactory.createSqlType(SqlTypeName.VARCHAR));
    builder.add(TIMESTAMP_KEY.toUpperCase(), typeFactory.createSqlType(SqlTypeName.TIMESTAMP));

    // add the extension extensionFields
    for (RelDataTypeField field : extensionFields) {
      builder.add(field);
    }
    return builder.build();
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    List<RelNode> subqueries = new ArrayList<>();
    // map the query to the actual underlying tables
    return new LogicalUnion(context.getCluster(), context.getCluster().traitSet(), subqueries,
      true);
  }

  @Override
  public Table extend(List<RelDataTypeField> fields) {
    FineoTable table = new FineoTable(this.schema);
    table.extensionFields.addAll(this.extensionFields);
    table.extensionFields.addAll(fields);
    return table;
  }
}
