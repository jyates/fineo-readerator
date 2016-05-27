package io.fineo.read.calcite;

import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ExtensibleTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RuleSets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_ID_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_METRIC_TYPE_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.TIMESTAMP_KEY;

/**
 * Base access for a logical Fineo table. This actually delegates to a series of unions to
 * underlying dynamo and/or spark tables, depending on the time range we are querying
 */
public class FineoTable extends AbstractTable implements TranslatableTable,
                                                         ExtensibleTable {
  private final SchemaStore schema;
  private final SchemaPlus calciteSchema;
  private List<RelDataTypeField> extensionFields = new ArrayList<>();

  public FineoTable(SchemaPlus calciteSchema, SchemaStore schema) {
    this.schema = schema;
    this.calciteSchema = calciteSchema;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();

    // add the default fields that we expect from all users
    builder.add(ORG_ID_KEY.toUpperCase(), typeFactory.createSqlType(SqlTypeName.VARCHAR));
    builder.add(ORG_METRIC_TYPE_KEY.toUpperCase(), typeFactory.createSqlType(SqlTypeName.VARCHAR));
    builder.add(TIMESTAMP_KEY.toUpperCase(), typeFactory.createSqlType(SqlTypeName.TIMESTAMP));

    // add the extension fields
    for (RelDataTypeField field : extensionFields) {
      builder.add(field);
    }
    return builder.build();
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    SchemaPlus dynamo = calciteSchema.getSubSchema(FineoSchemaFactory.DYNAMO_SCHEMA_NAME);
    FrameworkConfig config =
      Frameworks.newConfigBuilder()
                .defaultSchema(dynamo)
                .ruleSets(RuleSets.ofList(new MultiProjectRule(schema))).build();
    Collection<String> names = dynamo.getTableNames();
    RelBuilder builder = RelBuilder.create(config);
    for (String name : names) {
      builder.scan(name);
    }

    for (int i = 0; i < names.size() - 1; i++) {
      builder.join(JoinRelType.FULL);
    }

    return builder.build();
  }

  @Override
  public Table extend(List<RelDataTypeField> fields) {
    FineoTable table = new FineoTable(this.calciteSchema, this.schema);
    table.extensionFields.addAll(this.extensionFields);
    table.extensionFields.addAll(fields);
    return table;
  }
}
