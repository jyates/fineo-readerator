package io.fineo.read.calcite;

import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.DynamicRecordTypeImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.ArrayList;
import java.util.List;

/**
 * Base access for a logical Fineo table. This actually delegates to a series of unions to
 * underlying dynamo and/or spark tables, depending on the time range we are querying
 */
public class FineoTable extends AbstractTable implements ScannableTable, TranslatableTable {

//  implements TranslatableTable,ExtensibleTable {
  private final SchemaStore schema;
  private final SchemaPlus calciteSchema;
  private final Schema dynamoSchemaImpl;
  private List<RelDataTypeField> extensionFields = new ArrayList<>();
  private RelDataType type;

  public FineoTable(SchemaPlus calciteSchema, SchemaStore schema, Schema dynamo) {
    this.schema = schema;
    this.calciteSchema = calciteSchema;
    this.dynamoSchemaImpl = dynamo;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    // it could be anything!
    if(this.type == null) {
      this.type = new DynamicRecordTypeImpl(typeFactory);
    }
    return this.type;

//    RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
//    // add the default fields that we expect from all users
//    builder.add(ORG_ID_KEY.toUpperCase(), typeFactory.createJavaType(String.class));
//    builder.add(TIMESTAMP_KEY.toUpperCase(), typeFactory.createJavaType(Primitive.LONG.boxClass));
//    builder.add(ORG_METRIC_TYPE_KEY.toUpperCase(), typeFactory.createJavaType(String.class));
//
//    // add the extension fields
//    for (RelDataTypeField field : extensionFields) {
//      builder.add(field);
//    }
//
//    return builder.build();
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    this.type = new RelRecordType(this.type.getFieldList());
    return new LogicalTableScan(context.getCluster(), context.getCluster().traitSet(), relOptTable);
  }
//    // ideally we would just register the expansion rules now, but the planner you get here is
//    // not the same planner that executes these rels, so we have to create a RelNode to register
//    // the rules, which then gets converted into the larger underlying scan
//    final List<RelOptRule> rules = newArrayList(
//      new FineoMultiProjectRule(schema),
//      new FineoMultiScanRule(calciteSchema));
//    final RelOptCluster cluster = context.getCluster();
//    return new FineoScan(cluster, cluster.traitSetOf(FineoRel.CONVENTION), relOptTable, rules,
//      dynamoSchemaImpl);
//
////    SchemaPlus dynamoSchema = calciteSchema.getSubSchema(FineoSchemaFactory.DYNAMO_SCHEMA_NAME);
////    FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(dynamoSchema).build();
////    Collection<String> names = dynamoSchemaImpl.getTableNames();
////    RelBuilder builder = RelBuilder.create(config);
////    for (String name : names) {
////      builder.scan(name);
////    }
////
////    for (int i = 0; i < names.size() - 1; i++) {
////      builder.join(JoinRelType.FULL);
////    }
////
////    return builder.build();
//  }

//  @Override
  public Table extend(List<RelDataTypeField> fields) {
    FineoTable table = new FineoTable(this.calciteSchema, this.schema, dynamoSchemaImpl);
    table.extensionFields.addAll(this.extensionFields);
    table.extensionFields.addAll(fields);
    return table;
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root) {
     return Linq4j.asEnumerable(new ArrayList<>());
  }
}
