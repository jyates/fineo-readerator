package io.fineo.read.drill.exec.store.schema;

import com.google.common.collect.ImmutableList;
import io.fineo.read.drill.exec.store.FineoStoragePlugin;
import io.fineo.schema.store.SchemaStore;
import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Linq4j;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
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
import org.apache.calcite.tools.RelBuilder;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.StoragePlugin;

import java.util.ArrayList;
import java.util.List;

import static org.apache.drill.exec.planner.logical.DrillRel.DRILL_LOGICAL;

/**
 * Base access for a logical Fineo table. This actually delegates to a series of unions to
 * underlying dynamo and/or spark tables, depending on the time range we are querying
 */
public class FineoTable extends DynamicDrillTable implements TranslatableTable {

  //  implements TranslatableTable,ExtensibleTable {
  private final SchemaStore schema;
  private final Schema dynamoSchemaImpl;
  private List<RelDataTypeField> extensionFields = new ArrayList<>();

  public FineoTable(FineoStoragePlugin plugin,
    String storageEngineName, String userName, Object selection, SchemaStore store) {
    super(plugin, storageEngineName, userName, selection);
    this.schema = store;
    dynamoSchemaImpl = null;
  }

//  public FineoTable(SchemaPlus calciteSchema, SchemaStore schema, Schema dynamo) {
//    this.schema = schema;
//    this.calciteSchema = calciteSchema;
//    this.dynamoSchemaImpl = dynamo;
//  }

//  @Override
//  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
//    // it could be anything!
//    if(this.type == null) {
//      this.type = new DynamicRecordTypeImpl(typeFactory);
//    }
//    return this.type;
//
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
//  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
//    RelBuilder builder = RelBuilder.proto(context.getCluster().getPlanner().getContext())
//                                   .create(context.getCluster());
    RelOptTable table = relOptTable.getRelOptSchema().getTableForMember(ImmutableList.of("dfs",
      "/var/folders/43/tsp1ph8n5b96whkk0j_bkl540000gn/T/junit4249094017616689616"
      + "/drill/test.json"));
    RelOptCluster cluster = context.getCluster();
    return new LogicalTableScan(cluster, cluster.traitSetOf(Convention.NONE), table);
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
}
