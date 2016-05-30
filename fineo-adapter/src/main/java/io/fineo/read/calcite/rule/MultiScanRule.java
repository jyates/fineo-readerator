package io.fineo.read.calcite.rule;

import io.fineo.read.calcite.FineoRel;
import io.fineo.read.calcite.rel.FineoRecombinatorRel;
import io.fineo.read.calcite.rel.FineoScan;
import io.fineo.read.calcite.FineoSchemaFactory;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.RelBuilder;

import java.util.Collection;

/**
 * Extend a {@link FineoScan} into a joined scan across sub-tables.
 */
public class MultiScanRule extends RelOptRule {

  private final SchemaPlus calciteSchema;

  public MultiScanRule(SchemaPlus calciteSchema) {
    super(operand(FineoScan.class, FineoRel.CONVENTION, RelOptRule.any()));
    this.calciteSchema = calciteSchema;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    FineoScan scan = call.rel(0);
    Schema dynamoSchemaImpl = scan.getDynamoSchema();
    Collection<String> names = dynamoSchemaImpl.getTableNames();
    RelBuilder builder = call.builder(scan.getTable().getRelOptSchema());
    for (String name : names) {
      builder.scan(FineoSchemaFactory.DYNAMO_SCHEMA_NAME, name);
    }

    for (int i = 0; i < names.size() - 1; i++) {
      builder.join(JoinRelType.FULL);
    }

//    RelNode subscans = builder.build();
    call.transformTo(builder.build());//new FineoRecombinatorRel(subscans, scan.getRowType()));
  }
}
