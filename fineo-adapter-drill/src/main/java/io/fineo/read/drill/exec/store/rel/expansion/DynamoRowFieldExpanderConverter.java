package io.fineo.read.drill.exec.store.rel.expansion;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.drill.exec.planner.logical.DrillScanRel;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.drill.exec.planner.logical.DrillRel.DRILL_LOGICAL;

public class DynamoRowFieldExpanderConverter extends RelOptRule {

  public static final RelOptRule INSTANCE = new DynamoRowFieldExpanderConverter();

  private DynamoRowFieldExpanderConverter() {
    super(operand(DynamoRowFieldExpanderRel.class, operand(LogicalTableScan.class, none())),
      "Fineo::DynamoRowFieldExpanderConversion");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    DynamoRowFieldExpanderRel rel = call.rel(0);
    LogicalTableScan scan = call.rel(1);
    DrillScanRel dsr = new DrillScanRel(scan.getCluster(), scan.getTraitSet().plus(DRILL_LOGICAL)
      , scan.getTable());
    call.transformTo(rel.copy(rel.getTraitSet().plus(DRILL_LOGICAL), of(dsr)));
  }
}
