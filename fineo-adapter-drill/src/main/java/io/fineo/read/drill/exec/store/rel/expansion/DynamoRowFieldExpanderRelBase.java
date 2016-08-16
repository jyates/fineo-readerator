package io.fineo.read.drill.exec.store.rel.expansion;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.physical.PrelUtil;

/**
 * Base class for expanders. Mostly handles the costing evaluation.
 */
public abstract class DynamoRowFieldExpanderRelBase extends SingleRel {

  public static final int EXPECTED_ROWS_PER_TIMESTAMP = 10;

  public DynamoRowFieldExpanderRelBase(RelTraitSet traitSet, RelNode input) {
    super(input.getCluster(), traitSet, input);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    if (PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }
    // later we can inform this with statistics
    RelNode child = this.getInput();
    // basically we take the number of source rows and expand them by the average expected rows
    double inputRows = RelMetadataQuery.getRowCount(child) * EXPECTED_ROWS_PER_TIMESTAMP;
    // each column needs to be copied over into a new element for each new row, which is
    // basically a projection cost.. ish.
    double cpuCost = child.getRowType().getFieldCount() * EXPECTED_ROWS_PER_TIMESTAMP *
                     DrillCostBase.PROJECT_CPU_COST;
    DrillCostBase.DrillCostFactory costFactory =
      (DrillCostBase.DrillCostFactory) planner.getCostFactory();
    return costFactory.makeCost(inputRows, cpuCost, 0, 0);
  }
}
