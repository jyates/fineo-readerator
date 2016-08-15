package io.fineo.read.drill.exec.store.rel.expansion;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.planner.logical.DrillImplementor;
import org.apache.drill.exec.planner.logical.DrillRel;

public class DynamoRowFieldExpanderRel extends SingleRel implements DrillRel {
  public DynamoRowFieldExpanderRel(RelNode input) {
    super(input.getCluster(), input.getTraitSet(), input);
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    return new DynamoRowFieldExpanderLogicalOperator();
  }
}
