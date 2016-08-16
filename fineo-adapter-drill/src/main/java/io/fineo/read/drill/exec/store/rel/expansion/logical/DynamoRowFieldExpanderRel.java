package io.fineo.read.drill.exec.store.rel.expansion.logical;

import io.fineo.read.drill.exec.store.rel.expansion.DynamoRowFieldExpanderRelBase;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.planner.logical.DrillImplementor;
import org.apache.drill.exec.planner.logical.DrillRel;

import java.util.List;

public class DynamoRowFieldExpanderRel extends DynamoRowFieldExpanderRelBase implements DrillRel {
  public DynamoRowFieldExpanderRel(RelTraitSet traitSet, RelNode input) {
    super(traitSet, input);
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    return new DynamoRowFieldExpanderLogicalOperator();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DynamoRowFieldExpanderRel(traitSet, SingleRel.sole(inputs));
  }
}
