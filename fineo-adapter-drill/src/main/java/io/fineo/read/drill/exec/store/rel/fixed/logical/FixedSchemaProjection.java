package io.fineo.read.drill.exec.store.rel.fixed.logical;

import io.fineo.read.drill.exec.store.rel.fixed.physical.FixedSchemaOperator;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.planner.logical.DrillImplementor;
import org.apache.drill.exec.planner.logical.DrillRel;

import java.util.ArrayList;
import java.util.List;

import static io.fineo.read.drill.exec.store.rel.fixed.logical.FixedCommon.getProjectExpressions;

public class FixedSchemaProjection extends SingleRel implements DrillRel {
  private final List<RexNode> exps;

  public FixedSchemaProjection(RelOptCluster cluster,
    RelTraitSet traits, RelNode child,
    List<RexNode> exps,
    RelDataType rowType) {
    super(cluster, traits, child);
    this.rowType = rowType;
    this.exps = exps;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FixedSchemaProjection(this.getCluster(), traitSet, inputs.get(0), getProjections(),
      getRowType());
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    LogicalOperator inputOp = implementor.visitChild(this, 0, getInput());
    List<NamedExpression> exprs = new ArrayList<>();
    exprs.addAll(getProjectExpressions(implementor.getContext(), getProjections(),
      this, getRowType()));
    FixedSchemaOperator op = new FixedSchemaOperator(exprs);
    op.setInput(inputOp);
    return op;
  }

  public List<RexNode> getProjections() {
    return exps;
  }
}
