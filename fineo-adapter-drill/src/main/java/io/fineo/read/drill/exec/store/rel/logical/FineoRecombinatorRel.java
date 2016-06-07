package io.fineo.read.drill.exec.store.rel.logical;

import io.fineo.internal.customer.Metric;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.planner.logical.DrillImplementor;
import org.apache.drill.exec.planner.logical.DrillRel;

import java.util.List;

/**
 * Logical conversion of fields from the sub-query into the fields that is actually visible in this
 * metric.
 */
public class FineoRecombinatorRel extends SingleRel implements DrillRel {

  private final Metric metric;

  /**
   * Creates a <code>SingleRel</code>.
   *
   * @param cluster Cluster this relational expression belongs to
   * @param traits
   * @param input   Input relational expression
   * @param metric
   * @param rowType
   */
  protected FineoRecombinatorRel(RelOptCluster cluster,
    RelTraitSet traits, RelNode input, Metric metric, RelDataType rowType) {
    super(cluster, traits, input);
    this.metric = metric;
    this.rowType = rowType;
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    final LogicalOperator input = implementor.visitChild(this, 0, getInput());
    FineoRecombinatorLogicalOperator op = new FineoRecombinatorLogicalOperator(metric);
    op.setInput(input);
    return op;
  }

  public Metric getMetric() {
    return this.metric;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FineoRecombinatorRel(this.getCluster(), traitSet, SingleRel.sole(inputs), metric,
      rowType);
  }
}
