package io.fineo.read.drill.exec.store.rel.recombinator.logical;

import io.fineo.internal.customer.Metric;
import io.fineo.schema.store.StoreClerk;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.planner.logical.DrillImplementor;
import org.apache.drill.exec.planner.logical.DrillRel;

import java.util.List;

/**
 * Logical conversion of fields from the sub-query into the fields that is actually visible in this
 * metric.
 */
public class FineoRecombinatorRel extends SingleRel implements DrillRel {

  private final StoreClerk.Metric metric;

  /**
   * Creates a <code>SingleRel</code>.
   *
   * @param cluster Cluster this relational expression belongs to
   * @param traits
   * @param input   Input relational expression
   * @param metric
   */
  protected FineoRecombinatorRel(RelOptCluster cluster,
    RelTraitSet traits, RelNode input, StoreClerk.Metric metric) {
    super(cluster, traits, input);
    this.metric = metric;
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    final LogicalOperator input = implementor.visitChild(this, 0, getInput());
    FineoRecombinatorLogicalOperator op =
      new FineoRecombinatorLogicalOperator(metric.getUnderlyingMetric());
    op.setInput(input);
    return op;
  }

  public StoreClerk.Metric getMetric() {
    return this.metric;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FineoRecombinatorRel(this.getCluster(), traitSet, SingleRel.sole(inputs), metric);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("rowtype", this.getRowType());
  }
}
