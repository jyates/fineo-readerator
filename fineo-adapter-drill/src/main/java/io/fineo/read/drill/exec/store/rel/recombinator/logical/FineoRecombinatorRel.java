package io.fineo.read.drill.exec.store.rel.recombinator.logical;

import io.fineo.schema.store.StoreClerk;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
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

  private final StoreClerk.Metric metric;
  private final SourceType type;

  /**
   * Creates a <code>SingleRel</code>.
   *
   * @param cluster Cluster this relational expression belongs to
   * @param traits
   * @param input   Input relational expression
   * @param metric
   * @param rowType
   * @param type
   */
  protected FineoRecombinatorRel(RelOptCluster cluster,
    RelTraitSet traits, RelNode input, StoreClerk.Metric metric, RelDataType rowType,
    SourceType type) {
    super(cluster, traits, input);
    this.metric = metric;
    this.rowType = rowType;
    this.type = type;
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    final LogicalOperator input = implementor.visitChild(this, 0, getInput());
    FineoRecombinatorLogicalOperator op =
      new FineoRecombinatorLogicalOperator(metric.getUnderlyingMetric(), type);
    op.setInput(input);
    return op;
  }

  public StoreClerk.Metric getMetric() {
    return this.metric;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FineoRecombinatorRel(this.getCluster(), traitSet, SingleRel.sole(inputs), metric,
      rowType, type);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("rowtype", this.getRowType());
  }

  public SourceType getSourceType() {
    return type;
  }
}
