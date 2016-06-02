package io.fineo.read.drill.exec.store.rel.logical;

import io.fineo.internal.customer.Metric;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.planner.logical.DrillImplementor;
import org.apache.drill.exec.planner.logical.DrillRel;

import java.util.List;
import java.util.Map;

/**
 * Logical conversion of fields from the sub-query into the fields that is actually visible in this
 * metric.
 */
public class FineoRecombinatorRel extends SingleRel implements DrillRel {

  private Map<String, List<String>> cnameToAlias;

  /**
   * Creates a <code>SingleRel</code>.
   *
   * @param cluster Cluster this relational expression belongs to
   * @param traits
   * @param input   Input relational expression
   * @param metric
   */
  protected FineoRecombinatorRel(RelOptCluster cluster,
    RelTraitSet traits, RelNode input, Metric metric) {
    super(cluster, traits, input);
    this.cnameToAlias = metric.getMetadata().getCanonicalNamesToAliases();
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    final LogicalOperator input = implementor.visitChild(this, 0, getInput());
    FineoRecombinatorLogicalOperator op = new FineoRecombinatorLogicalOperator(cnameToAlias);
    op.setInput(input);
    return op;
  }

  public Map<String, List<String>> getCnameToAlias() {
    return this.cnameToAlias;
  }
}
