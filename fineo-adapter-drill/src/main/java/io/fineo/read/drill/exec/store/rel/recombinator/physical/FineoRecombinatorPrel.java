package io.fineo.read.drill.exec.store.rel.recombinator.physical;

import io.fineo.internal.customer.Metric;
import io.fineo.schema.store.StoreClerk;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.physical.PhysicalPlanCreator;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.SinglePrel;
import org.apache.drill.exec.record.BatchSchema;

import java.io.IOException;
import java.util.List;

/**
 * Physical representation of a recombinator
 */
public class FineoRecombinatorPrel extends SinglePrel implements Prel {

  private final StoreClerk.Metric metric;
  private final RelDataType parentRowType;

  public FineoRecombinatorPrel(RelOptCluster cluster, RelTraitSet traits, RelNode input,
    StoreClerk.Metric metric, RelDataType rowType) {
    super(cluster, traits, input);
    this.metric = metric;
    this.parentRowType = rowType;
  }

  @Override
  protected RelDataType deriveRowType() {
    return parentRowType;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);
    Recombinator op = new Recombinator(childPOP, metric.getUnderlyingMetric());
    return creator.addMetadata(this, op);
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return BatchSchema.SelectionVectorMode.NONE;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FineoRecombinatorPrel(getCluster(), traitSet, SinglePrel.sole(inputs), metric,
      getRowType());
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("rowtype", this.getRowType());
  }
}