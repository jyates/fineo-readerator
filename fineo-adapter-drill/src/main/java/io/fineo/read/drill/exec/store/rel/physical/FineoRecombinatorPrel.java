package io.fineo.read.drill.exec.store.rel.physical;

import io.fineo.internal.customer.Metric;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
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

  private final Metric metric;

  public FineoRecombinatorPrel(RelOptCluster cluster, RelTraitSet traits, RelNode input,
    Metric metric) {
    super(cluster, traits, input);
    this.metric  = metric;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);
    Recombinator op = new Recombinator(childPOP, metric);
    return creator.addMetadata(this, op);
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    // i guess we just use whatever the child is returning...
    return ((Prel) this.getInput()).getEncoding();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FineoRecombinatorPrel(getCluster(), traitSet, SinglePrel.sole(inputs), metric);
  }
}
