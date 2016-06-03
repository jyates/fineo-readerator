package io.fineo.read.drill.exec.store.rel.physical;

import io.fineo.read.drill.exec.store.rel.logical.FineoRecombinatorRel;
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
import java.util.Map;

/**
 * Physical representation of a recombinator
 */
public class FineoRecombinatorPrel extends SinglePrel implements Prel {

  private final Map<String, List<String>> cnameMap;

  public FineoRecombinatorPrel(RelOptCluster cluster,
    RelTraitSet traits, FineoRecombinatorRel from) {
    this(cluster, traits, from.getInput(), from.getCnameToAlias());
  }

  private FineoRecombinatorPrel(RelOptCluster cluster, RelTraitSet traits, RelNode input,
    Map<String, List<String>> cnameMap) {
    super(cluster, traits, input);
    this.cnameMap = cnameMap;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);
    Recombinator op = new Recombinator(childPOP, cnameMap);
    return creator.addMetadata(this, op);
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return null;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FineoRecombinatorPrel(getCluster(), traitSet, SinglePrel.sole(inputs), cnameMap);
  }
}
