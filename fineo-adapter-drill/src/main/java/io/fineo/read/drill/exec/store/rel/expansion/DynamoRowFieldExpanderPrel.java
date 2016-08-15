package io.fineo.read.drill.exec.store.rel.expansion;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.physical.PhysicalPlanCreator;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.SinglePrel;
import org.apache.drill.exec.record.BatchSchema;

import java.io.IOException;

public class DynamoRowFieldExpanderPrel extends SinglePrel {

  public DynamoRowFieldExpanderPrel(RelTraitSet traits, RelNode child) {
    super(child.getCluster(), traits, child);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);
    return creator.addMetadata(this, new DynamoExpander(childPOP));
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return BatchSchema.SelectionVectorMode.NONE;
  }
}
