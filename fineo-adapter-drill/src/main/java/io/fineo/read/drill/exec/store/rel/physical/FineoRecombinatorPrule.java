package io.fineo.read.drill.exec.store.rel.physical;

import io.fineo.read.drill.exec.store.rel.logical.FineoRecombinatorRel;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.Prule;

/**
 * Rule to convert to a {@link FineoRecombinatorPrule}.
 */
public class FineoRecombinatorPrule extends Prule {
  public FineoRecombinatorPrule() {
    super(RelOptHelper.any(FineoRecombinatorRel.class), "Prel.FineoRecombinatorPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    FineoRecombinatorRel rel = call.rel(0);
    // one everywhere we run
//    DrillDistributionTrait distribution =
//      new DrillDistributionTrait(DrillDistributionTrait.DistributionType.BROADCAST_DISTRIBUTED);
    final RelTraitSet traits = rel.getTraitSet()
                                  .plus(Prel.DRILL_PHYSICAL)
                                  .plus(DrillDistributionTrait.SINGLETON);
    RelNode convertedInput = convert(rel.getInput(), traits);
    call.transformTo(
      new FineoRecombinatorPrel(rel.getCluster(), traits, convertedInput, rel.getMetric(), rel.getRowType()));
  }
}
