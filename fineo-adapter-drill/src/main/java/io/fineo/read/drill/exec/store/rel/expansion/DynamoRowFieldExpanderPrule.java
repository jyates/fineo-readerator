package io.fineo.read.drill.exec.store.rel.expansion;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.Prule;

/**
 * Rule to convert to a {@link DynamoRowFieldExpanderPrule}.
 */
public class DynamoRowFieldExpanderPrule extends Prule {

  public static final DynamoRowFieldExpanderPrule INSTANCE = new DynamoRowFieldExpanderPrule();

  private DynamoRowFieldExpanderPrule() {
    super(RelOptHelper.any(DynamoRowFieldExpanderRel.class), "Prel.DynamoRowFieldExpanderPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    DynamoRowFieldExpanderRel rel = call.rel(0);
    final RelTraitSet traits = rel.getTraitSet()
                                  .plus(Prel.DRILL_PHYSICAL)
                                  .plus(DrillDistributionTrait.SINGLETON);
    RelNode convertedInput = convert(rel.getInput(), traits);
    call.transformTo(new DynamoRowFieldExpanderPrel(traits, convertedInput));
  }
}
