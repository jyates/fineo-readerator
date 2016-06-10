package io.fineo.read.drill.exec.store.rel.fixed.physical;

import io.fineo.read.drill.exec.store.rel.fixed.logical.FixedSchemaProjection;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.Prule;

public class FixedSchemaPrule extends Prule {

  public static FixedSchemaPrule INSTANCE = new FixedSchemaPrule();

  private FixedSchemaPrule() {
    super(RelOptHelper.any(FixedSchemaProjection.class), "Prel.FixedSchemaPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    FixedSchemaProjection project = call.rel(0);
    RelNode input = project.getInput();
    RelTraitSet traits = input.getTraitSet().plus(Prel.DRILL_PHYSICAL);
    RelNode convertedInput = convert(input, traits);
    FixedSchemaPrel prel =
      new FixedSchemaPrel(project.getCluster(), traits, convertedInput, project.getRowType(),
        project.getProjections());
    call.transformTo(prel);
  }
}
