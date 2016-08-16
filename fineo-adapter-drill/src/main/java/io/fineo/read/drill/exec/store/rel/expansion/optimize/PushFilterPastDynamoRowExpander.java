package io.fineo.read.drill.exec.store.rel.expansion.optimize;


import io.fineo.lambda.dynamo.Schema;
import io.fineo.read.drill.exec.store.rel.expansion.logical.DynamoRowFieldExpanderRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.drill.exec.planner.physical.PrelUtil.getPlannerSettings;

public class PushFilterPastDynamoRowExpander extends RelOptRule {

  private static final Logger LOG = LoggerFactory.getLogger(PushFilterPastDynamoRowExpander.class);
  public static final PushFilterPastDynamoRowExpander INSTANCE = new
    PushFilterPastDynamoRowExpander();

  private PushFilterPastDynamoRowExpander() {
    super(operand(DrillFilterRel.class, operand(DynamoRowFieldExpanderRel.class, any())),
      "Fineo::PushFilterPathDynamoRowExpander");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    DrillFilterRel filterRel = call.rel(0);
    DynamoRowFieldExpanderRel expander = call.rel(1);

    RexNode filterCond = filterRel.getCondition();
    RexBuilder builder = filterRel.getCluster().getRexBuilder();
    FilterFieldLifter lifter = new FilterFieldLifter(filterCond, expander
      .getRowType(), builder, Schema.PARTITION_KEY_NAME, Schema.SORT_KEY_NAME);
    RexNode lifted = lifter.lift();
    if (lifted == null) {
      LOG.debug("Cannot lift expression: {}", filterCond);
      return;
    }

    List<RexNode> conjunctions = RelOptUtil.conjunctions(lifted);
    List<RexNode> filterConjunctions = RelOptUtil.conjunctions(lifter.getFilterCnf());

    // attempt to remove any part of the lifted part from the original filter
    boolean removed = false;
    for (RexNode part : conjunctions) {
      if (RexUtil.removeAll(filterConjunctions, part)) {
        removed = true;
      }
    }

    // we didn't remove any, but we did pull some out - this filter seems complex. Just add a new
    // filter below the expander and push down what we can
    if (!removed) {
      Filter dynamoFilter = filter(lifted, expander.getInput(), filterRel.getTraitSet());
      RelNode newExpander = expander.copy(expander.getTraitSet(), of(dynamoFilter));
      Filter newFilter = filter(filterCond, newExpander, filterRel.getTraitSet());
      call.transformTo(newFilter);
      return;
    }

    // we did remove some! time to generate a new filter
    if (filterConjunctions.size() == 0) {
      // removed all of them - create a new filter below!
      Filter newFilterRel = filter(lifted, expander.getInput(), filterRel.getTraitSet());
      RelNode newExpander = expander.copy(expander.getTraitSet(), of(newFilterRel));
      call.transformTo(newExpander);
    } else {
      // only removed some of them - rebuild the filter appropriately
      Filter dynamoFilter = filter(lifted, expander.getInput(), filterRel.getTraitSet());
      RelNode newExpander = expander.copy(expander.getTraitSet(), of(dynamoFilter));
      Filter newFilter = filter(RexUtil.composeConjunction(builder,
        filterConjunctions, false), newExpander, filterRel.getTraitSet());
      call.transformTo(newFilter);
    }
  }

  private DrillFilterRel filter(RexNode condition, RelNode input, RelTraitSet traits) {
    RelOptCluster cluster = input.getCluster();
    return new DrillFilterRel(cluster, traits, input, condition);
  }
}
