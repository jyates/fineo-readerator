package io.fineo.read.drill.exec.store.rel.recombinator.logical;

import com.google.common.collect.ImmutableList;
import io.fineo.internal.customer.Metric;
import io.fineo.read.drill.exec.store.rel.fixed.logical.FixedSchemaProjection;
import io.fineo.read.drill.exec.store.rel.recombinator.FineoRecombinatorMarkerRel;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.drill.exec.planner.StarColumnHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.schema.avro.AvroSchemaEncoder.*;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_ID_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_METRIC_TYPE_KEY;
import static java.util.stream.Collectors.toList;
import static org.apache.drill.exec.planner.logical.DrillRel.DRILL_LOGICAL;

/**
 * Converts a projection + filter into a projection + filter across all the possible field names
 * in the underlying table and combined back into a single relation via the
 * {@link FineoRecombinatorMarkerRel}
 */
public class FineoRecombinatorRule extends RelOptRule {

  private static final List<String> REQUIRED_FIELDS = newArrayList(ORG_ID_KEY, ORG_METRIC_TYPE_KEY);

  public static final FineoRecombinatorRule INSTANCE = new FineoRecombinatorRule();

  private FineoRecombinatorRule() {
    super(operand(FineoRecombinatorMarkerRel.class, RelOptRule.any()), "FineoRecombinatorRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    FineoRecombinatorMarkerRel fmr = call.rel(0);

    // need the uppercase names here because that's how we pulled them out of the query
    SchemaStore store = fmr.getStore();
    AvroSchemaManager schema = new AvroSchemaManager(store, fmr.getOrgId());
    Metric metric = schema.getMetricInfo(fmr.getMetricType());

    // This is actually a marker for a set of logical unions between types
    RelOptCluster cluster = fmr.getCluster();
    RelBuilder builder = RelBuilder.proto(call.getPlanner().getContext())
                                   .create(cluster, fmr.getRelSchema());

    // output type to which we need to conform
    RelDataType rowType = fmr.getRowType();

    // each input (table) is wrapped with an FRR to normalize output types
    int scanCount = 0;
    for (RelNode relNode : fmr.getInputs()) {

      // wrap with a filter to limit the output to the correct org and metric
      RelNode filter = LogicalFilter
        .create(relNode, getOrgAndMetricFilter(cluster.getRexBuilder(), fmr, relNode));
      // Child should be the logical equivalent of the filter
      filter = convert(filter, relNode.getTraitSet().plus(DRILL_LOGICAL));
      FineoRecombinatorRel rel =
        new FineoRecombinatorRel(cluster, relNode.getTraitSet().plus(DRILL_LOGICAL), filter, metric);
      // that is then wrapped in "fixed row type projection" so the union and downstream
      // projections apply nicely. This is especially important as the StarColumnConverter only
      // pushes down per-table projections when a parent * exists. Thus, the above rel, with the
      // dynamic field, must exist before the get to the actual schema.
      FixedSchemaProjection fsp =
        new FixedSchemaProjection(cluster, rel.getTraitSet().plus(DRILL_LOGICAL),
          rel, getProjects(cluster.getRexBuilder(), rowType), rowType);
      builder.push(fsp);
//      addSort(builder, cluster);
      scanCount++;
    }

    // combine the subqueries with a set of unions
    for (int i = 0; i < scanCount - 1; i++) {
      builder.union(true);
    }

    // result needs to be sorted on the timestamp
    addSort(builder, cluster);
    call.transformTo(builder.build());
  }

  private RexNode getOrgAndMetricFilter(RexBuilder builder, FineoRecombinatorMarkerRel fmr,
    RelNode input) {
    RelDataType row = input.getRowType();
    RexInputRef org =
      builder.makeInputRef(input, row.getField(ORG_ID_KEY, false, false).getIndex());
    RexInputRef metric =
      builder.makeInputRef(input, row.getField(ORG_METRIC_TYPE_KEY, false, false).getIndex());
    RexLiteral orgId = builder.makeLiteral(fmr.getOrgId());
    RexLiteral metricType = builder.makeLiteral(fmr.getMetricType());
    RexNode orgEq = builder.makeCall(SqlStdOperatorTable.EQUALS, org, orgId);
    RexNode metricEq = builder.makeCall(SqlStdOperatorTable.EQUALS, metric, metricType);
    return RexUtil.composeConjunction(builder, of(orgEq, metricEq), false);
  }

  private List<RexNode> getProjects(RexBuilder rexBuilder, RelDataType rowType) {
    return rowType.getFieldList().stream()
                  .filter(field -> !StarColumnHelper.isStarColumn(field.getName()))
                  .map(field -> rexBuilder.makeInputRef(field.getType(), field.getIndex()))
                  .collect(toList());
  }

  private void addSort(RelBuilder builder, RelOptCluster cluster) {
    RelNode node = builder.peek();
    RelDataType type = node.getRowType();
    RelDataTypeField field = type.getField(TIMESTAMP_KEY, false, false);
    RexNode sortNode = cluster.getRexBuilder().makeInputRef(node, field.getIndex());
    builder.sort(sortNode);
  }
}
