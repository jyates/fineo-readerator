package io.fineo.read.drill.exec.store.rel.recombinator.logical;

import io.fineo.read.drill.exec.store.rel.fixed.logical.FixedSchemaProjection;
import io.fineo.read.drill.exec.store.rel.recombinator.FineoRecombinatorMarkerRel;
import io.fineo.schema.store.StoreClerk;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.drill.exec.planner.StarColumnHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_ID_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_METRIC_TYPE_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.TIMESTAMP_KEY;
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

    // This is actually a marker for a set of logical unions between types
    RelOptCluster cluster = fmr.getCluster();
    RelBuilder builder = RelBuilder.proto(call.getPlanner().getContext())
                                   .create(cluster, fmr.getRelSchema());

    // output type to which we need to conform
    RelDataType rowType = fmr.getRowType();

    // each input (table) is wrapped with an FRR to normalize output types
    int scanCount = 0;
    List<StoreClerk.Field> userFields = fmr.getMetric().getUserVisibleFields();
    for (RelNode relNode : fmr.getInputs()) {
      RelDataType type = relNode.getRowType();
      Map<RelDataTypeField, StoreClerk.Field> typeToField = new HashMap<>();
      // add the user fields + aliases - its a dynamic table!
      for (StoreClerk.Field field : userFields) {
        typeToField.put(type.getField(field.getName(), false, false), field);
        for (String alias : field.getAliases()) {
          typeToField.put(type.getField(alias, false, false), field);
        }
      }

      // create proper casts for known fields + expansions
      List<RexNode> expanded = new ArrayList<>();
      for (RelDataTypeField field : type.getFieldList()) {
        RexNode node = cluster.getRexBuilder().makeInputRef(relNode, field.getIndex());
        StoreClerk.Field storeField = typeToField.get(field);
        if (storeField != null) {
          node = cast(cluster.getRexBuilder(), rowType, storeField, node);
        }
        expanded.add(node);
      }
      relNode = LogicalProject.create(relNode, expanded, relNode.getRowType());


      // wrap with a filter to limit the output to the correct org and metric
      RelNode filter = LogicalFilter
        .create(relNode, getOrgAndMetricFilter(cluster.getRexBuilder(), fmr, relNode));
      // Child should be the logical equivalent of the filter
      filter = convert(filter, relNode.getTraitSet().plus(DRILL_LOGICAL));
      FineoRecombinatorRel rel =
        new FineoRecombinatorRel(cluster, relNode.getTraitSet().plus(DRILL_LOGICAL), filter,
          fmr.getMetric());
      // that is then wrapped in "fixed row type projection" so the union and downstream
      // projections apply nicely. This is especially important as the StarColumnConverter only
      // pushes down per-table projections when a parent * exists. Thus, the above rel, with the
      // dynamic field, must exist before the get to the actual schema.
      FixedSchemaProjection fsp =
        new FixedSchemaProjection(cluster, rel.getTraitSet().plus(DRILL_LOGICAL),
          rel, getProjects(cluster.getRexBuilder(), rowType), rowType);
      builder.push(fsp);
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

  private RexNode cast(RexBuilder builder, RelDataType finoRowType, StoreClerk.Field storeField,
    RexNode nodetoCast) {
    return builder
      .makeCast(finoRowType.getField(storeField.getName(), false, false).getType(), nodetoCast);
  }

  private RexNode getOrgAndMetricFilter(RexBuilder builder, FineoRecombinatorMarkerRel fmr,
    RelNode input) {
    RelDataType row = input.getRowType();
    RexInputRef org =
      builder.makeInputRef(input, row.getField(ORG_ID_KEY, false, false).getIndex());
    RexInputRef metric =
      builder.makeInputRef(input, row.getField(ORG_METRIC_TYPE_KEY, false, false).getIndex());
    StoreClerk.Metric userMetric = fmr.getMetric();
    RexLiteral orgId = builder.makeLiteral(userMetric.getOrgId());
    RexLiteral metricType = builder.makeLiteral(userMetric.getMetricId());
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
