package io.fineo.read.drill.exec.store.rel.recombinator.logical;

import io.fineo.lambda.dynamo.Schema;
import io.fineo.read.drill.exec.store.rel.fixed.logical.FixedSchemaProjection;
import io.fineo.read.drill.exec.store.rel.recombinator.FineoRecombinatorMarkerRel;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.store.StoreClerk;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.RelSubset;
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.drill.exec.planner.sql.DrillSqlOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.of;
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

    RexBuilder rexBuilder = cluster.getRexBuilder();
    int scanCount = 0;
    List<StoreClerk.Field> userFields = fmr.getMetric().getUserVisibleFields();
    for (RelNode tableNode : fmr.getInputs()) {
      // cast table fields to the expected user-typed fields
      tableNode = cast(tableNode, userFields, rexBuilder, rowType);

      // wrap with a filter to limit the output to the correct org and metric
      RexNode orgMetricCondition = getOrgAndMetricFilter(rexBuilder, fmr, tableNode);
      RelNode filter = LogicalFilter.create(tableNode, orgMetricCondition);

      // Child of this is the -logical- equivalent of the filter
      filter = convert(filter, tableNode.getTraitSet().plus(DRILL_LOGICAL));
      FineoRecombinatorRel rel =
        new FineoRecombinatorRel(cluster, tableNode.getTraitSet().plus(DRILL_LOGICAL), filter,
          fmr.getMetric(), rowType);
      // that is then wrapped in "fixed row type projection" so the union and downstream
      // projections apply nicely. This is especially important as the StarColumnConverter only
      // pushes down per-table projections when a parent * exists. Thus, the above rel, with the
      // dynamic field, must exist before we project to the actual schema.
//      FixedSchemaProjection fsp =
//        new FixedSchemaProjection(cluster, rel.getTraitSet().plus(DRILL_LOGICAL),
//          rel, getProjects(cluster.getRexBuilder(), rowType), rowType);
//      builder.push(fsp);
      builder.push(rel);
      scanCount++;
    }

    // combine the subqueries with a set of unions
    for (int i = 0; i < scanCount - 1; i++) {
      builder.union(true);
    }

    // result needs to be sorted on the timestamp
//    addSort(builder, cluster);
    call.transformTo(builder.build());
  }

  private RelNode cast(RelNode tableNode, List<StoreClerk.Field> userFields, RexBuilder rexer,
    RelDataType rowType) {
    RelDataType tableType = tableNode.getRowType();
    Map<RelDataTypeField, StoreClerk.Field> typeToField = new HashMap<>();
    // add the user fields + aliases - its a dynamic table!
    for (StoreClerk.Field field : userFields) {
      typeToField.put(tableType.getField(field.getName(), false, false), field);
      for (String alias : field.getAliases()) {
        typeToField.put(tableType.getField(alias, false, false), field);
      }
    }

    // create proper casts for known fields + expansions.
    List<RexNode> expanded = new ArrayList<>();
    for (RelDataTypeField field : tableType.getFieldList()) {
      RexNode node = rexer.makeInputRef(tableNode, field.getIndex());
      StoreClerk.Field storeField = typeToField.get(field);
      if (storeField != null) {
        node = cast(rexer, rowType, storeField, node);
      } else if (field.getName().equals(AvroSchemaEncoder.TIMESTAMP_KEY)) {
        // dynamo reads the timestamp as a string, so we need to cast it to the expected LONG type
        node = cast(rexer, rowType, AvroSchemaEncoder.TIMESTAMP_KEY, node);
      }
      expanded.add(node);
    }
    return LogicalProject.create(tableNode, expanded, tableType);
  }

  private RexNode cast(RexBuilder builder, RelDataType fineoRowType, StoreClerk.Field storeField,
    RexNode nodeToCast) {
    return cast(builder, fineoRowType, storeField.getName(), nodeToCast);
  }

  private RexNode cast(RexBuilder builder, RelDataType row, String fieldName, RexNode refToCast) {
    RelDataType type = row.getField(fieldName, false, false).getType();
    RexNode cast = builder.makeCast(type, refToCast);
    // add a base64 decoding for byte[] stored as text
    if (type.getSqlTypeName() == SqlTypeName.BINARY) {
      cast =
        builder.makeCall(type, new DrillSqlOperator("FINEO_BASE64_DECODE", 1, false), of(cast));
    }
    return cast;
  }

  private RexNode getOrgAndMetricFilter(RexBuilder builder, FineoRecombinatorMarkerRel fmr,
    RelNode castProject) {
    RelDataType row = castProject.getRowType();
    StoreClerk.Metric userMetric = fmr.getMetric();
    List<String> name = getSourceTableQualifiedName(castProject);
    switch (name.get(0)) {
      case "dfs":
        RexInputRef org =
          builder.makeInputRef(castProject, row.getField(ORG_ID_KEY, false, false).getIndex());
        RexInputRef metric =
          builder
            .makeInputRef(castProject, row.getField(ORG_METRIC_TYPE_KEY, false, false).getIndex());
        RexLiteral orgId = builder.makeLiteral(userMetric.getOrgId());
        RexLiteral metricType = builder.makeLiteral(userMetric.getMetricId());
        RexNode orgEq = builder.makeCall(SqlStdOperatorTable.EQUALS, org, orgId);
        RexNode metricEq = builder.makeCall(SqlStdOperatorTable.EQUALS, metric, metricType);
        return RexUtil.composeConjunction(builder, of(orgEq, metricEq), false);
      case "dynamo":
        String partitionKey = Schema.getPartitionKey(userMetric.getOrgId(), userMetric
          .getMetricId()).getS();
        RexInputRef pkRef =
          builder.makeInputRef(castProject,
            row.getField(Schema.PARTITION_KEY_NAME, false, false).getIndex());
        RexLiteral pk = builder.makeLiteral(partitionKey);
        return builder.makeCall(SqlStdOperatorTable.EQUALS, pkRef, pk);
    }
    throw new UnsupportedOperationException("Don't know how to support table: " + name);
  }

  private List<String> getSourceTableQualifiedName(RelNode input) {
    RelOptTable table = null;
    while (table == null) {
      table = input.getTable();
      if (table == null) {
        input = input instanceof RelSubset ?
                ((RelSubset) input).getRelList().get(0) :
                input.getInput(0);
      }
    }
    return table.getQualifiedName();
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
