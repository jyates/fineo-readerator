package io.fineo.read.drill.exec.store.rel.recombinator.logical;

import io.fineo.lambda.dynamo.Schema;
import io.fineo.read.drill.exec.store.rel.recombinator.FineoRecombinatorMarkerRel;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.partition.TableTypeSetMarker;
import io.fineo.schema.store.AvroSchemaProperties;
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.drill.exec.planner.sql.DrillSqlOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.drill.exec.planner.logical.DrillRel.DRILL_LOGICAL;

/**
 * Converts a projection + filter into a projection + filter across all the possible field names
 * in the underlying table and combined back into a single relation via the
 * {@link FineoRecombinatorMarkerRel}
 */
public class FineoRecombinatorRule extends RelOptRule {

  public static final FineoRecombinatorRule INSTANCE = new FineoRecombinatorRule();

  private FineoRecombinatorRule() {
    super(operand(FineoRecombinatorMarkerRel.class,
      operand(TableTypeSetMarker.class, RelOptRule.any())),
      "Fineo::LogicalRecombinatorRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    FineoRecombinatorMarkerRel fmr = call.rel(0);
    TableTypeSetMarker marker = call.rel(1);

    // This is actually a marker for a set of logical unions between types
    RelOptCluster cluster = fmr.getCluster();
    RelBuilder builder = RelBuilder.proto(call.getPlanner().getContext())
                                   .create(cluster, fmr.getRelSchema());

    // output type to which we need to conform
    RelDataType rowType = fmr.getRowType();

    RexBuilder rexBuilder = cluster.getRexBuilder();
    int scanCount = 0;
    List<StoreClerk.Field> userFields = fmr.getMetric().getUserVisibleFields();
    for (int i = 0; i < marker.getInputs().size(); i++) {
      SourceType type = marker.getType(i);
      // cast table fields to the expected user-typed fields
      RelNode table = cast(marker.getInput(i), userFields, rexBuilder, rowType);

      // wrap with a filter to limit the output to the correct org and metric tableNode
      RexNode condition = getOrgAndMetricFilter(rexBuilder, fmr, table, type);
      RelNode filter = LogicalFilter.create(table, condition);

      // Child of this is the -logical- equivalent of the filter
      filter = convert(filter, table.getTraitSet().plus(DRILL_LOGICAL));
      FineoRecombinatorRel rel =
        new FineoRecombinatorRel(cluster, table.getTraitSet().plus(DRILL_LOGICAL), filter,
          fmr.getMetric(), rowType, type);
      builder.push(rel);
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

  private RelNode cast(RelNode tableNode, List<StoreClerk.Field> userFields, RexBuilder rexer,
    RelDataType rowType) {
    RelDataType tableType = tableNode.getRowType();
    Map<RelDataTypeField, StoreClerk.Field> typeToField = new HashMap<>();
    // add the user fields + aliases - its a dynamic table!
    for (StoreClerk.Field field : userFields) {
      // user visible field
      typeToField.put(tableType.getField(field.getName(), false, false), field);
      // aliases
      for (String alias : field.getAliases()) {
        typeToField.put(tableType.getField(alias, false, false), field);
      }
      // cname
      typeToField.put(tableType.getField(field.getCname(), false, false), field);
    }

    // create proper casts for known fields + expansions.
    List<RexNode> expanded = new ArrayList<>();
    for (RelDataTypeField field : tableType.getFieldList()) {
      RexNode node = rexer.makeInputRef(tableNode, field.getIndex());
      StoreClerk.Field storeField = typeToField.get(field);
      if (storeField != null) {
        node = cast(rexer, rowType, storeField, node);
      } else if (field.getName().equals(AvroSchemaProperties.TIMESTAMP_KEY)) {
        // dynamo reads the timestamp as a string, so we need to cast it to the expected LONG type
        node = cast(rexer, rowType, AvroSchemaProperties.TIMESTAMP_KEY, node);
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
    RelNode castProject, SourceType type) {
    RelDataType row = castProject.getRowType();
    StoreClerk.Metric userMetric = fmr.getMetric();
    switch (type) {
      case DFS:
        RexInputRef org =
          builder.makeInputRef(castProject, row.getField(AvroSchemaProperties.ORG_ID_KEY, false,
            false).getIndex());
        RexInputRef metric =
          builder
            .makeInputRef(castProject, row.getField(AvroSchemaProperties.ORG_METRIC_TYPE_KEY,
              false, false).getIndex());
        RexLiteral orgId = builder.makeLiteral(userMetric.getOrgId());
        RexLiteral metricType = builder.makeLiteral(userMetric.getMetricId());
        RexNode orgEq = builder.makeCall(SqlStdOperatorTable.EQUALS, org, orgId);
        RexNode metricEq = builder.makeCall(SqlStdOperatorTable.EQUALS, metric, metricType);
        return RexUtil.composeConjunction(builder, of(orgEq, metricEq), false);
      case DYNAMO:
        String partitionKey = Schema.getPartitionKey(userMetric.getOrgId(), userMetric
          .getMetricId()).getS();
        RexInputRef pkRef =
          builder.makeInputRef(castProject,
            row.getField(Schema.PARTITION_KEY_NAME, false, false).getIndex());
        RexLiteral pk = builder.makeLiteral(partitionKey);
        return builder.makeCall(SqlStdOperatorTable.EQUALS, pkRef, pk);
    }
    throw new UnsupportedOperationException("Don't know how to support table type: " + type);
  }

  private void addSort(RelBuilder builder, RelOptCluster cluster) {
    RelNode node = builder.peek();
    RelDataType type = node.getRowType();
    RelDataTypeField field = type.getField(AvroSchemaProperties.TIMESTAMP_KEY, false, false);
    RexNode sortNode = cluster.getRexBuilder().makeInputRef(node, field.getIndex());
    builder.sort(sortNode);
  }
}
