package io.fineo.read.drill.exec.store.rel;

import io.fineo.schema.avro.AvroSchemaEncoder;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Helper builder for a fineo scan over multiple tables.
 */
public class LogicalScanBuilder {

  private static final String[] REQUIRED_FIELDS =
    new String[]{AvroSchemaEncoder.ORG_ID_KEY, AvroSchemaEncoder.ORG_METRIC_TYPE_KEY,
      AvroSchemaEncoder.TIMESTAMP_KEY};
  private static final String UNKNOWN_FIELD = "_unknown";

  private final RelBuilder builder;
  private RelOptTable.ToRelContext context;
  private final RelOptTable relOptTable;
  private final RelOptCluster cluster;
  private int scanCount = 0;

  public LogicalScanBuilder(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    this.context = context;
    this.cluster = context.getCluster();
    this.relOptTable = relOptTable;
    this.builder = RelBuilder.proto(cluster.getPlanner().getContext())
                             .create(cluster, relOptTable.getRelOptSchema());
  }

  /**
   * Work around for {@link RelBuilder#scan(String)} not taking multiple String parts as in Calcite
   * 1.8. Once Drill bumps up, we can replace with just using that
   *
   * @param schemaAndTable
   * @return
   */
  public LogicalScanBuilder scan(String... schemaAndTable) {
    RelOptTable table =
      relOptTable.getRelOptSchema().getTableForMember(newArrayList(schemaAndTable));
    // ensures that the "*" operator is added to the row type
    table.getRowType().getFieldCount();
    LogicalTableScan scan =
      new LogicalTableScan(cluster, cluster.traitSetOf(Convention.NONE), table);
    builder.push(scan);
    scanCount++;
    return this;
  }

  public RelNode build() {
    // join all the sub-tables together on the common keys
    OffsetTracker offsets = new OffsetTracker();
    for (int i = 0; i < scanCount - 1; i++) {
      builder.join(JoinRelType.FULL, composeCondition(offsets, AvroSchemaEncoder.ORG_ID_KEY));
    }

    RelNode subscans = builder.build();
    return subscans;
  }

  private RexNode composeCondition(OffsetTracker offsets, String... fieldNames) {
    RelNode table1 = builder.peek(0);
    RelNode table2 = builder.peek(1);
    // build the rex node for the two tables
    final List<RexNode> conditions = new ArrayList<>();
    for (String fieldName : fieldNames) {
      conditions.add(
        builder.call(SqlStdOperatorTable.EQUALS,
          field(table1, offsets, fieldName),
          field(table2, offsets, fieldName)));
    }
    return RexUtil.composeConjunction(cluster.getRexBuilder(), conditions, false);
  }

  private RexNode field(RelNode table1, OffsetTracker offsets, String fieldName) {
    int offset = offsets.getOffset(table1);
    RelDataType row = table1.getRowType();
    RelDataTypeField field = row.getField(fieldName, true, false);
    int index = field.getIndex();
    RexBuilder rexer = cluster.getRexBuilder();
    return cluster.getRexBuilder().makeInputRef(row, index);
//    return rexer.makeRangeReference(row, offset + index, false);
  }

  private class OffsetTracker {
    private Map<RelNode, Integer> offsets = new HashMap<>();
    private int nextOffset = 0;

    public Integer getOffset(RelNode node) {
      Integer offset = offsets.get(node);
      if (offset == null) {
        offset = nextOffset;
        offsets.put(node, offset);
        nextOffset += node.getRowType().getFieldList().size();
      }
      return offset;
    }
  }
}
