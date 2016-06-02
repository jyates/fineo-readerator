package io.fineo.read.drill.exec.store.schema;

import io.fineo.read.drill.exec.store.rel.FineoRecombinatorMarkerRel;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_ID_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_METRIC_TYPE_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.TIMESTAMP_KEY;

/**
 * Helper builder for a fineo scan over multiple tables.
 */
public class LogicalScanBuilder {

  private static final List<String> REQUIRED_FIELDS =
    newArrayList(ORG_ID_KEY, ORG_METRIC_TYPE_KEY);//, TIMESTAMP_KEY);

  private final RelBuilder builder;
  private final RelOptTable relOptTable;
  private final RelOptCluster cluster;
  private int scanCount = 0;

  public LogicalScanBuilder(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
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
    LogicalTableScan scan =
      new LogicalTableScan(cluster, cluster.traitSetOf(Convention.NONE), table);
    addFields(scan);
    builder.push(scan);
    scanCount++;
    return this;
  }

  private void addFields(RelNode scan) {
    // ensures that the "*" operator is added to the row type
    scan.getRowType().getFieldList();
    // add the other fields that we are sure are present
    for (String field : REQUIRED_FIELDS) {
      scan.getRowType().getField(field, false, false);
    }
  }

  public RelNode build(SchemaStore store) {
    OffsetTracker tracker = new OffsetTracker();
    // join all the sub-tables together on the common keys
    for (int i = 0; i < scanCount - 1; i++) {
      RexNode equals = composeCondition(tracker, ORG_ID_KEY);
      builder.join(JoinRelType.INNER, equals);
    }

//    int index = builder.peek().getRowType().getField(TIMESTAMP_KEY, false, false).getIndex();
//    builder.sort(-index - 1);
    RelNode subscans = builder.build();
    return new FineoRecombinatorMarkerRel(store, subscans, subscans.getRowType());
  }

  private RexNode composeCondition(OffsetTracker tracker, String... fieldNames) {
    RelNode table1 = builder.peek(0);
    RelNode table2 = builder.peek(1);

    // build the rex node for the two tables
    final List<RexNode> conditions = new ArrayList<>();
    for (String fieldName : fieldNames) {
      conditions.add(
        builder.call(SqlStdOperatorTable.EQUALS,
          field(table1, fieldName, tracker),
          field(table2, fieldName, tracker)));
    }
    return RexUtil.composeConjunction(cluster.getRexBuilder(), conditions, false);
  }

  private RexNode field(RelNode table, String fieldName, OffsetTracker offset) {
    RelDataType row = table.getRowType();
    RelDataTypeField field = row.getField(fieldName, true, false);
    int index = field.getIndex();
    return cluster.getRexBuilder().makeInputRef(row, index + offset.getOffset(table));
  }

  private class OffsetTracker {
    private final Map<RelNode, Integer> offsets = new HashMap<>();
    private int nextOffset = 0;

    public int getOffset(RelNode node) {
      Integer offset = offsets.get(node);
      if (offset == null) {
        offset = nextOffset;
        offsets.put(node, offset);
        nextOffset = node.getRowType().getFieldCount();
      }
      return offset;
    }
  }
}
