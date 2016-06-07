package io.fineo.read.drill.exec.store.schema;

import io.fineo.read.drill.exec.store.FineoCommon;
import io.fineo.read.drill.exec.store.rel.FineoRecombinatorMarkerRel;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Helper builder for a fineo scan over multiple tables.
 */
public class LogicalScanBuilder {

  private final RelBuilder builder;
  private final RelOptTable relOptTable;
  private final RelOptCluster cluster;
  private int scanCount = 0;
  private List<RelNode> tables = new ArrayList<>();

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
    this.tables.add(scan);
    scanCount++;
    return this;
  }

  public FineoRecombinatorMarkerRel buildMarker(SchemaStore store) {
    FineoRecombinatorMarkerRel marker =
      new FineoRecombinatorMarkerRel(cluster, cluster.traitSet().plus(Convention.NONE), store,
        this.relOptTable);
    marker.setInputs(this.tables);
    return marker;
  }

  private void addFields(RelNode scan) {
    // ensures that the "*" operator is added to the row type
    scan.getRowType().getFieldList();
    // add the other fields that we are sure are present
    for (String field : FineoCommon.REQUIRED_FIELDS) {
      scan.getRowType().getField(field, false, false);
    }
  }

  public RelNode build(SchemaStore store) {
//    for (LogicalTableScan lts : this.tables) {
////      FineoRecombinatorMarkerRel rel =
////        new FineoRecombinatorMarkerRel(store, lts, cluster.getTypeFactory());
//      builder.push(rel);
//    }

    // union all the results
    for (int i = 0; i < scanCount - 1; i++) {
      builder.union(true);
    }

    return builder.build();
  }
}
