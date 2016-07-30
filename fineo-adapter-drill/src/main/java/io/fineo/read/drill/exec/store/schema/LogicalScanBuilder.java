package io.fineo.read.drill.exec.store.schema;

import io.fineo.read.drill.exec.store.FineoCommon;
import io.fineo.read.drill.exec.store.rel.recombinator.FineoRecombinatorMarkerRel;
import io.fineo.schema.store.StoreClerk;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;

/**
 * Helper builder for a fineo scan over multiple tables.
 */
public class LogicalScanBuilder {

  private final RelOptTable relOptTable;
  private final RelOptCluster cluster;
  private List<RelNode> tables = new ArrayList<>();

  public LogicalScanBuilder(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    this.cluster = context.getCluster();
    this.relOptTable = relOptTable;
  }

  /**
   * Work around for {@link RelBuilder#scan(String)} not taking multiple String parts as in Calcite
   * 1.8. Once Drill bumps up, we can replace with just using that
   *
   * @param schemaAndTable
   * @return
   */
  public LogicalScanBuilder scan(String... schemaAndTable) {
    LogicalTableScan scan = getTableScan(schemaAndTable);
    this.tables.add(scan);
    return this;
  }

  public LogicalTableScan getTableScan(String... schemaAndTable) {
    RelOptTable table =
      checkNotNull(relOptTable.getRelOptSchema().getTableForMember(newArrayList(schemaAndTable)),
        "Could not find any input table from %s", schemaAndTable);
    LogicalTableScan scan =
      new LogicalTableScan(cluster, cluster.traitSetOf(Convention.NONE), table);
    addFields(scan);
    return scan;
  }

  private void addFields(RelNode scan) {
    // this is always a dynamic table - that the "*" operator is added to the row type
    scan.getRowType().getFieldList();
    // add the other fields that we are sure are in the table
    for (String field : FineoCommon.REQUIRED_FIELDS) {
      scan.getRowType().getField(field, false, false);
    }
  }

  public FineoRecombinatorMarkerRel buildMarker(StoreClerk.Metric metric) {
    RelTraitSet traits = cluster.traitSet()
                                .plus(Convention.NONE);
    FineoRecombinatorMarkerRel marker =
      new FineoRecombinatorMarkerRel(cluster, traits, this.relOptTable, metric);
    marker.setInputs(this.tables);
    return marker;
  }

  public void scan(RelNode relNode) {
    this.tables.add(relNode);
  }
}
