package io.fineo.read.drill.exec.store.schema;

import io.fineo.read.drill.exec.store.FineoCommon;
import io.fineo.read.drill.exec.store.rel.recombinator.FineoRecombinatorMarkerRel;
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

  private final RelOptTable relOptTable;
  private final RelOptCluster cluster;
  private List<RelNode> tables = new ArrayList<>();
  private String orgId;
  private String metricType;

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
    // this is always a dynamic table
    RelOptTable table =
      relOptTable.getRelOptSchema().getTableForMember(newArrayList(schemaAndTable));
    LogicalTableScan scan =
      new LogicalTableScan(cluster, cluster.traitSetOf(Convention.NONE), table);
    addFields(scan);
    this.tables.add(scan);
    return this;
  }

  private void addFields(RelNode scan) {
    // ensures that the "*" operator is added to the row type
    scan.getRowType().getFieldList();
    // add the other fields that we are sure are in the table
    for (String field : FineoCommon.REQUIRED_FIELDS) {
      scan.getRowType().getField(field, false, false);
    }
  }

  public FineoRecombinatorMarkerRel buildMarker(SchemaStore store) {
    FineoRecombinatorMarkerRel marker =
      new FineoRecombinatorMarkerRel(cluster, cluster.traitSet().plus(Convention.NONE), store,
        this.relOptTable, orgId, metricType);
    marker.setInputs(this.tables);
    return marker;
  }

  public RelNode getFirstScan(){
    return this.tables.get(0);
  }

  public LogicalScanBuilder withOrgId(String orgid1) {
    this.orgId = orgid1;
    return this;
  }

  public LogicalScanBuilder withMetricType(String metricType) {
    this.metricType = metricType;
    return this;
  }
}
