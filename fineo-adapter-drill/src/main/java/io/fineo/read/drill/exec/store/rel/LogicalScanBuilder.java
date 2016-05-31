package io.fineo.read.drill.exec.store.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.tools.RelBuilder;

import static com.google.common.collect.Lists.newArrayList;

/**
 *
 */
public class LogicalScanBuilder {

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
    LogicalTableScan scan =
      new LogicalTableScan(cluster, cluster.traitSetOf(Convention.NONE), table);
    builder.push(scan);
    scanCount++;
    return this;
  }

  public RelNode build() {
    // join all the sub-tables together
    for (int i = 0; i < scanCount - 1; i++) {
      builder.union(true);
    }

    return builder.build();
  }
}
