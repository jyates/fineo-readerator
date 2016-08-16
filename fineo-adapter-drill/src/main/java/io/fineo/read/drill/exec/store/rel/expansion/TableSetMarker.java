package io.fineo.read.drill.exec.store.rel.expansion;

import io.fineo.read.drill.exec.store.rel.recombinator.logical.SourceType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * Basic information about the source table - its name and type. This is mostly because I can't
 * figure out how to force Calcite to give me the input TableScan as a child in
 * ConvertFineoMarkerIntoFilterInputTable. The other major use to to ensure that we step through
 * the GroupTablesAndOptionallyExpandRule to ensure the use of the DynamoRowFieldExpander as it
 * is necessary (i.e. when reading dynamo).
 */
public class TableSetMarker extends SingleRel {
  private SourceType type;
  private String tableName;

  public TableSetMarker(RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType, RelNode
    input, SourceType type, String tableName) {
    super(cluster, traitSet, input);
    this.type = type;
    this.tableName = tableName;
    this.rowType = rowType;
  }


  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new TableSetMarker(this.getCluster(), traitSet, this.rowType, inputs.get(0), this.type,
      tableName);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    pw.item("source-type", this.getType());
    pw.item("rowtype", this.getRowType());
    return pw;
  }

  public SourceType getType() {
    return this.type;
  }

  public String getTableName() {
    return tableName;
  }
}
