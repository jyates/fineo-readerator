package io.fineo.read.drill.exec.store.rel.expansion;

import io.fineo.read.drill.exec.store.rel.recombinator.logical.SourceType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public class TableSetMarker extends SingleRel {
  private SourceType type;

  public TableSetMarker(RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType, RelNode
    input, SourceType type) {
    super(cluster, traitSet, input);
    this.type = type;
    this.rowType = rowType;
  }


  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new TableSetMarker(this.getCluster(), traitSet, this.rowType, inputs.get(0), this.type);
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
}
