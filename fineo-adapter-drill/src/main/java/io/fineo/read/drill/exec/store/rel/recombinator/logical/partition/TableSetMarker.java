package io.fineo.read.drill.exec.store.rel.recombinator.logical.partition;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Collection;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Marker Rel for tracking the tables in a given set
 */
public class TableSetMarker extends AbstractRelNode {
  private List<RelNode> inputs;

  public TableSetMarker(RelOptCluster cluster, RelTraitSet traitSet, RelDataType
    rowType) {
    super(cluster, traitSet);
    this.rowType = rowType;
  }

  public void setInputs(Collection<RelNode> tables) {
    this.inputs = newArrayList(tables);
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode p) {
    this.inputs.remove(ordinalInParent);
    this.inputs.add(ordinalInParent, p);
  }

  @Override
  public List<RelNode> getInputs() {
    return this.inputs;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    TableSetMarker marker = new TableSetMarker(this.getCluster(), traitSet, this.getRowType());
    marker.inputs = inputs;
    return marker;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    for (RelNode node : inputs) {
      pw.input("sub-table", node);
    }
    pw.item("rowtype", this.getRowType());
    return pw;
  }
}
