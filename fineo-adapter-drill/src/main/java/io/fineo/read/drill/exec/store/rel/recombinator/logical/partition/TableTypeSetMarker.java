package io.fineo.read.drill.exec.store.rel.recombinator.logical.partition;

import io.fineo.read.drill.exec.store.rel.recombinator.logical.SourceType;
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
public class TableTypeSetMarker extends AbstractRelNode {
  private List<RelNode> inputs;
  private List<SourceType> types;

  public TableTypeSetMarker(RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType) {
    super(cluster, traitSet);
    this.rowType = rowType;
  }

  public void setInputs(Collection<RelNode> tables, List<SourceType> types) {
    this.inputs = newArrayList(tables);
    this.types = types;
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode p) {
    this.inputs.set(ordinalInParent, p);
  }

  @Override
  public List<RelNode> getInputs() {
    return this.inputs;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    TableTypeSetMarker marker = new TableTypeSetMarker(this.getCluster(), traitSet, this
      .getRowType());
    marker.setInputs(inputs, this.types);
    return marker;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    for (int i = 0; i < inputs.size(); i++) {
      pw.input("sub-table[" + this.types.get(i) + "]: ", inputs.get(i));
    }
    pw.item("rowtype", this.getRowType());
    return pw;
  }

  public SourceType getType(int i) {
    return this.types.get(i);
  }
}
