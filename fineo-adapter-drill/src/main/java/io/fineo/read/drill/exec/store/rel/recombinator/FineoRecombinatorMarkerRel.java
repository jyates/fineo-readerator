package io.fineo.read.drill.exec.store.rel.recombinator;

import io.fineo.read.drill.exec.store.rel.recombinator.logical.FineoRecombinatorRel;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.FineoRecombinatorRule;
import io.fineo.schema.store.StoreClerk;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * Marker relation that gets converted into a {@link FineoRecombinatorRel} via the
 * {@link FineoRecombinatorRule} since we cannot know at creation time what the projected and
 * filtered columns actually are to get the sub-columns that we are expanding
 */
public class FineoRecombinatorMarkerRel extends AbstractRelNode {
  private final RelOptTable parent;
  private List<RelNode> inputs;
  private StoreClerk.Metric metric;

  public FineoRecombinatorMarkerRel(RelOptCluster cluster, RelTraitSet traits,
    RelOptTable parent, StoreClerk.Metric metric) {
    super(cluster, traits.plus(Convention.NONE));
    this.parent = parent;
    this.metric = metric;
  }

  @Override
  protected RelDataType deriveRowType() {
    return parent.getRowType();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    FineoRecombinatorMarkerRel rel =
      new FineoRecombinatorMarkerRel(this.getCluster(), traitSet, this.parent, metric);
    rel.setInputs(inputs);
    return rel;
  }

  public void setInputs(List<RelNode> inputs) {
    this.inputs = inputs;
  }

  @Override
  public List<RelNode> getInputs() {
    return this.inputs;
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode p) {
    this.inputs.remove(ordinalInParent);
    this.inputs.add(ordinalInParent, p);
  }

  public RelOptSchema getRelSchema() {
    return this.parent.getRelOptSchema();
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

  public StoreClerk.Metric getMetric() {
    return metric;
  }
}
