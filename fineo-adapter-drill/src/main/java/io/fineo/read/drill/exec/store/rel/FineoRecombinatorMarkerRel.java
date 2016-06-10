package io.fineo.read.drill.exec.store.rel;

import io.fineo.read.drill.exec.store.rel.logical.FineoRecombinatorRel;
import io.fineo.read.drill.exec.store.rel.logical.FineoRecombinatorRule;
import io.fineo.schema.store.SchemaStore;
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
  private final SchemaStore store;
  private final RelOptTable parent;
  private final String orgId;
  private final String metricType;
  private List<RelNode> inputs;

  public FineoRecombinatorMarkerRel(RelOptCluster cluster, RelTraitSet traits, SchemaStore store,
    RelOptTable parent, String orgId, String metricType) {
    super(cluster, traits.plus(Convention.NONE));
    this.store = store;
    this.parent = parent;
    this.orgId = orgId;
    this.metricType = metricType;
  }

  @Override
  protected RelDataType deriveRowType() {
    return parent.getRowType();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    FineoRecombinatorMarkerRel rel =
      new FineoRecombinatorMarkerRel(this.getCluster(), traitSet, this.store, this.parent, orgId,
        metricType);
    rel.setInputs(inputs);
    return rel;
  }

  public SchemaStore getStore() {
    return store;
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

  public String getOrgId() {
    return orgId;
  }

  public String getMetricType() {
    return metricType;
  }
}
