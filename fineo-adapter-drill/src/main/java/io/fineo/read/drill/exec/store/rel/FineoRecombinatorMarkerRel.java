package io.fineo.read.drill.exec.store.rel;

import io.fineo.read.drill.exec.store.rel.logical.FineoRecombinatorRel;
import io.fineo.read.drill.exec.store.rel.logical.FineoRecombinatorRule;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * Marker relation that gets converted into a {@link FineoRecombinatorRel} via the
 * {@link FineoRecombinatorRule} since we cannot know at creation time what the projected and
 * filtered columns actually are.
 */
public class FineoRecombinatorMarkerRel extends SingleRel {
  private final RelDataType row;
  private final SchemaStore store;

  public FineoRecombinatorMarkerRel(SchemaStore store, RelNode subscans, RelDataType rowType) {
    super(subscans.getCluster(), subscans.getTraitSet().plus(Convention.NONE), subscans);
    this.row = rowType;
    this.store = store;
  }

  @Override
  protected RelDataType deriveRowType() {
    return this.row;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FineoRecombinatorMarkerRel(store, inputs.get(0), getRowType());
  }

  public SchemaStore getStore() {
    return store;
  }
}
