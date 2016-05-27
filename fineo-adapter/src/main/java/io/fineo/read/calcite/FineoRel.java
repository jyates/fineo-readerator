package io.fineo.read.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public interface FineoRel extends RelNode {

  /** Calling convention for relational operations that occur in MongoDB. */
  Convention CONVENTION = new Convention.Impl("FINEO", FineoRel.class);

  /** Callback for the implementation process that converts a tree of
   * {@link FineoRel} nodes into a MongoDB query. */
  class Implementor {
    final List<Pair<String, String>> list =
      new ArrayList<Pair<String, String>>();

    RelOptTable table;
    FineoTable mongoTable;

    public void add(String findOp, String aggOp) {
      list.add(Pair.of(findOp, aggOp));
    }

    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((FineoRel) input).implement(this);
    }
  }

  void implement(Implementor implementor);
}
