package io.fineo.read.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Marker interface for Fineo relational conventions
 */
public interface FineoRel extends RelNode {

  /** Calling convention for relational operations that occur in MongoDB. */
  Convention CONVENTION = new Convention.Impl("FINEO", FineoRel.class);
}
