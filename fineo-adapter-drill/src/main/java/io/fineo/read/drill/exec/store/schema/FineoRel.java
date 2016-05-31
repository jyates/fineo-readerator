package io.fineo.read.drill.exec.store.schema;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

/**
 * Marker interface for Fineo relational conventions
 */
public interface FineoRel extends RelNode {

  /** Calling convention for relational operations that occur in MongoDB. */
  Convention CONVENTION = new Convention.Impl("FINEO", FineoRel.class);
}
