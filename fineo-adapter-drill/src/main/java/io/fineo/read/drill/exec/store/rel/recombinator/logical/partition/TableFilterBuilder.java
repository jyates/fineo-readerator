package io.fineo.read.drill.exec.store.rel.recombinator.logical.partition;

import io.fineo.drill.exec.store.dynamo.filter.SingleFunctionProcessor;
import org.apache.calcite.rex.RexNode;

public interface TableFilterBuilder {

  RexNode replaceTimestamp(SingleFunctionProcessor processor);
  String getFilterFieldName();
}
