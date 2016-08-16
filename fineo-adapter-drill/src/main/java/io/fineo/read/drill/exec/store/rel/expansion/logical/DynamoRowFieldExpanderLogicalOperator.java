package io.fineo.read.drill.exec.store.rel.expansion.logical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.fineo.internal.customer.Metric;
import io.fineo.read.drill.exec.store.rel.MetricUtils;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.SourceType;
import org.apache.drill.common.logical.data.SingleInputOperator;
import org.apache.drill.common.logical.data.visitors.AbstractLogicalVisitor;
import org.apache.drill.common.logical.data.visitors.LogicalVisitor;
import org.apache.htrace.fasterxml.jackson.annotation.JsonGetter;
import org.apache.htrace.fasterxml.jackson.annotation.JsonTypeName;

import java.io.IOException;

/**
 * The logical representation of a recombinator. Used to distribute the plan
 */
@JsonTypeName("dynamo-expander")
public class DynamoRowFieldExpanderLogicalOperator extends SingleInputOperator {

  @JsonCreator
  public DynamoRowFieldExpanderLogicalOperator() {}

  @Override
  public <T, X, E extends Throwable> T accept(LogicalVisitor<T, X, E> logicalVisitor, X x)
    throws E {
    if (logicalVisitor instanceof AbstractLogicalVisitor) {
      ((AbstractLogicalVisitor<T, X, E>) logicalVisitor).visitOp(this, x);
    }
    throw new UnsupportedOperationException(
      "Cannot add operation if its not an AbstractLogicalVisitor b/c we need to add a generic op");
  }
}
