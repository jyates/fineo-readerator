package io.fineo.read.drill.exec.store.rel.logical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.fineo.internal.customer.Metric;
import org.apache.drill.common.logical.data.SingleInputOperator;
import org.apache.drill.common.logical.data.visitors.AbstractLogicalVisitor;
import org.apache.drill.common.logical.data.visitors.LogicalVisitor;
import org.apache.htrace.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;
import java.util.Map;

/**
 * The logical representation of a recombinator. Used to distribute the plan
 */
@JsonTypeName("fineo-recombinator")
public class FineoRecombinatorLogicalOperator extends SingleInputOperator {

  private final Metric metric;
  // managed by JSON ser/de and used for mapping fragments
  private Map<String, List<String>> cnameToAlias;

  @JsonCreator
  public FineoRecombinatorLogicalOperator(
    @JsonProperty("cnameMap") Metric metric) {
    this.metric = metric;
    Preconditions.checkNotNull(metric, "No metric found for mapping!");
  }

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
