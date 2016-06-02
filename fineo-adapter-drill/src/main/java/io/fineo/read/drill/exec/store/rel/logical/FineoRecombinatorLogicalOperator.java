package io.fineo.read.drill.exec.store.rel.logical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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

  private Map<String, List<String>> cnameToAlias;

  @JsonCreator
  public FineoRecombinatorLogicalOperator(
    @JsonProperty("cnameMap") Map<String, List<String>> cnameToAlias) {
    this.cnameToAlias = cnameToAlias;
    if (cnameToAlias == null || cnameToAlias.size() == 0) {
      throw new IllegalArgumentException(
        "No canonical name -> alias mapping provided. Expect at least one field to be mapped");
    }
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
