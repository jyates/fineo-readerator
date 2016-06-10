package io.fineo.read.drill.exec.store.rel.fixed.physical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Project;
import org.apache.drill.common.logical.data.SingleInputOperator;
import org.apache.drill.common.logical.data.visitors.AbstractLogicalVisitor;
import org.apache.drill.common.logical.data.visitors.LogicalVisitor;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;


@JsonTypeName("fineo-fixed-project")
public class FixedSchemaOperator extends SingleInputOperator {

  private final NamedExpression[] selections;

  public FixedSchemaOperator(List<NamedExpression> exprs) {
    this.selections = exprs.toArray(new NamedExpression[0]);
  }

  @JsonCreator
  public FixedSchemaOperator(@JsonProperty("projections") NamedExpression[] selections) {
    this(newArrayList(selections));
  }

  @JsonProperty("projections")
  public NamedExpression[] getSelections() {
    return this.selections;
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
