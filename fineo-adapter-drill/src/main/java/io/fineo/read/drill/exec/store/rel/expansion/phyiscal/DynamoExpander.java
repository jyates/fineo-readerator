package io.fineo.read.drill.exec.store.rel.expansion.phyiscal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fineo.read.drill.FineoInternalProperties;
import org.apache.drill.exec.physical.base.AbstractSingle;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;

@JsonTypeName("dynamo-expander")
@JsonDeserialize
public class DynamoExpander extends AbstractSingle {
  @JsonCreator
  public DynamoExpander(@JsonProperty("child") PhysicalOperator child) {
    super(child);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new DynamoExpander(child);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
    throws E {
    return physicalVisitor.visitOp(this, value);
  }

  @Override
  public int getOperatorType() {
    return FineoInternalProperties.OperatorIndex.DRILL_EXPANDER;
  }
}
