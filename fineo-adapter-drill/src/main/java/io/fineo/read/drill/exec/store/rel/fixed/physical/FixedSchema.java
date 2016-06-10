package io.fineo.read.drill.exec.store.rel.fixed.physical;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.Project;

import java.util.List;

@JsonTypeName("fineo-project")
public class FixedSchema extends Project {
  public FixedSchema(@JsonProperty("exprs") List<NamedExpression> exprs,
    @JsonProperty("child") PhysicalOperator child) {
    super(exprs, child);
  }
}
