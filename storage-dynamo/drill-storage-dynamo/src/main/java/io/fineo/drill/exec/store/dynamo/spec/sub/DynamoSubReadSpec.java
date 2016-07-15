package io.fineo.drill.exec.store.dynamo.spec.sub;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.fineo.drill.exec.store.dynamo.spec.DynamoReadFilterSpec;
import org.apache.drill.common.expression.SchemaPath;

import java.util.List;


public abstract class DynamoSubReadSpec {
  private final List<SchemaPath> columns;
  private final DynamoReadFilterSpec filter;

  public DynamoSubReadSpec(DynamoReadFilterSpec filter, List<SchemaPath> columns) {
    this.filter = filter;
    this.columns = columns;
  }

  @JsonProperty
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty
  public DynamoReadFilterSpec getFilter() {
    return filter;
  }
}
