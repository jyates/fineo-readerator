package io.fineo.drill.exec.store.dynamo.spec.sub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.fineo.drill.exec.store.dynamo.spec.DynamoReadFilterSpec;
import org.apache.drill.common.expression.SchemaPath;

import java.util.List;

@JsonTypeName("dynamo-sub-query-spec")
public class DynamoSubQuerySpec extends DynamoSubReadSpec {

  @JsonCreator
  public DynamoSubQuerySpec(@JsonProperty("filter") DynamoReadFilterSpec filter,
    @JsonProperty("columns") List<SchemaPath> columns) {
    super(filter, columns);
  }
}
