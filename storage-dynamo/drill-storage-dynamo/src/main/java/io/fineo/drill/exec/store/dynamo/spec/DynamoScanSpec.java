package io.fineo.drill.exec.store.dynamo.spec;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Fully define a scan of the table
 */
@JsonTypeName(DynamoScanSpec.NAME)
@JsonAutoDetect
public class DynamoScanSpec {

  public static final String NAME = "dynamo-scan-spec";

  private DynamoTableDefinition table;
  private DynamoScanFilterSpec filter;

  @JsonCreator
  public DynamoScanSpec(@JsonProperty("table") DynamoTableDefinition table, @JsonProperty
    ("filter") DynamoScanFilterSpec filter) {
    this.table = table;
    this.filter = filter;
  }

  public DynamoScanSpec(){
    this.filter = new DynamoScanFilterSpec();
  }

  public void setTable(DynamoTableDefinition table) {
    this.table = table;
  }

  public DynamoTableDefinition getTable() {
    return this.table;
  }

  public DynamoScanFilterSpec getFilter() {
    return filter;
  }

  public void setFilter(DynamoScanFilterSpec filter) {
    this.filter = filter;
  }

  public DynamoScanSpec(DynamoScanSpec other) {
    this.table = other.table;
    this.filter = other.filter;
  }
}
