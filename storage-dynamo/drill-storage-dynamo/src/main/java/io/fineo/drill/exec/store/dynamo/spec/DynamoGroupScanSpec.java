package io.fineo.drill.exec.store.dynamo.spec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.ArrayList;
import java.util.List;

@JsonTypeName(DynamoGroupScanSpec.NAME)
public class DynamoGroupScanSpec {
  public static final String NAME = "dynamo-group-scan-spec";

  private DynamoTableDefinition table;
  private final DynamoReadFilterSpec scan;
  private final List<DynamoReadFilterSpec> getOrQuery;

  @JsonCreator
  public DynamoGroupScanSpec(@JsonProperty("table") DynamoTableDefinition table,
    @JsonProperty("scan") DynamoReadFilterSpec scan,
    @JsonProperty("getOrQuery") List<DynamoReadFilterSpec> getOrQuery) {
    this.scan = scan;
    this.getOrQuery = getOrQuery;
    this.table = table;
  }

  public DynamoGroupScanSpec() {
    this.getOrQuery = null;
    this.scan = null;
  }

  @JsonProperty
  public DynamoTableDefinition getTable() {
    return table;
  }

  @JsonProperty
  public DynamoReadFilterSpec getScan() {
    return scan;
  }

  @JsonProperty
  public List<DynamoReadFilterSpec> getGetOrQuery() {
    return getOrQuery;
  }

  public void setTable(DynamoTableDefinition table) {
    this.table = table;
  }

  @Override
  public String toString() {
    return "DynamoGroupScanSpec{" +
           "table=" + table +
           ", scan=" + scan +
           ", getOrQuery=" + getOrQuery +
           '}';
  }
}
