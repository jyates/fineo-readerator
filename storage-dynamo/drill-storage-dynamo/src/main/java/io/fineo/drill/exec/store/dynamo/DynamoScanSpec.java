package io.fineo.drill.exec.store.dynamo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.fineo.drill.exec.store.dynamo.config.ClientProperties;
import io.fineo.drill.exec.store.dynamo.config.ParallelScanProperties;

import java.util.Map;

/**
 * Fully define a scan of the table
 */
@JsonTypeName(DynamoScanSpec.NAME)
@JsonAutoDetect
public class DynamoScanSpec {

  public static final String NAME = "dynamo-scan-spec";

  private DynamoTableDefinition table;
  private ClientProperties client;
  private ParallelScanProperties scan;

  public ClientProperties getClient() {
    return client;
  }

  public ParallelScanProperties getScan() {
    return scan;
  }

  public void setClient(ClientProperties client) {
    this.client = client;
  }

  public void setScan(ParallelScanProperties scan) {
    this.scan = scan;
  }

  public void setTable(DynamoTableDefinition table) {
    this.table = table;
  }

  public DynamoTableDefinition getTable() {
    return this.table;
  }
}
