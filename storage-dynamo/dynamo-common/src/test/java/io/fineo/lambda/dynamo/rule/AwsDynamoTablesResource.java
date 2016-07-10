package io.fineo.lambda.dynamo.rule;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import io.fineo.lambda.dynamo.LocalDynamoTestUtil;
import org.junit.rules.ExternalResource;


/**
 * Manage aws tables and getting a connection to them. Generally, this should be used at the
 * {@link org.junit.Rule} level.
 */
public class AwsDynamoTablesResource extends ExternalResource {

  private final AwsDynamoResource dynamoResource;
  private LocalDynamoTestUtil util;
  private AmazonDynamoDBAsyncClient client;

  public AwsDynamoTablesResource(AwsDynamoResource dynamo) {
    this.dynamoResource = dynamo;
  }

  @Override
  protected void after() {
    if (getAsyncClient().listTables().getTableNames().size() == 0) {
      return;
    }

    util.cleanupTables();
    // reset any open clients
    if (client != null) {
      client.shutdown();
      client = null;
    }
  }

  public String getTestTableName() {
    return getUtil().getCurrentTestTable();
  }

  public AmazonDynamoDBAsyncClient getAsyncClient() {
    if (this.client == null) {
      this.client = getUtil().getAsyncClient();
    }
    return this.client;
  }

  private LocalDynamoTestUtil getUtil() {
    if (this.util == null) {
      this.util = dynamoResource.getUtil();
    }
    return this.util;
  }
}
