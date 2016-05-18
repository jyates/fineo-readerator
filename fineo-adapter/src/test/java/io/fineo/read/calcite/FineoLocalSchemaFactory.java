package io.fineo.read.calcite;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import io.fineo.aws.rule.AwsCredentialResource;

import java.util.Map;

/**
 * Schema factory that points to a local dynamo instance
 */
public class FineoLocalSchemaFactory extends FineoSchemaFactory {

  @Override
  public AmazonDynamoDBAsyncClient getDynamoDBClient(Map<String, Object> operand) {
    Map<String, String> dynamo = (Map<String, String>) operand.get("dynamo");
    String url = dynamo.get("url");
    AwsCredentialResource credentials = new AwsCredentialResource();
    AmazonDynamoDBAsyncClient client = new AmazonDynamoDBAsyncClient(credentials.getFakeProvider());
    client.setEndpoint(url);
    return client;
  }
}
