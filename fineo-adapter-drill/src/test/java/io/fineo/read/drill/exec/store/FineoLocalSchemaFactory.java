package io.fineo.read.drill.exec.store;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePluginConfig;
import io.fineo.read.drill.exec.store.schema.FineoSchemaFactory;

import java.util.Map;

/**
 * Schema factory that points to a local dynamo instance
 */
public class FineoLocalSchemaFactory extends FineoSchemaFactory {

  public FineoLocalSchemaFactory(FineoStoragePlugin fineoStoragePlugin, String name) {
    super(fineoStoragePlugin, name,
      ((FineoStoragePluginConfig) fineoStoragePlugin.getConfig()).getOrgs());
  }

  @Override
  public AmazonDynamoDBAsyncClient getDynamoDBClient(FineoStoragePluginConfig config) {
    FineoLocalTestStoragePluginConfig conf = (FineoLocalTestStoragePluginConfig) config;
    Map<String, String> dynamo = conf.getDynamo();
    String url = dynamo.get("url");
    AwsCredentialResource credentials = new AwsCredentialResource();
    AmazonDynamoDBAsyncClient client = new AmazonDynamoDBAsyncClient(credentials.getFakeProvider());
    client.setEndpoint(url);
    return client;
  }
}
