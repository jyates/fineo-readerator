package io.fineo.drill.exec.store.dynamo;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import io.fineo.drill.exec.store.dynamo.config.CredentialsUtil;
import io.fineo.drill.exec.store.dynamo.config.DynamoEndpoint;
import io.fineo.drill.exec.store.dynamo.config.DynamoStoragePluginConfig;
import io.fineo.drill.exec.store.dynamo.config.StaticCredentialsConfig;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoTablesResource;
import io.fineo.lambda.dynamo.rule.BaseDynamoTableTest;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestDynamoEndToEnd extends BaseTestQuery {

  @ClassRule
  public static AwsDynamoResource dynamo =
    new AwsDynamoResource(BaseDynamoTableTest.STATIC_CREDENTIALS_PROVIDER);
  @Rule
  public AwsDynamoTablesResource tables = new AwsDynamoTablesResource(dynamo);

  private static DynamoStoragePlugin storagePlugin;
  private static DynamoStoragePluginConfig storagePluginConfig;

  @BeforeClass
  public static void setupDefaultTestCluster() throws Exception {
    BaseTestQuery.setupDefaultTestCluster();

    final StoragePluginRegistry pluginRegistry = getDrillbitContext().getStorage();
    storagePlugin = (DynamoStoragePlugin) pluginRegistry.getPlugin(DynamoStoragePlugin.NAME);
    storagePluginConfig = (DynamoStoragePluginConfig) storagePlugin.getConfig();
    storagePluginConfig.setEnabled(true);

    DynamoEndpoint endpoint = new DynamoEndpoint(dynamo.getUtil().getUrl());
    storagePluginConfig.setEndpointForTesting(endpoint);

    Map<String, Object> credentials = new HashMap<>();
    credentials.put(CredentialsUtil.CREDENTIALS_TYPE_KEY, "provided");
    AWSCredentials creds = BaseDynamoTableTest.STATIC_CREDENTIALS_PROVIDER.getCredentials();
    StaticCredentialsConfig credentialsConfig = new StaticCredentialsConfig(creds
      .getAWSAccessKeyId(), creds.getAWSSecretKey());
    credentials.put(StaticCredentialsConfig.NAME, credentialsConfig);
    storagePluginConfig.setCredentialsForTesting(credentials);

    pluginRegistry.createOrUpdate(DynamoStoragePlugin.NAME, storagePluginConfig, true);
  }

  @Test
  public void testSimpleRead() throws Exception {

  }
}
