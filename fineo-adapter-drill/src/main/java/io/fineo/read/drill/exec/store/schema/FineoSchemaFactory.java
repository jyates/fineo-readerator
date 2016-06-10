package io.fineo.read.drill.exec.store.schema;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.google.common.collect.ImmutableList;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePluginConfig;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.schemarepo.ValidatorFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;


public class FineoSchemaFactory implements SchemaFactory {

  protected final FineoStoragePlugin plugin;
  protected final String name;
  private SchemaStore store;
  private boolean set = false;

  public FineoSchemaFactory(FineoStoragePlugin fineoStoragePlugin, String name) {
    this.plugin = fineoStoragePlugin;
    this.name = name;
  }

  public enum CredentialProvider {
    Default(() -> new DefaultAWSCredentialsProviderChain());

    private final Supplier<AWSCredentialsProvider> creds;

    CredentialProvider(Supplier<AWSCredentialsProvider> creds) {
      this.creds = creds;
    }
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    FineoStoragePluginConfig config = (FineoStoragePluginConfig) this.plugin.getConfig();
    this.store = createSchemaStore(config);
    FineoSubSchemas sub = new FineoSubSchemas(config.getSources());
    SchemaPlus next = parent.add(name, new FineoSchema(this.name, this.plugin, sub, store));
    next.add("sub", new FineoSchema(ImmutableList.of("fineo"), "sub", this.plugin, sub, store));
    if (set) {
      next.add("sub2", new FineoSchema(ImmutableList.of("fineo"), "sub2", this.plugin, sub, store));
    }
    this.set = true;

  }

  protected SchemaStore createSchemaStore(FineoStoragePluginConfig config) {
    AmazonDynamoDBAsyncClient dynamo = getDynamoDBClient(config);
    String schemaTable = config.getRepository().get("table");
    DynamoDBRepository schemaRepo = new DynamoDBRepository(ValidatorFactory.EMPTY, dynamo,
      DynamoDBRepository.getBaseTableCreate(schemaTable));
    return new SchemaStore(schemaRepo);
  }

  protected AmazonDynamoDBAsyncClient getDynamoDBClient(FineoStoragePluginConfig config) {
    String provider = config.getAws().get("provider");
    CredentialProvider credentials = CredentialProvider.valueOf(provider);
    checkNotNull("No valid credential provider of type: %s. Valid credential types are: %s",
      provider,
      Arrays.toString(CredentialProvider.values()));

    // create the schema repository
    String region = config.getAws().get("region");
    AmazonDynamoDBAsyncClient dynamo = new AmazonDynamoDBAsyncClient(credentials.creds.get());
    dynamo.setRegion(Region.getRegion(Regions.fromName(region)));
    return dynamo;
  }

  public SchemaStore getStore() {
    return store;
  }
}
