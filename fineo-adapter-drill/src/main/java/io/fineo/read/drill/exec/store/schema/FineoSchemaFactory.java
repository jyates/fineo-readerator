package io.fineo.read.drill.exec.store.schema;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import io.fineo.read.drill.exec.store.FineoStoragePlugin;
import io.fineo.read.drill.exec.store.FineoStoragePluginConfig;
import io.fineo.read.drill.exec.store.dynamo.DynamoSchemaFactory;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.schemarepo.ValidatorFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;


public class FineoSchemaFactory implements SchemaFactory {

  public static final String DYNAMO_SCHEMA_NAME = "dynamo";
  protected final FineoStoragePlugin plugin;
  protected final String name;

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
    SchemaStore store = createSchemaStore(config);

    // add each of the child data sources as their own schema
    DynamoSchemaFactory dynamoFactory = new DynamoSchemaFactory();
    Schema dynamoSchema = dynamoFactory.create(parent, DYNAMO_SCHEMA_NAME, null);
    parent = parent.add(DYNAMO_SCHEMA_NAME, dynamoSchema);

    parent.add(name, new FineoSchema(parent, this.name, this.plugin, store, dynamoSchema));
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

  protected static <T> T getNested(Map<String, Object> map, String key) {
    String[] parts = key.split("[.]");
    for (int i = 0; i < parts.length; i++) {
      Object o = map.get(parts[i]);
      if (i == parts.length - 1 || o instanceof String) {
        return (T) o;
      }
      map = (Map<String, Object>) o;
    }
    throw new IllegalStateException("Should not be reachable");
  }
}
