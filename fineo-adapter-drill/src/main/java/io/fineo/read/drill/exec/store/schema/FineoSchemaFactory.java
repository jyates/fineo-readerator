package io.fineo.read.drill.exec.store.schema;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.google.common.collect.ImmutableList;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePluginConfig;
import io.fineo.read.drill.exec.store.plugin.SourceFsTable;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.schemarepo.ValidatorFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;
import static jersey.repackaged.com.google.common.collect.ImmutableList.of;


public class FineoSchemaFactory implements SchemaFactory {

  protected final FineoStoragePlugin plugin;
  protected final String name;
  private final Collection<String> orgs;
  private SchemaStore store;

  public FineoSchemaFactory(FineoStoragePlugin fineoStoragePlugin, String name, Collection<String>
    orgs) {
    this.plugin = fineoStoragePlugin;
    this.name = name;
    this.orgs = orgs;
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
    // we need a 'parent' schema, even if it doesn't have any sub-tables
    parent = parent.add("fineo", new FineoBaseSchema(of(), "fineo") {
    });

    List<String> parentName = ImmutableList.of("fineo");
    for (String org : orgs) {
      List<SourceFsTable> sources = sub.getSchemas(org);
      SubTableScanBuilder scanner = new SubTableScanBuilder(sources);
      parent.add(org, new FineoSchema(parentName, org, this.plugin, scanner, store));
    }
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
