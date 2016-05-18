package io.fineo.read.calcite;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.schemarepo.ValidatorFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *
 */
public class FineoSchemaFactory implements SchemaFactory {
  public enum CredentialProvider {
    Default(() -> new DefaultAWSCredentialsProviderChain());

    private final Supplier<AWSCredentialsProvider> creds;

    private CredentialProvider(Supplier<AWSCredentialsProvider> creds) {
      this.creds = creds;
    }
  }

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> map) {
    AmazonDynamoDBAsyncClient dynamo = getDynamoDBClient(map);
    String schemaTable = getNested(map, "repository.table");
    DynamoDBRepository schemaRepo = new DynamoDBRepository(ValidatorFactory.EMPTY, dynamo,
      DynamoDBRepository.getBaseTableCreate(schemaTable));
    SchemaStore store = new SchemaStore(schemaRepo);
    return new FineoSchema(store);
  }

  public AmazonDynamoDBAsyncClient getDynamoDBClient(Map<String, Object> operand) {
    String provider = (String) operand.get("provider");
    CredentialProvider credentials = CredentialProvider.valueOf(provider);
    checkNotNull("No valid credential provider of type: %s. Valid credential types are: %s",
      provider,
      Arrays.toString(CredentialProvider.values()));

    // create the schema repository
    String region = (String) operand.get("aws.region");
    AmazonDynamoDBAsyncClient dynamo = new AmazonDynamoDBAsyncClient(credentials.creds.get());
    dynamo.setRegion(Region.getRegion(Regions.fromName(region)));
    return dynamo;
  }

  private static String getNested(Map<String, Object> map, String key) {
    String[] parts = key.split("[.]");
    for (int i = 0; i < parts.length; i++) {
      Object o = map.get(parts[i]);
      if (i == parts.length || o instanceof String) {
        return (String) o;
      }
      map = (Map<String, Object>) o;
    }
    throw new IllegalStateException("Should not be reachable");
  }
}
