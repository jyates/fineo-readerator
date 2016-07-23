package io.fineo.read.drill.exec.store.schema;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePluginConfig;
import io.fineo.read.drill.exec.store.plugin.source.SourceTable;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.schemarepo.ValidatorFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.collect.ImmutableList.of;

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

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    FineoStoragePluginConfig config = (FineoStoragePluginConfig) this.plugin.getConfig();
    this.store = createSchemaStore(config);
    // we need a 'parent' schema, even if it doesn't have any sub-tables
    parent = parent.add("fineo", new FineoBaseSchema(of(), "fineo") {
    });

    List<SourceTable> sources = new ArrayList<>();
    sources.addAll(config.getFsSources());
    sources.addAll(config.getDynamoSources());
    List<String> parentName = of("fineo");
    for (String org : orgs) {
      SubTableScanBuilder scanner = new SubTableScanBuilder(org, sources, plugin.getDynamo());
      parent.add(org, new FineoSchema(parentName, org, this.plugin, scanner, store));
    }
  }

  private SchemaStore createSchemaStore(FineoStoragePluginConfig config) {
    String schemaTable = config.getRepository().getTable();
    AmazonDynamoDBAsyncClient dynamo = plugin.getDynamoClient();
    DynamoDBRepository schemaRepo = new DynamoDBRepository(ValidatorFactory.EMPTY, dynamo,
      DynamoDBRepository.getBaseTableCreate(schemaTable));
    return new SchemaStore(schemaRepo);
  }
}
