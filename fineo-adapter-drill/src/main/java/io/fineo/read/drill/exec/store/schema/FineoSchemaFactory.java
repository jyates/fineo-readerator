package io.fineo.read.drill.exec.store.schema;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import io.fineo.read.drill.FineoInternalProperties;
import io.fineo.read.drill.exec.store.ischema.FineoInfoSchemaUserFilters;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePluginConfig;
import io.fineo.read.drill.exec.store.plugin.OrgLoader;
import io.fineo.read.drill.exec.store.plugin.SchemaRepositoryConfig;
import io.fineo.read.drill.exec.store.plugin.source.SourceTable;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.schemarepo.CacheRepository;
import org.schemarepo.InMemoryCache;
import org.schemarepo.ValidatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.of;

public class FineoSchemaFactory implements SchemaFactory {

  private static final Logger LOG = LoggerFactory.getLogger(FineoSchemaFactory.class);

  protected final FineoStoragePlugin plugin;
  protected final String name;
  private final OrgLoader orgs;
  private SchemaStore store;
  private FineoStoragePluginConfig prevConfig;

  public FineoSchemaFactory(FineoStoragePlugin fineoStoragePlugin, String name, OrgLoader
    orgs) {
    this.plugin = fineoStoragePlugin;
    this.name = name;
    this.orgs = orgs;
  }

  public SchemaPlus registerSchemasWithNewParent(SchemaConfig schemaConfig, SchemaPlus parent)
    throws IOException {
    // we need a 'parent' schema, even if it doesn't have any sub-tables
    parent = parent.add(FineoInternalProperties.FINEO_DRILL_SCHEMA_NAME, new FineoBaseSchema(of(),
      FineoInternalProperties.FINEO_DRILL_SCHEMA_NAME) {
    });
    this.registerSchemas(schemaConfig, parent);
    return parent;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    Stopwatch timer = Stopwatch.createStarted();
    int total = 0;
    try {
      FineoStoragePluginConfig config = (FineoStoragePluginConfig) this.plugin.getConfig();
      this.store = getSchemaStore(config);

      // add the non-null sources
      Set<SourceTable> sources = new HashSet<>();
      Arrays.asList(config.getFsSources(), config.getDynamoSources()).stream()
            .filter(list -> list != null)
            .forEach(list -> sources.addAll(list));
      if (sources.isEmpty()) {
        LOG.error("No sources specified in schema - skipping adding children schemas!");
        return;
      }

      List<String> parentName = of(FineoInternalProperties.FINEO_DRILL_SCHEMA_NAME);

      // if there are no orgs, or things are really borked and we can't get a connection this fails.
      // this should be bad and we should know right away if things are wrong.
      String username = schemaConfig.getUserName();
      boolean superUser = username.equals(FineoInfoSchemaUserFilters.FINEO_HIDDEN_USER_NAME);
      LOG.trace("User: {}. Type: {}", username, superUser? "super" :"regular");
      for (String org : orgs) {
        // skip schemas that don't match the current user name (and are not the super user)
        if(!superUser && !org.matches(schemaConfig.getUserName())){
          LOG.trace("Skipping registering org: {}", org);
          continue;
        }
        SubTableScanBuilder scanner = new SubTableScanBuilder(org, sources, plugin.getDynamo());
        LOG.trace("Registering schemas for: {}", org);
        parent.add(org, new FineoSchema(parentName, org, this.plugin, scanner, store));
      }
    } finally {
      LOG.debug("FineoSchemaFactory.registerSchemas() took {} ms, numSchemas: {}",
        timer.elapsed(TimeUnit.MILLISECONDS), total);
    }
  }

  private SchemaStore getSchemaStore(FineoStoragePluginConfig config) {
    // first time creating the store
    if (this.prevConfig == null || this.store == null) {
      return setStore(config);
    }

    // check to see if the schema store config has changed
    SchemaRepositoryConfig schemaConfig = config.getRepository();
    if (schemaConfig.equals(this.prevConfig.getRepository())) {
      return this.store;
    }

    return setStore(config);
  }

  private SchemaStore setStore(FineoStoragePluginConfig config) {
    LOG.debug("Creating a new schema store!");
    this.prevConfig = config;
    this.store = createSchemaStore(config);
    return this.store;
  }

  private SchemaStore createSchemaStore(FineoStoragePluginConfig config){
    AmazonDynamoDBAsyncClient dynamo = plugin.getDynamoClient();
    return createSchemaStore(config, dynamo);
  }

  @VisibleForTesting
  static SchemaStore createSchemaStore(FineoStoragePluginConfig config,
    AmazonDynamoDBClient dynamo) {
    String schemaTable = config.getRepository().getTable();
    DynamoDBRepository schemaRepo = new DynamoDBRepository(ValidatorFactory.EMPTY, dynamo,
      schemaTable);
    // wrap with a light caching repository
    CacheRepository cache = new CacheRepository(schemaRepo, new InMemoryCache());
    return new SchemaStore(cache);
  }
}
