package io.fineo.read.drill;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.fineo.drill.ClusterTest;
import io.fineo.lambda.configure.util.InstanceToNamed;
import io.fineo.lambda.dynamo.DynamoTableCreator;
import io.fineo.lambda.dynamo.DynamoTableTimeManager;
import io.fineo.lambda.dynamo.LocalDynamoTestUtil;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.lambda.dynamo.rule.BaseDynamoTableTest;
import io.fineo.lambda.handle.schema.SchemaStoreModuleForTesting;
import io.fineo.lambda.handle.schema.inject.DynamoDBRepositoryProvider;
import io.fineo.read.drill.exec.store.dynamo.DynamoTranslator;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.AvroSchemaProperties;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreManager;
import io.fineo.test.rule.TestOutput;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;

@Category(ClusterTest.class)
public class BaseFineoDynamoTest extends BaseDynamoTableTest {
  private static final Logger LOG = LoggerFactory.getLogger(BaseFineoDynamoTest.class);
  @Rule
  public TestOutput folder = new TestOutput(false);

  protected final String org = "orgid1", metrictype = "metricid1", fieldname = "field1";
  private static final String DYNAMO_TABLE_PREFIX = "test-dynamo-client-";

  protected TestState register(Pair<String, StoreManager.Type>... fields)
    throws IOException, OldSchemaException {
    // create a simple schema and store it
    SchemaStore store = createDynamoSchemaStore();
    registerSchema(store, true, fields);
    return new TestState(store, tables.getAsyncClient());
  }

  protected void registerSchema(SchemaStore store, boolean newOrg,
    Pair<String, StoreManager.Type>...
      fields) throws IOException, OldSchemaException {
    StoreManager manager = new StoreManager(store);
    StoreManager.OrganizationBuilder builder =
      newOrg ? manager.newOrg(org) : manager.updateOrg(org);

    StoreManager.MetricBuilder mb = builder.newMetric().setDisplayName(metrictype);
    // default just creates a boolean field
    if (fields == null || fields.length == 0) {
      mb.newField().withName(fieldname).withType(StoreManager.Type.BOOLEAN).build();
    } else {
      for (Pair<String, StoreManager.Type> field : fields) {
        mb.newField().withName(field.getKey()).withType(field.getValue()).build();
      }
    }

    mb.build().commit();
  }

  protected SchemaStore createDynamoSchemaStore() {
    // setup the schema repository
    SchemaStoreModuleForTesting module = new SchemaStoreModuleForTesting();
    Injector inject = Guice.createInjector(module, tables.getDynamoModule(), InstanceToNamed
      .namedInstance(DynamoDBRepositoryProvider.DYNAMO_SCHEMA_STORE_TABLE,
        tables.getTestTableName()));
    return inject.getInstance(SchemaStore.class);
  }

  protected class TestState {
    private final DynamoTranslator trans;
    SchemaStore store;
    private DynamoTableCreator creator;

    public TestState(SchemaStore store, AmazonDynamoDBAsyncClient asyncClient) {
      this.store = store;
      this.trans = new DynamoTranslator(asyncClient);
    }

    public FsSourceTable write(File dir, long ts, Map<String, Object>... values)
      throws IOException {
      return write(dir, org, metrictype, ts, values);
    }

    public FsSourceTable write(File dir, String org, String metricType, long ts,
      Map<String, Object>... values) throws IOException {
      return write(dir, org, metricType, ts, newArrayList(values));
    }

    public FsSourceTable write(File dir, String org, String metricType, long ts,
      List<Map<String, Object>> values) throws IOException {
      return FineoTestUtil.writeJson(store, dir, org, metricType, ts, values);
    }

    public Table writeToDynamo(Map<String, Object> item) throws
      Exception {
      // older code used the sort key, so check for that instead
      Long ts = (Long) item.remove(Schema.SORT_KEY_NAME);
      if (ts != null) {
        item.put(AvroSchemaProperties.TIMESTAMP_KEY, ts);
      }
      ts = (Long) item.get(AvroSchemaProperties.TIMESTAMP_KEY);

      Table table = getAndEnsureTable(ts);
      trans.write(creator, store, item);
      return table;
    }

    public void update(Map<String, Object> item)
      throws Exception {
      writeToDynamo(item);
    }

    private Table getAndEnsureTable(long ts) {
      DynamoDB dynamo = new DynamoDB(tables.getAsyncClient());
      if (creator == null) {
        DynamoTableTimeManager ttm = new DynamoTableTimeManager(tables.getAsyncClient(),
          DYNAMO_TABLE_PREFIX);
        this.creator = new DynamoTableCreator(ttm, dynamo, 1, 1);
      }
      String name = creator.getTableAndEnsureExists(ts);
      return dynamo.getTable(name);
    }

    public SchemaStore getStore() {
      return store;
    }
  }

  protected Map<String, Object> prepareItem() throws SchemaNotFoundException {
    Map<String, Object> wrote = new HashMap<>();
    wrote.put(AvroSchemaProperties.ORG_ID_KEY, org);
    wrote.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metrictype);
    return wrote;
  }

  protected BootstrapFineo.DrillConfigBuilder basicBootstrap(
    BootstrapFineo.DrillConfigBuilder builder) {
    LocalDynamoTestUtil util = dynamo.getUtil();
    return builder.withLocalDynamo(util.getUrl())
                  .withRepository(tables.getTestTableName())
                  .withOrgs(org)
                  .withCredentials(dynamo.getCredentials().getFakeProvider());
  }
}
