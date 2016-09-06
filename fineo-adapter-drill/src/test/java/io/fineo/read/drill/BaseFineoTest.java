package io.fineo.read.drill;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.google.common.base.Joiner;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.fineo.drill.ClusterTest;
import io.fineo.drill.rule.DrillClusterRule;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.configure.util.InstanceToNamed;
import io.fineo.lambda.dynamo.DynamoTableCreator;
import io.fineo.lambda.dynamo.DynamoTableTimeManager;
import io.fineo.lambda.dynamo.LocalDynamoTestUtil;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.lambda.dynamo.rule.BaseDynamoTableTest;
import io.fineo.lambda.handle.schema.SchemaStoreModuleForTesting;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;
import io.fineo.read.drill.exec.store.dynamo.DynamoTranslator;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.AvroSchemaProperties;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import io.fineo.test.rule.TestOutput;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;

@Category(ClusterTest.class)
public class BaseFineoTest extends BaseDynamoTableTest {
  private static final Logger LOG = LoggerFactory.getLogger(BaseFineoTest.class);

  @ClassRule
  public static DrillClusterRule drill = new DrillClusterRule(1);

  @Rule
  public TestOutput folder = new TestOutput(false);

  protected final String org = "orgid1", metrictype = "metricid1", fieldname = "field1";
  private static final String DYNAMO_TABLE_PREFIX = "test-dynamo-client-";

  @BeforeClass
  public static void prepareCluster() throws Exception {
    FineoDrillStartupSetup setup = new FineoDrillStartupSetup(drill.getConnection());
    setup.run();
  }

  @FunctionalInterface
  protected interface Verify<T> {
    void verify(T obj) throws SQLException;
  }

  protected class QueryRunnable {
    private FineoSqlRewriter rewriter = new FineoSqlRewriter(org);
    List<String> wheres;
    Verify<ResultSet> verify;
    boolean withUnion = true;
    private String statement;
    private List<String> select = new ArrayList<>();
    private List<String> sorts = newArrayList("`timestamp` ASC");

    public QueryRunnable(Verify<ResultSet> verify) {
      this(null, verify);
    }

    public QueryRunnable(List<String> wheres, Verify<ResultSet> verify) {
      this.wheres = wheres;
      this.verify = verify;
    }

    public void prepare(Connection conn) throws SQLException {
      if (withUnion) {
        conn.createStatement().execute("ALTER SESSION SET `exec.enable_union_type` = true");
      }
    }

    public QueryRunnable select(String... elems) {
      select.addAll(asList(elems));
      return this;
    }

    public String getStatement() throws Exception {
      if (statement == null) {
        String from = format(" FROM %s", metrictype);
        String where = wheres == null || wheres.size() == 0 ? "" :
                       " WHERE " + join(" AND ", wheres);
        String fields = select.size() == 0 ? "*" : join(",", select);
        statement =
          format("SELECT %s %s %s ORDER BY %s", fields, from, where, join(" , ", sorts));
      }
      return rewriter.rewrite(statement);
    }

    public QueryRunnable sortBy(String field) {
      this.sorts.add(format("`%s` ASC", field));
      return this;
    }

    private String join(String joiner, Collection<?> parts) {
      return Joiner.on(joiner).join(parts);
    }
  }

  protected String equals(String left, String right) {
    return format("%s = '%s'", left, right);
  }

  /**
   * @param verify handle the result verification
   * @return the statement that was run
   * @throws Exception on failure
   */
  protected String verifySelectStar(Verify<ResultSet> verify) throws Exception {
    return verifySelectStar(null, verify);
  }

  protected String verifySelectStar(List<String> wheres, Verify<ResultSet> verify) throws
    Exception {
    QueryRunnable runnable = new QueryRunnable(wheres, verify);
    return runAndVerify(runnable);
  }

  protected static String runAndVerify(QueryRunnable runnable) throws Exception {
    String stmt = runnable.getStatement();
    LOG.info("Attempting query: " + stmt);
    Connection conn = drill.getConnection();
    runnable.prepare(conn);
    if (runnable.verify != null) {
      runnable.verify.verify(conn.createStatement().executeQuery(stmt));
    }
    return stmt;
  }

  protected void bootstrap(FsSourceTable... files) throws IOException {
    BootstrapFineo bootstrap = new BootstrapFineo();
    BootstrapFineo.DrillConfigBuilder builder = basicBootstrap(bootstrap.builder());

    for (FsSourceTable file : files) {
      builder.withLocalSource(file);
    }
    assertTrue("Failed to bootstrap drill!", bootstrap.strap(builder));
  }

  protected BootstrapFineo.DrillConfigBuilder basicBootstrap(
    BootstrapFineo.DrillConfigBuilder builder) {
    LocalDynamoTestUtil util = dynamo.getUtil();
    return builder.withLocalDynamo(util.getUrl())
                  .withRepository(tables.getTestTableName())
                  .withOrgs(org)
                  .withCredentials(dynamo.getCredentials().getFakeProvider());
  }

  protected TestState register(Pair<String, StoreManager.Type>... fields)
    throws IOException, OldSchemaException {
    // create a simple schema and store it
    SchemaStore store = createDynamoSchemaStore();
    registerSchema(store, true, fields);

    StoreClerk clerk = new StoreClerk(store, org);
    StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metrictype);
    return new TestState(metric.getUnderlyingMetric(), store);
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
      .namedInstance(SchemaStoreModule.DYNAMO_SCHEMA_STORE_TABLE, tables.getTestTableName()));
    return inject.getInstance(SchemaStore.class);
  }

  protected class TestState {
    private final DynamoTranslator trans = new DynamoTranslator();
    Metric metric;
    SchemaStore store;
    private DynamoTableCreator creator;

    public TestState(Metric metric, SchemaStore store) {
      this.metric = metric;
      this.store = store;
    }

    public FsSourceTable writeParquet(File dir, long ts, Map<String, Object>... values)
      throws Exception {
      return BaseFineoTest.this.writeParquet(this, dir, org, metrictype, ts, values).getKey();
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

    public Item translate(Map<String, Object> item) throws Exception {
      return trans.apply(item);
    }

    public Table write(Map<String, Object> item) throws Exception {
      return write(translate(item));
    }

    public Table write(AmazonDynamoDBAsyncClient client, Map<String, Object> item) throws
      Exception {
      Table table = getAndEnsureTable((Long) item.get(AvroSchemaProperties.TIMESTAMP_KEY));
      trans.write(client, creator, store, item);
      return table;
    }

    public void update(Table table, Map<String, Object> item)
      throws UnsupportedEncodingException, NoSuchAlgorithmException {
      UpdateItemSpec spec = trans.updateItem(item);
      table.updateItem(spec);
    }

    public Table write(Item wrote) {
      long ts = wrote.getLong(Schema.SORT_KEY_NAME);
      Table table = getAndEnsureTable(ts);
      table.putItem(wrote);
      return table;
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

    public Metric getMetric() {
      return metric;
    }

    public SchemaStore getStore() {
      return store;
    }
  }

  protected Pair<FsSourceTable, File> writeParquet(TestState state, File dir, String orgid,
    String metricType, long ts, Map<String, Object>... rows) throws Exception {
    return FineoTestUtil
      .writeParquet(state, drill.getConnection(), dir, orgid, metricType, ts, rows);
  }

  protected Map<String, Object> prepareItem() throws SchemaNotFoundException {
    Map<String, Object> wrote = new HashMap<>();
    wrote.put(AvroSchemaProperties.ORG_ID_KEY, org);
    wrote.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metrictype);
    return wrote;
  }
}
