package io.fineo.read.drill;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.base.Joiner;
import io.fineo.drill.rule.DrillClusterRule;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.dynamo.DynamoTableCreator;
import io.fineo.lambda.dynamo.DynamoTableTimeManager;
import io.fineo.lambda.dynamo.LocalDynamoTestUtil;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.lambda.dynamo.rule.BaseDynamoTableTest;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import io.fineo.test.rule.TestOutput;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.schemarepo.ValidatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class BaseFineoTest extends BaseDynamoTableTest {
  private static final Logger LOG = LoggerFactory.getLogger(BaseFineoTest.class);

  @ClassRule
  public static DrillClusterRule drill = new DrillClusterRule(1);

  @Rule
  public TestOutput folder = new TestOutput(false);

  protected final String org = "orgid1", metrictype = "metricid1", fieldname = "field1";
  private static final String DYNAMO_TABLE_PREFIX = "test-dynamo-client-";
  private static final Joiner AND = Joiner.on(" AND ");

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

    public String getStatement() throws Exception {
      if (statement == null) {
        String from = format(" FROM %s", metrictype);
        String where = wheres == null ? "" : " WHERE " + AND.join(wheres);
        statement = "SELECT *" + from + where + " ORDER BY `timestamp` ASC";
      }
//      return statement;
      return rewriter.rewrite(statement);
    }
  }

  protected String equals(String left, String right) {
    return format("%s = '%s'", left, right);
  }

  protected String verifySelectStar(Verify<ResultSet> verify) throws Exception {
    return verifySelectStar(null, verify);
  }

  protected String verifySelectStar(List<String> wheres, Verify<ResultSet> verify) throws
    Exception {
    QueryRunnable runnable = new QueryRunnable(wheres, verify);
    runAndVerify(runnable);
    return runnable.getStatement();
  }

  protected static void runAndVerify(QueryRunnable runnable) throws Exception {
    String stmt = runnable.getStatement();
    LOG.info("Attempting query: " + stmt);
    Connection conn = drill.getConnection();
    runnable.prepare(conn);
    if (runnable.verify != null) {
      runnable.verify.verify(conn.createStatement().executeQuery(stmt));
    }
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
    // setup the schema repository
    DynamoDBRepository repository =
      new DynamoDBRepository(ValidatorFactory.EMPTY, tables.getAsyncClient(),
        FineoTestUtil.getCreateTable(tables.getTestTableName()));
    SchemaStore store = new SchemaStore(repository);

    // create a simple schema and store it
    StoreManager manager = new StoreManager(store);
    StoreManager.OrganizationBuilder builder = manager.newOrg(org);

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

    StoreClerk clerk = new StoreClerk(store, org);
    StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metrictype);
    return new TestState(metric.getUnderlyingMetric(), store);
  }

  protected class TestState {
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

    public Table write(Item wrote) {
      DynamoDB dynamo = new DynamoDB(tables.getAsyncClient());
      if (creator == null) {
        DynamoTableTimeManager ttm = new DynamoTableTimeManager(tables.getAsyncClient(),
          DYNAMO_TABLE_PREFIX);
        this.creator = new DynamoTableCreator(ttm, dynamo, 1, 1);
      }

      long ts = wrote.getLong(Schema.SORT_KEY_NAME);
      String name = creator.getTableAndEnsureExists(ts);
      Table table = dynamo.getTable(name);
      table.putItem(wrote);
      return table;
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
}
