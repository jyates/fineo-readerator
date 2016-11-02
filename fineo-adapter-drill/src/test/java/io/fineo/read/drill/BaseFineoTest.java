package io.fineo.read.drill;

import com.google.common.base.Joiner;
import io.fineo.drill.ClusterTest;
import io.fineo.drill.rule.DrillClusterRule;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;

@Category(ClusterTest.class)
public class BaseFineoTest extends BaseFineoDynamoTest {
  private static final Logger LOG = LoggerFactory.getLogger(BaseFineoTest.class);

  @ClassRule
  public static DrillClusterRule drill = new DrillClusterRule(1);

  @BeforeClass
  public static void prepareCluster() throws Exception {
    FineoDrillStartupSetup setup = new FineoDrillStartupSetup(drill.getConnection());
    setup.run();
  }

  @FunctionalInterface
  public interface Verify<T> {
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
    BootstrapFineo bootstrap = newBootstrap();
    BootstrapFineo.DrillConfigBuilder builder = basicBootstrap(bootstrap.builder());

    for (FsSourceTable file : files) {
      builder.withLocalSource(file);
    }
    assertTrue("Failed to bootstrap drill!", bootstrap.strap(builder));
  }

  public FsSourceTable writeParquet(TestState state, File dir, long ts,
    Map<String, Object>... values) throws Exception {
    return writeParquet(state, dir, org, metrictype, ts, values).getKey();
  }

  protected Pair<FsSourceTable, File> writeParquet(TestState state, File dir, String orgid,
    String metricType, long ts, Map<String, Object>... rows) throws Exception {
    return FineoTestUtil
      .writeParquet(state, drill.getConnection(), dir, orgid, metricType, ts, rows);
  }

  protected BootstrapFineo newBootstrap() {
    return newBootstrap(drill);
  }

  public static BootstrapFineo newBootstrap(DrillClusterRule drill) {
    return new BootstrapFineo(drill.getWebPort());
  }

  protected BootstrapFineo.DrillConfigBuilder bootstrapper() {
   return basicBootstrap(newBootstrap(drill).builder());
  }
}
