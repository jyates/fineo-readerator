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
import java.util.Properties;

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

  public QueryRunnable selectStarForOrg(String org, Verify<ResultSet> verify) {
    QueryRunnable runnable = new QueryRunnable(verify).withUser(org);
    runnable.rewriter = new FineoSqlRewriter(org);
    return runnable;
  }

  protected class QueryRunnable {
    private FineoSqlRewriter rewriter = new FineoSqlRewriter(org);
    List<String> wheres;
    Verify<ResultSet> verify;
    public boolean withUnion = true;
    private String statement;
    private List<String> select = new ArrayList<>();
    public List<String> sorts = newArrayList("`timestamp` ASC");
    public boolean rewrite = true;
    private String from = null;
    private String user = org;

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
        String from = format(" FROM %s", this.from != null ? this.from : metrictype);
        String where = wheres == null || wheres.size() == 0 ? "" :
                       " WHERE " + join(" AND ", paren(wheres));
        String fields = select.size() == 0 ? "*" : join(",", select);
        statement =
          format("SELECT %s %s %s %s", fields, from, where,
            sorts.isEmpty() ? "" : format("ORDER BY %s", join(" , ", sorts)));
      }
      if (rewrite) {
        statement = rewriter.rewrite(statement);
      }
      return this.statement;
    }

    public QueryRunnable from(String table) {
      this.from = table;
      return this;
    }

    public QueryRunnable sortBy(String field) {
      this.sorts.add(format("`%s` ASC", field));
      return this;
    }

    private String join(String joiner, Collection<?> parts) {
      return Joiner.on(joiner).join(parts);
    }

    public QueryRunnable withUser(String user) {
      this.user = user;
      return this;
    }

    public Properties getProperties() {
      Properties props = new Properties();
      props.put("user", this.user);
      return props;
    }
  }

  private List<String> paren(List<String> parts) {
    List<String> ret = new ArrayList<>();
    for (int i = 0; i < parts.size(); i++) {
      String part = parts.get(i);
      if (part.startsWith("(") && part.endsWith(")")) {
        continue;
      }
      ret.add(format("(%s)", part));
    }
    return parts;
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
    String query = runnable.getStatement();
    LOG.info("Attempting query: " + query);
    Properties props = runnable.getProperties();
    try (Connection conn = drill.getUnmanagedConnection(props)) {
      runnable.prepare(conn);
      if (runnable.verify != null) {
        runnable.verify.verify(conn.createStatement().executeQuery(query));
      }
      return query;
    }
  }

  protected void bootstrap(FsSourceTable... files) throws IOException {
    BootstrapFineo bootstrap = newBootstrap();
    BootstrapFineo.DrillConfigBuilder builder = simpleBootstrap(bootstrap.builder());

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

  protected static BootstrapFineo newBootstrap() {
    return newBootstrap(drill);
  }

  public static BootstrapFineo newBootstrap(DrillClusterRule drill) {
    return new BootstrapFineo(drill.getWebPort());
  }

  protected BootstrapFineo.DrillConfigBuilder bootstrapper() {
    return simpleBootstrap(newBootstrap(drill).builder());
  }

  protected void validateAsOrgUser(PlanValidator validator) throws Exception {
    QueryRunnable runnable = new QueryRunnable(null);
    Properties props = runnable.getProperties();
    try(Connection conn = drill.getUnmanagedConnection(props)) {
      validator.validate(conn);
    }
  }
}
