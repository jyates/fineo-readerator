package io.fineo.read.serve.driver;

import io.fineo.read.FineoJdbcProperties;
import io.fineo.read.drill.FineoSqlRewriter;
import io.fineo.read.serve.FineoServer;
import net.hydromatic.scott.data.hsqldb.ScottHsqldb;
import org.apache.calcite.avatica.jdbc.FineoJdbcMeta;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.util.Pair;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.fineo.read.serve.driver.FineoDatabaseMetaData.FINEO_CATALOG;
import static io.fineo.read.serve.driver.FineoDatabaseMetaData.FINEO_SCHEMA;
import static io.fineo.read.serve.driver.FineoDatabaseMetaData.TABLE_CATALOG_COLUMN;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestFineoServerDriver {

  private static final Logger LOG = LoggerFactory.getLogger(TestFineoServerDriver.class);
  private static final String ORG = "orgid";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private List<Connection> connections = new ArrayList<>();
  private List<RunnableWith> cleanup = new ArrayList<>();

  @After
  public void cleanupConnections() {
    boolean failures = false;
    for (RunnableWith r : cleanup) {
      try {
        r.run();
      } catch (Exception e) {
        LOG.error("Failed cleanup step!", e);
        failures = true;
      }
    }
    cleanup.clear();
    for (Connection conn : connections) {
      try {
        conn.close();
      } catch (SQLException e) {
        LOG.error("Failed to close connection!", e);
        failures = true;
      }
    }
    connections.clear();
    assertFalse("Got some failures in cleanup - see logs", failures);
  }

  @Test
  public void testGetConnection() throws Exception {
    getConnection();
  }

  @Test
  public void testGetCatalog() throws Exception {
    Connection conn = getConnection();
    assertOneCatalog(conn);
  }

  static void assertOneCatalog(Connection conn) throws SQLException {
    DatabaseMetaData meta = conn.getMetaData();
    try (ResultSet catalogs = meta.getCatalogs()) {
      assertTrue(catalogs.next());
      assertEquals(FINEO_CATALOG, catalogs.getString(1));
      assertEquals(FINEO_CATALOG, catalogs.getMetaData().getCatalogName(1));
      assertEquals(TABLE_CATALOG_COLUMN, catalogs.getMetaData().getColumnName(1));
      assertEquals(1, catalogs.getMetaData().getColumnCount());
      assertFalse(catalogs.next());
    }
  }

  /**
   * Ensures that we have requests that match the fineo catalog and schema. However, this is kind
   * of a false equivalence as the catalog/schema matching happens in Drill and is actually
   * transformed after we do the filtering, so its a bit messed up honestly.
   * @throws Exception
   */
  @Test
  public void testGetTables() throws Exception {
    Connection conn = getConnection();
    try (ResultSet tables = conn.getMetaData().getTables(FINEO_CATALOG, FINEO_SCHEMA, null, null)) {
      assertFalse(tables.next());
    }
  }

  @Test
  public void testReadSingleRowFromTable() throws Exception {
    Connection conn = getConnection();

    // setup a table that has a single row
    String table = "metric1";
    createTableAndWriteRow(table);

    // we need to rewrite this into a form that hsqldb understands. Not testing that the rewriter
    // works correctly, just that we do in fact rewrite the query
    FineoConnection fc = (FineoConnection) conn;
    FineoJdbcMeta meta = fc.getMetaForTesting();
    meta.setRewriterForTesting(new FineoSqlRewriter(ORG) {
      @Override
      public String rewrite(String sql) throws SQLException {
        return format("SELECT * FROM \"fineo.%s\".%s", ORG, table);
      }
    });
    // try to read that row from the table
    ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + table);
    assertTrue("Expected to read a single row!", rs.next());
    assertEquals(1, rs.getMetaData().getColumnCount());
    assertEquals("C1", rs.getMetaData().getColumnName(1));
    assertEquals("v1", rs.getString(1));
    assertFalse("Should only have gotten one row!", rs.next());
  }

  /**
   * Suppose that someone accidentally messed up and sets their schema - make sure they can't
   * read another table. This does not cover the case of intentionally setting the COMPANY_KEY in
   * the properties and stealing credentials
   *
   * @throws Exception on failure
   */
  @Test
  public void testReadingOtherOrgBySwitchingSchema() throws Exception {
    // setup a table that has a single row
    String table = "metric1";
    createTableAndWriteRow(table);
    // and create another org table
    String org2 = "another_org";
    createTable(table, org2);

    thrown.expect(RuntimeException.class);
    getConnection(org2);
  }

  private void createTableAndWriteRow(String table) throws SQLException {
    Pair<Connection, String> conn_name = createTable(table);
    Connection hsql = conn_name.getKey();
    String name = conn_name.getValue();
    hsql.createStatement().execute("INSERT INTO " + name + "(c1) VALUES ('v1')");
    hsql.commit();
  }

  private Pair<Connection, String> createTable(String table) throws SQLException {
    return createTable(table, ORG);
  }

  private Pair<Connection, String> createTable(String table, String org) throws SQLException {
    // create a fineo named table
    Connection hsqldb = hsqldb();
    String schema = getSchema(org);
    hsqldb.createStatement().execute("CREATE SCHEMA " + schema);
    String name = format(schema + ".%s", table);
    hsqldb.createStatement().execute("CREATE TABLE " + name + " (c1 varchar)");
    cleanup.add(() -> {
      hsqldb.createStatement().execute("DROP TABLE " + name);
    });
    cleanup.add(() -> {
      hsqldb.createStatement().execute("DROP SCHEMA " + schema + " IF EXISTS");
    });
    return new Pair<>(hsqldb, name);
  }

  private Connection hsqldb() throws SQLException {
    Properties props = new Properties();
    props.put("user", ScottHsqldb.USER);
    props.put("password", ScottHsqldb.PASSWORD);
    return getConnection(ScottHsqldb.URI, props);
  }

  private String getSchema(String org) {
    return format("\"%s.%s\"", "fineo", org);
  }

  public static String toString(ResultSet resultSet) throws SQLException {
    StringBuilder buf = new StringBuilder();
    final List<Ord<String>> columns = columnLabels(resultSet);
    while (resultSet.next()) {
      for (Ord<String> column : columns) {
        buf.append(column.i == 1 ? "" : "; ").append(column.e).append("=")
           .append(resultSet.getObject(column.i));
      }
      buf.append("\n");
    }
    return buf.toString();
  }

  private static List<Ord<String>> columnLabels(ResultSet resultSet) throws SQLException {
    int n = resultSet.getMetaData().getColumnCount();
    List<Ord<String>> columns = new ArrayList<>();
    for (int i = 1; i <= n; i++) {
      columns.add(Ord.of(i, resultSet.getMetaData().getColumnLabel(i)));
    }
    return columns;
  }

  private Connection getConnection() throws SQLException {
    return getConnection(ScottHsqldb.USER);
  }

  private Connection getConnection(String org) throws SQLException {
    FineoServerDriver.load();
    Properties props = new Properties();
    props.put(FineoServer.DRILL_CONNECTION_PARAMETER_KEY, ScottHsqldb.URI);
    props.put("user", ScottHsqldb.USER);
    props.put("password", ScottHsqldb.PASSWORD);
    props.put(FineoJdbcProperties.COMPANY_KEY_PROPERTY, org);
    // actually the hsqldb catalog...
    props.put(FineoServer.DRILL_CATALOG_PARAMETER_KEY, "PUBLIC");
    return getConnection(FineoServerDriver.CONNECT_PREFIX, props);
  }

  private Connection getConnection(String url, Properties props) throws SQLException {
    Connection conn = DriverManager.getConnection(url, props);
    connections.add(conn);
    return conn;
  }

  @FunctionalInterface
  private interface RunnableWith {
    void run() throws Exception;
  }
}
