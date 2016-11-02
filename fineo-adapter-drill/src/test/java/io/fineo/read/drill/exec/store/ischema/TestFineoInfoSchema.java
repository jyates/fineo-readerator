package io.fineo.read.drill.exec.store.ischema;

import com.amazonaws.services.dynamodbv2.document.Table;
import io.fineo.drill.rule.DrillClusterRule;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.read.drill.BaseFineoDynamoTest;
import io.fineo.read.drill.BaseFineoTest.Verify;
import io.fineo.read.drill.BootstrapFineo;
import io.fineo.read.drill.FineoTestUtil;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import io.fineo.schema.store.StoreManager;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.fineo.read.drill.BaseFineoTest.newBootstrap;
import static io.fineo.read.drill.FineoTestUtil.get1980;
import static io.fineo.read.drill.FineoTestUtil.p;
import static io.fineo.read.drill.exec.store.ischema.FineoInfoSchemaUserFilters
  .FINEO_HIDDEN_USER_NAME;

/**
 * Test that we are doing the correct filtering and translations for fineo tables, without
 * exposing internal information to users
 * <p>
 * We extend off the BaseFineoDynamoTest because we need to modify the drill cluster setup
 * </p>
 */
public class TestFineoInfoSchema extends BaseFineoDynamoTest {

  private static final Logger LOG = LoggerFactory.getLogger(TestFineoInfoSchema.class);
  private static final String FINEO = "FINEO";
  private static final String IS = "INFORMATION_SCHEMA";

  @ClassRule
  public static DrillClusterRule drill = new DrillClusterRule(1);

  // set the Fineo info_schema filter rules
  static {
    drill.overrideConfigHook(FineoInfoSchemaUserFilters::overrideWithInfoSchemaFilters);
  }

  @Before
  public void setupTables() throws Exception {
    createTablesAndRows();
  }

  @Test
  public void testAnonymousUserCannotSeeAnySchemas() throws Exception {
    verify("SHOW SCHEMAS", FineoTestUtil.withNext());
    verify("SELECT * FROM INFORMATION_SCHEMA.`SCHEMATA`", FineoTestUtil.withNext());
  }

  @Test
  public void testAnonymousUserCannotSeeAnyTables() throws Exception {
    verify("SELECT * FROM INFORMATION_SCHEMA.`TABLES`", FineoTestUtil.withNext());
  }

  @Test
  public void testAnonymousUserCanSeeTheCatalog() throws Exception {
    Map<String, Object> info = new HashMap<>();
    info.put("CATALOG_NAME", "FINEO");
    info.put("CATALOG_DESCRIPTION", FineoInfoSchemaUserTranslator.FINEO_DESCRIPTION);
    info.put("CATALOG_CONNECT", "");
    verify("SELECT * FROM INFORMATION_SCHEMA.`CATALOGS`", FineoTestUtil.withNext(info));
  }

  @Test
  public void testTenantUserCanSeeInfoAndFineoSchema() throws Exception {
    verifyForUser(org, "SHOW SCHEMAS", FineoTestUtil.withNext(
      map("SCHEMA_NAME", "INFORMATION_SCHEMA"),
      map("SCHEMA_NAME", FINEO)));
  }

  @Test
  public void testTenantUserCanSeeFineoTable() throws Exception {
    verifyForUser(org, "SHOW TABLES IN " + FINEO, FineoTestUtil.withNext(
      map("TABLE_SCHEMA", FINEO, "TABLE_NAME", metrictype)));

    verifyForUser(org, "SELECT * FROM INFORMATION_SCHEMA.`TABLES`", FineoTestUtil.withNext(
      table(FINEO, IS, "CATALOGS"),
      table(FINEO, IS, "COLUMNS"),
      table(FINEO, IS, "SCHEMATA"),
      table(FINEO, IS, "TABLES"),
      table(FINEO, IS, "VIEWS"),
      table(FINEO, FINEO, metrictype)));
  }

  private Map<String, Object> table(String catalog, String schema, String name) {
    return map("TABLE_CATALOG", catalog, "TABLE_SCHEMA", schema, "TABLE_NAME", name,
      "TABLE_TYPE", "TABLE");
  }

  @Test
  public void testSuperUserCanSeeNativeTables() throws Exception {
    verifyForUser(FINEO_HIDDEN_USER_NAME, "SHOW SCHEMAS",
      FineoTestUtil.withNext(
        map("SCHEMA_NAME", "INFORMATION_SCHEMA"),
        map("SCHEMA_NAME", "cp.default"),
        map("SCHEMA_NAME", "dfs.default"),
        map("SCHEMA_NAME", "dfs.root"),
        map("SCHEMA_NAME", "dfs.tmp"),
        map("SCHEMA_NAME", "dfs_test.default"),
        map("SCHEMA_NAME", "dfs_test.home"),
        map("SCHEMA_NAME", "dynamo"),
        map("SCHEMA_NAME", "fineo." + org),
        map("SCHEMA_NAME", "fineo"),
        map("SCHEMA_NAME", "sys")
      ));
  }

  @Test
  public void testSelectUserColumnsFromTable() throws Exception {
    verifyForUser(org,
      "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE" +
      " FROM INFORMATION_SCHEMA.COLUMNS " +
      "WHERE TABLE_SCHEMA like 'FINEO'",
      FineoTestUtil.withNext(
        map("TABLE_SCHEMA", "FINEO",
          "TABLE_NAME", metrictype,
          "COLUMN_NAME", "timestamp",
          "ORDINAL_POSITION", 1,
          "DATA_TYPE", "BIGINT"),
        map("TABLE_SCHEMA", "FINEO",
          "TABLE_NAME", metrictype,
          "COLUMN_NAME", "field1",
          "ORDINAL_POSITION", 2,
          "DATA_TYPE", "INTEGER")
      ));
  }

  @Test
  public void testReadSchemataForUser() throws Exception {
    verifyForUser(org,
      "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA",
      FineoTestUtil.withNext(
        map("CATALOG_NAME", "FINEO",
          "SCHEMA_NAME", "INFORMATION_SCHEMA",
          "SCHEMA_OWNER", "system",
          "TYPE", "ischema",
          "IS_MUTABLE", "NO"),
        map("CATALOG_NAME", "FINEO",
          "SCHEMA_NAME", "FINEO",
          "SCHEMA_OWNER", "user",
          "TYPE", "FINEO",
          "IS_MUTABLE", "NO")
      ));
  }

  private void verifyForUser(String user, String query, Verify<ResultSet> verify) throws Exception {
    Properties props = new Properties();
    props.put("user", user);
    try (Connection conn = drill.getUnmanagedConnection(props)) {
      verify(conn, query, verify);
    }
  }

  private void verify(String query, Verify<ResultSet> verify) throws Exception {
    Connection conn = drill.getConnection();
    verify(conn, query, verify);
  }

  private void verify(Connection conn, String query, Verify<ResultSet> verify) throws Exception {
    LOG.debug("Running query: {}", query);
    try (ResultSet set = conn.createStatement().executeQuery(query)) {
      verify.verify(set);
    }
  }

  private Map<String, Object> map(Object... elements) {
    Map<String, Object> map = new HashMap<>();
    String key = null;
    for (Object e : elements) {
      if (key == null) {
        key = (String) e;
      } else {
        map.put(key, e);
        key = null;
      }
    }
    return map;
  }

  private void createTablesAndRows() throws Exception {
    TestState state = register(p(fieldname, StoreManager.Type.INTEGER));
    long ts = get1980();

    File tmp = folder.newFolder("drill");
    long tsFile = ts - Duration.ofDays(35).toMillis();

    Map<String, Object> parquetRow = new HashMap<>();
    parquetRow.put(fieldname, 1);
    Pair<FsSourceTable, File> parquet = FineoTestUtil
      .writeParquet(state, drill.getConnection(), tmp, org, metrictype, tsFile, parquetRow);

    Map<String, Object> wrote = prepareItem();
    wrote.put(Schema.SORT_KEY_NAME, ts);
    wrote.put(fieldname, 2);
    Table table = state.writeToDynamo(wrote);

    bootstrapper()
      // dynamo
      .withDynamoKeyMapper()
      .withDynamoTable(table)
      // fs
      .withLocalSource(parquet.getKey())
      .bootstrap();
  }

  private BootstrapFineo.DrillConfigBuilder bootstrapper() {
    return basicBootstrap(newBootstrap(drill).builder());
  }
}
