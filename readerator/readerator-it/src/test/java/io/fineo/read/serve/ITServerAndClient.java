package io.fineo.read.serve;

import io.fineo.read.Driver;
import io.fineo.read.jdbc.FineoConnectionProperties;
import net.hydromatic.scott.data.hsqldb.ScottHsqldb;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class ITServerAndClient {

  // org has to match the HsqlDB user (the underlying DB user!)
  private static final String ORG = ScottHsqldb.USER;
  private static FineoServer SERVER;
  private static int port;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setup() {
    Driver.load();
    SERVER = new FineoServer();
    SERVER.setCatalogForTesting("PUBLIC");
    SERVER.setOrgForTesting(ORG);
    port = new Random().nextInt(65535 - 49151);
    port += 49151;
    Properties props = new Properties();
    props.put("user", ScottHsqldb.USER);
    props.put("password", ScottHsqldb.PASSWORD);
    SERVER.setPropsForTesting(props);
    SERVER.setPortForTesting(port);
    SERVER.setDrillForTesting(ScottHsqldb.URI);
    SERVER.start();
  }

  @AfterClass
  public static void shutdown() throws InterruptedException {
    SERVER.stop();
  }

  @Test
  public void testWrongOrgIdRejected() throws Exception {
    Properties props = getProps();
    props.setProperty(FineoConnectionProperties.API_KEY.camelName(), "1234-not-the-org");
    thrown.expect(RuntimeException.class);
    connect(props);
  }

  @Test
  public void testSimpleConnection() throws Exception {
    try (Connection conn = connect()) {
      DatabaseMetaData meta = conn.getMetaData();
      try (ResultSet rs = meta.getCatalogs()) {
        assertTrue("No catalogs found!", rs.next());
        assertEquals("Wrong catalog name!", "FINEO", rs.getString("TABLE_CAT"));
        assertFalse("More than one catalog found!", rs.next());
      }

      try (ResultSet rs = meta.getSchemas()) {
        assertTrue("No schemas found!", rs.next());
        assertEquals("Missing info schema", "INFORMATION_SCHEMA", rs.getString("TABLE_SCHEM"));
        assertTrue("Should have two schemas", rs.next());
        assertEquals("Did not just delegate schema lookup to internal jdbc metadata",
          "PUBLIC", rs.getString("TABLE_SCHEM"));
      }
    }
  }

  @Test
  public void testGetTypeInfo() throws Exception {
    try (Connection conn = connect()) {
      ResultSet set = conn.getMetaData().getTypeInfo();
      assertTrue(set.next());
    }
  }

  private Connection connect() throws SQLException {
    return connect(getProps());
  }

  private Properties getProps() {
    Properties props = new Properties();
    props.setProperty("user", "test-user");
    props.setProperty("password", "test-password");
    props.setProperty("fineo.internal.test.api-prefix", "/");
    props.setProperty(FineoConnectionProperties.API_KEY.camelName(), ORG);
    return props;
  }

  private Connection connect(Properties props) throws SQLException {
    String fineoUrl = format("jdbc:fineo:url=%s", "http://localhost:" + port);
    return DriverManager.getConnection(fineoUrl, props);
  }
}
