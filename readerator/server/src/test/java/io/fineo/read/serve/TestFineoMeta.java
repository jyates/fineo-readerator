package io.fineo.read.serve;

import net.hydromatic.scott.data.hsqldb.ScottHsqldb;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.jdbc.FineoJdbcMeta;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.junit.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

import static org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem.getInstance;

/**
 *
 */
public class TestFineoMeta {

  private static final String ORG = "orgid";

  @Test
  public void testConnect() throws Exception {
    FineoJdbcMeta meta = getMeta();
    Meta.ConnectionHandle handle = newHandle();
    meta.openConnection(handle, new HashMap<>());
  }

  private Meta.ConnectionHandle newHandle() {
    return new Meta.ConnectionHandle(UUID.randomUUID().toString());
  }

  private FineoJdbcMeta getMeta() throws SQLException {
    Properties props = new Properties();
    props.put("user", ScottHsqldb.USER);
    props.put("password", ScottHsqldb.PASSWORD);
    return new FineoJdbcMeta(ScottHsqldb.URI, props, getInstance(), ORG);
  }
}
