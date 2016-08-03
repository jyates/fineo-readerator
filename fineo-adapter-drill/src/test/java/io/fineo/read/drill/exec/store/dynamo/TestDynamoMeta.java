package io.fineo.read.drill.exec.store.dynamo;

import io.fineo.drill.ClusterTest;
import io.fineo.read.drill.BaseFineoTest;
import io.fineo.read.drill.BootstrapFineo;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;

@Category(ClusterTest.class)
public class TestDynamoMeta extends BaseFineoTest {

  @Test
  public void testReadTableNamesInMetadata() throws Exception {
    register();
    // setup schema for dynamo with no tables
    BootstrapFineo bootstrap = new BootstrapFineo();
    BootstrapFineo.DrillConfigBuilder builder = basicBootstrap(bootstrap.builder());
    builder.bootstrap();
    Connection conn = drill.getConnection();
    ResultSet r = conn.getMetaData().getCatalogs();
  }
}
