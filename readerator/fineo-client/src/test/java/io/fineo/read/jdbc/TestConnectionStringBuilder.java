package io.fineo.read.jdbc;

import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.avatica.ConnectionConfigImpl;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.fineo.read.jdbc.FineoConnectionProperties.API_KEY;
import static io.fineo.read.jdbc.FineoConnectionProperties.CLIENT_INIT_TIMEOUT;
import static org.junit.Assert.assertEquals;

public class TestConnectionStringBuilder {

  private final String prefix = "jdbc:fineo:";
  private final String url = "https://some.url";

  @Test
  public void testUrlSpecification() throws Exception {
    ConnectionStringBuilder builder = new ConnectionStringBuilder(prefix, url);
    assertEquals(prefix + "url=" + url, builder.build());
  }

  @Test
  public void testStringQueryParameters() throws Exception {
    String out = new ConnectionStringBuilder(prefix, url)
      .with("k", "v")
      .with("k2", "1").build();

    Map<String, String> expected = new HashMap<>();
    expected.put("k", "v");
    expected.put("k2", "1");
    assertUrlContainsQueryStrings(expected, out);
  }

  @Test
  public void testConnectionPropertySetting() throws Exception {
    Properties props = new Properties();
    props.put(FineoConnectionProperties.CLIENT_INIT_TIMEOUT.camelName(), "1");
    props.put(API_KEY.camelName(), "key");
    Map<String, String> expected = new HashMap<>();
    expected.put(CLIENT_INIT_TIMEOUT.camelName(), "1");
    expected.put(API_KEY.camelName(), "key");
    assertUrlContainsQueryStrings(expected, new ConnectionStringBuilder(prefix, url)
      .with(FineoConnectionProperties.CLIENT_INIT_TIMEOUT, props)
      .with(API_KEY, props).build());
  }

  private void assertUrlContainsQueryStrings(Map<String, String> expected, String out)
    throws SQLException, MalformedURLException {
    Properties props = ConnectStringParser.parse(out.replace(prefix, ""));
    ConnectionConfigImpl config = new ConnectionConfigImpl(props);
    assertEquals(expected, ConnectionStringBuilder.parse(new URL(config.url())));
  }
}
