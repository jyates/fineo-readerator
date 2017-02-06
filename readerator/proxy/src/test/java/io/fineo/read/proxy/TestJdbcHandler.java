package io.fineo.read.proxy;

import io.fineo.read.serve.StandaloneServerRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static fineo.client.com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

public class TestJdbcHandler {
  @ClassRule
  public static StandaloneServerRule SERVER = StandaloneServerRule.create();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testMustProvideNonNullUrl() throws Exception {
    thrown.expect(NullPointerException.class);
    new JdbcHandler(null, new Properties());
  }

  @Test
  public void testReadValues() throws Exception {
    JdbcHandler handler = new JdbcHandler(getUrl(), new Properties());
    List<Map<String, Object>> result = handler.read("SELECT * FROM (VALUES(1))", SERVER.getOrg());
    Map<String, Object> expected = new HashMap<>();
    // match the HSQLDB unknown column format of CX
    expected.put("C1", 1);
    assertEquals(newArrayList(expected), result);
  }

  private String getUrl() {
    return String.format("jdbc:fineo:url=http://localhost:%s", SERVER.port());
  }
}
