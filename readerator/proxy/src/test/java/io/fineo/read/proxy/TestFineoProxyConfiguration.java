package io.fineo.read.proxy;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestFineoProxyConfiguration {

  @Test
  public void testPortFixing() throws Exception {
    FineoProxyConfiguration conf =
      new FineoProxyConfiguration()
        .setJdbcPortFix(200)
        .setJdbcUrl("jdbc:fineo:url=http://localhost:5200");
    assertEquals("jdbc:fineo:url=http://localhost:5000", conf.getUrl());
  }
}
