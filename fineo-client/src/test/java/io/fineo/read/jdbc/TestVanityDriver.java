package io.fineo.read.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static junit.framework.TestCase.assertEquals;

/**
 *
 */
public class TestVanityDriver {

  @Test
  public void testGetDriver() throws Exception {
    Class.forName("io.fineo.read.Driver");
    assertEquals(DriverManager.getDriver("jdbc:fineo:").getClass(), io.fineo.read.Driver.class);
    DriverManager.getConnection("jdbc:fineo:url=https://some.url;api_key=1234");
  }
}
