package io.fineo.read.jdbc;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.core.Is;
import org.hamcrest.core.IsEqual;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.ConnectException;
import java.net.Inet4Address;
import java.sql.Connection;
import java.sql.DriverManager;

import static java.lang.String.format;
import static junit.framework.TestCase.assertEquals;

/**
 *
 */
public class TestVanityDriver {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testGetDriver() throws Exception {
    Class.forName("io.fineo.read.Driver");
    assertEquals(DriverManager.getDriver("jdbc:fineo:").getClass(), io.fineo.read.Driver.class);
    String url = "https://" + Inet4Address.getLocalHost().getCanonicalHostName();
    thrown.expectCause(new BaseMatcher<Throwable>() {
      @Override
      public boolean matches(Object item) {
        if(item instanceof ConnectException){
          return ((ConnectException) item).getMessage().equals("Connection refused");
        }
        return false;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("Must have a message of 'Connection refused'");
      }
    });
    DriverManager.getConnection(format("jdbc:fineo:url=%s;api_key=1234", url));
  }
}
