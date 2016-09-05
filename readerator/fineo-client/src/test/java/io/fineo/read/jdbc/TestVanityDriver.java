package io.fineo.read.jdbc;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.ConnectException;
import java.net.Inet4Address;
import java.sql.DriverManager;
import java.util.concurrent.ExecutionException;

import static com.amazonaws.SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY;
import static com.amazonaws.SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY;
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
    // ensure that we an AWS key/secret set properties set
    System.setProperty(ACCESS_KEY_SYSTEM_PROPERTY, "AKIAIUHLZLB7VTXUAX7A");
    System.setProperty(SECRET_KEY_SYSTEM_PROPERTY, "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");

    Class.forName("io.fineo.read.Driver");
    assertEquals(DriverManager.getDriver("jdbc:fineo:").getClass(), io.fineo.read.Driver.class);
    String url = "https://" + Inet4Address.getLocalHost().getCanonicalHostName();
    thrown.expectCause(new BaseMatcher<Throwable>() {
      @Override
      public boolean matches(Object item) {
        if(item instanceof ExecutionException){
          item = ((ExecutionException) item).getCause();
          return ((ConnectException) item).getMessage().contains("Connection refused");
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
