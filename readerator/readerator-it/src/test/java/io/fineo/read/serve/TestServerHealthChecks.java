package io.fineo.read.serve;

import fineo.client.org.asynchttpclient.DefaultAsyncHttpClient;
import fineo.client.org.asynchttpclient.Response;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class TestServerHealthChecks {

  private static String org = "ORGID";
  @ClassRule
  public static StandaloneServerRule SERVER = new StandaloneServerRule(org, ServerTestUtils.LOAD_DRIVER);

  @Test
  public void testAlive() throws Exception {
    DefaultAsyncHttpClient client = new DefaultAsyncHttpClient();
    Response response = client.prepareGet(getUrl("alive")).execute().get();
    assertEquals(200, response.getStatusCode());
  }

  @Test
  @Ignore("Fineo health check relies on having the ORG in the request. Currently just checking if"
          + " the server is up instead")
  public void testFineoAlive() throws Exception {
    DefaultAsyncHttpClient client = new DefaultAsyncHttpClient();
    Response response = client.prepareGet(getUrl("alive/fineo")).execute().get();
    assertEquals(200, response.getStatusCode());
  }

  private String getUrl(String suffix){
    return format("http://%s:%s/%s", "localhost", SERVER.port(), suffix);
  }
}
