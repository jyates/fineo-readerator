package io.fineo.read.serve;

import fineo.client.org.asynchttpclient.DefaultAsyncHttpClient;
import fineo.client.org.asynchttpclient.Response;
import org.junit.ClassRule;
import org.junit.Test;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class TestServerHealthChecks {

  private static String org = "ORGID";
  @ClassRule
  public static StandaloneServerRule SERVER = new StandaloneServerRule(ServerTestUtils.LOAD_DRIVER);

  @Test
  public void testRoot() throws Exception {
    DefaultAsyncHttpClient client = new DefaultAsyncHttpClient();
    Response response = client.prepareGet(getUrl("")).execute().get();
    assertEquals(200, response.getStatusCode());
  }

  @Test
  public void testAlive() throws Exception {
    DefaultAsyncHttpClient client = new DefaultAsyncHttpClient();
    Response response = client.prepareGet(getUrl("alive")).execute().get();
    assertEquals(200, response.getStatusCode());
  }

  @Test
  public void testFineoAlive() throws Exception {
    DefaultAsyncHttpClient client = new DefaultAsyncHttpClient();
    Response response = client.prepareGet(getUrl("alive/fineo")).execute().get();
    assertEquals(200, response.getStatusCode());
  }

  private String getUrl(String suffix){
    return format("http://%s:%s/%s", "localhost", SERVER.port(), suffix);
  }
}
