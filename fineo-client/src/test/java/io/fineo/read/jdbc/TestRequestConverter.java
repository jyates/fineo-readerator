package io.fineo.read.jdbc;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import io.fineo.read.http.RequestConverter;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestRequestConverter {

  @Test
  public void testMockApi() throws Exception {
    String url = "https://53r0nhslih.execute-api.us-east-1.amazonaws.com/test";
    URI uri = new URI("https://53r0nhslih.execute-api.us-east-1.amazonaws.com");
    AsyncHttpClient client = new DefaultAsyncHttpClient();
    BoundRequestBuilder post = client.preparePost(url);
    RequestConverter converter = new RequestConverter(post, uri);
    ProfileCredentialsProvider provider = new ProfileCredentialsProvider("test-user");
    ListenableFuture<Response> future = converter.request(new byte[]{1}, provider,
      "pmV5QkC0RG7tHMYVdyvgG8qLgNV79Swh3XIiNsF1");
    Response response = future.get();
    System.out.println(response);
    assertEquals(200, response.getStatusCode());
  }
}
