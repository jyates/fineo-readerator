package io.fineo.read.http;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import io.fineo.read.http.RequestConverter;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestRequestConverter {

  String address = "https://53r0nhslih.execute-api.us-east-1.amazonaws.com";

  @Test
  @Ignore("Requires AWS test api connectivity")
  public void testMockApi() throws Exception {
    String url = address + "/prod";
    URI uri = new URI(address);
    AsyncHttpClient client = new DefaultAsyncHttpClient();
    BoundRequestBuilder post = client.preparePost(url);
    RequestConverter converter = new RequestConverter(post, uri);
    ProfileCredentialsProvider provider = new ProfileCredentialsProvider("test-user");
    converter.prepareRequest(new byte[]{1}, provider, "pmV5QkC0RG7tHMYVdyvgG8qLgNV79Swh3XIiNsF1");
    Response response = post.execute().get();
    System.out.println(response);
    assertEquals(200, response.getStatusCode());
  }
}
