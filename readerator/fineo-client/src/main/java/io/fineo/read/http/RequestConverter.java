package io.fineo.read.http;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.DefaultRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.http.HttpMethodName;
import com.google.common.annotations.VisibleForTesting;
import io.fineo.read.AwsApiGatewayBytesTranslator;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Convert the request into something that can be sent to AWS API Gateway with optional credentials
 */
public class RequestConverter {

  private final AwsApiGatewayBytesTranslator translator = new AwsApiGatewayBytesTranslator();
  private final BoundRequestBuilder post;
  private final URI endpoint;

  public RequestConverter(BoundRequestBuilder post, URI endpoint) {
    this.post = post;
    this.endpoint = endpoint;
  }

  public byte[] request(byte[] data, AWSCredentialsProvider credentials,
    String apiKey) {
    prepareRequest(data, credentials, apiKey);
    ListenableFuture<Response> future = post.execute();
    try {
      Response response = future.get();
      return translator.decode(response.getResponseBodyAsBytes());
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  void prepareRequest(byte[] data, AWSCredentialsProvider credentials,
    String apiKey) {
    byte[] translated = translator.encode(data);
    post.setBody(translated);
    if (credentials != null) {
      DefaultRequest<AmazonWebServiceRequest> awsReq = new DefaultRequest( "execute-api");
      awsReq.setContent(new ByteArrayInputStream(translated));
      awsReq.addHeader("Content-Length", Integer.toString(translated.length));
      awsReq.addHeader("Content-Type", "application/json");

      awsReq.setHttpMethod(HttpMethodName.POST);
      awsReq.setEndpoint(endpoint);
      awsReq.setResourcePath("/test");
      awsReq.addHeader("x-api-key", apiKey);

      AWS4Signer signer = new AWS4Signer();
      signer.setServiceName("execute-api");
      signer.setRegionName("us-east-1");
      signer.sign(awsReq, credentials.getCredentials());

      for (Map.Entry<String, String> header : awsReq.getHeaders().entrySet()) {
        post.addHeader(header.getKey(), header.getValue());
      }

      post.setQueryParams(awsReq.getParameters());
    }
  }
}
