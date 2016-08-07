package io.fineo.read.http;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.mobileconnectors.apigateway.ApiClientFactory;
import io.fineo.read.http.Api;
import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.remote.AuthenticationType;
import org.apache.calcite.avatica.remote.AvaticaHttpClient;
import org.apache.calcite.avatica.remote.UsernamePasswordAuthenticateable;

import java.net.URL;

/**
 * An Avatica client that writes/reads a Fineo AWS endpoint
 */
public class FineoAvaticaAwsHttpClient implements AvaticaHttpClient,
                                                  UsernamePasswordAuthenticateable {
  private final URL url;
  private StaticCredentialsProvider credentials;
  private volatile Api client;

  public FineoAvaticaAwsHttpClient(URL url, ConnectionConfig config) {
    this.url = url;
  }

  @Override
  public byte[] send(byte[] request) {
    ensureClient();
    return new byte[0];
  }

  private void ensureClient() {
    synchronized (client) {
      if (client == null) {
        synchronized (client) {
          client = createClient();
        }
      }
    }
  }

  private Api createClient() {
    ApiClientFactory factory = new ApiClientFactory()
      .credentialsProvider(this.credentials)
      .apiKey(key)
      .endpoint(url.toString());
    return factory.build(Api.class);
  }

  @Override
  public void setUsernamePassword(AuthenticationType authType, String username, String password) {
    switch (authType) {
      case BASIC:
      case DIGEST:
        this.credentials =
          new StaticCredentialsProvider(new BasicAWSCredentials(username, password));
    }
  }
}
