package io.fineo.read.http;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.mobileconnectors.apigateway.ApiClientFactory;
import io.fineo.read.AwsApiGatewayBytesTranslator;
import io.fineo.read.jdbc.ConnectionStringBuilder;
import io.fineo.read.jdbc.FineoConnectionProperties;
import org.apache.calcite.avatica.remote.AuthenticationType;
import org.apache.calcite.avatica.remote.AvaticaHttpClient;
import org.apache.calcite.avatica.remote.UsernamePasswordAuthenticateable;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;

import static io.fineo.read.jdbc.ConnectionPropertyUtil.setInt;
import static io.fineo.read.jdbc.FineoConnectionProperties.API_KEY;

/**
 * An Avatica client that writes/reads a Fineo AWS endpoint
 */
public class FineoAvaticaAwsHttpClient implements AvaticaHttpClient,
                                                  UsernamePasswordAuthenticateable {
  private final URL url;
  private final Map<String, String> properties;
  private final BoundRequestBuilder post;
  private final RequestConverter converter;
  private StaticCredentialsProvider credentials;
  private volatile Api client;

  public FineoAvaticaAwsHttpClient(URL url) throws MalformedURLException, URISyntaxException {
    // simplify the url to just the bit we will actually send
    this.url = (
      url.getPort() == -1 ?
      new URL(url.getProtocol(), url.getHost(), url.getPath()) :
      new URL(url.getProtocol(), url.getHost(), url.getPort(), url.getPath()));
    this.properties = ConnectionStringBuilder.parse(url);
    AsyncHttpClient client = new DefaultAsyncHttpClient();
    this.post = client.preparePost(this.url.toExternalForm());
    this.converter = new RequestConverter(post, url.toURI());
  }

  @Override
  public byte[] send(byte[] request) {
    return converter.request(request, credentials, properties.get(API_KEY));
  }

  /**
   * Ensure client must come later as we may or may not be configured with credentials, so we
   * always need to check to ensure that its created. Using double-checked locking so hopefully
   * its not too bad. Ideally, we will be notified when creation is complete so we can initiate
   * the connection/client creation, but that's not in avatica (yet).
   */
  private void ensureClient() {
    if (client == null) {
      synchronized (this) {
        if (client == null) {
          client = createClient();
        }
      }
    }
  }

  private Api createClient() {
    ApiClientFactory factory = new ApiClientFactory()
      .clientConfiguration(getClientConfiguration())
      .credentialsProvider(this.credentials)
      .apiKey(properties.get(API_KEY))
      .endpoint(url.toString());
    return factory.build(Api.class);
  }

  private ClientConfiguration getClientConfiguration() {
    ClientConfiguration client = new ClientConfiguration();
    setInt(properties, FineoConnectionProperties.CLIENT_MAX_CONNECTIONS,
      prop -> client.withMaxConnections(prop));
    setInt(properties, FineoConnectionProperties.CLIENT_REQUEST_TIMEOUT,
      prop -> client.withSocketTimeout(prop));
    setInt(properties, FineoConnectionProperties.CLIENT_INIT_TIMEOUT,
      prop -> client.withConnectionTimeout(prop));
    setInt(properties, FineoConnectionProperties.CLIENT_MAX_ERROR_RETRY,
      prop -> client.withMaxErrorRetry(prop));
    return client;
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
