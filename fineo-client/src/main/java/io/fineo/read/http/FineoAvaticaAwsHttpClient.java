package io.fineo.read.http;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.mobileconnectors.apigateway.ApiClientFactory;
import io.fineo.read.jdbc.ConnectionPropertyUtil;
import io.fineo.read.jdbc.ConnectionStringBuilder;
import io.fineo.read.jdbc.FineoConnectionProperties;
import org.apache.calcite.avatica.remote.AuthenticationType;
import org.apache.calcite.avatica.remote.AvaticaHttpClient;
import org.apache.calcite.avatica.remote.UsernamePasswordAuthenticateable;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import static io.fineo.read.jdbc.ConnectionPropertyUtil.setInt;

/**
 * An Avatica client that writes/reads a Fineo AWS endpoint
 */
public class FineoAvaticaAwsHttpClient implements AvaticaHttpClient,
                                                  UsernamePasswordAuthenticateable {
  private final String url;
  private final Map<String, String> properties;
  private StaticCredentialsProvider credentials;
  private volatile Api client;

  public FineoAvaticaAwsHttpClient(URL url) throws MalformedURLException {
    // simplify the url to just the bit we will actually send
    this.url = new URL(url.getProtocol(), url.getAuthority(), url.getPath()).toExternalForm();
    this.properties = ConnectionStringBuilder.parse(url);
  }

  @Override
  public byte[] send(byte[] request) {
    ensureClient();
    return client.send(request).getBytes();
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
      .apiKey(properties.get(FineoConnectionProperties.API_KEY))
      .endpoint(url.toString());
    // not having credentials allows us to not generate a signer, which allows us to connect to
    // any URL, not just an AWS endpoitn
    if (this.credentials != null) {
      factory.credentialsProvider(this.credentials);
    }
    return factory.build(Api.class);
  }

  private ClientConfiguration getClientConfiguration() {
    ClientConfiguration client = new ClientConfiguration();
    setInt(properties, FineoConnectionProperties.CLIENT_EXEC_TIMEOUT,
      prop -> client.withClientExecutionTimeout(prop));
    setInt(properties, FineoConnectionProperties.CLIENT_MAX_CONNECTIONS,
      prop -> client.withMaxConnections(prop));
    setInt(properties, FineoConnectionProperties.CLIENT_MAX_IDLE,
      prop -> client.withConnectionMaxIdleMillis(prop));
    setInt(properties, FineoConnectionProperties.CLIENT_REQUEST_TIMEOUT,
      prop -> client.withRequestTimeout(prop));
    setInt(properties, FineoConnectionProperties.CLIENT_INIT_TIMEOUT,
      prop -> client.withConnectionTimeout(prop));
    ConnectionPropertyUtil.set(properties, FineoConnectionProperties.CLIENT_TTL,
      prop -> {
        Long l = Long.parseLong(prop);
        if (l >= 0) {
          client.withConnectionTTL(l);
        }
      });
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
