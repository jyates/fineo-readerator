package io.fineo.read.jdbc;

import io.fineo.read.http.FineoAvaticaAwsHttpClient;
import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.remote.AuthenticationType;
import org.apache.calcite.avatica.remote.AvaticaHttpClient;
import org.apache.calcite.avatica.remote.AvaticaHttpClientFactory;
import org.apache.calcite.avatica.remote.KerberosConnection;

import java.net.URL;

public class FineoHttpClientFactory implements AvaticaHttpClientFactory {

  private static final String URL =
    "https://6s6rc7bqxb.execute-api.us-east-1.amazonaws.com/prod/v1";

  @Override
  public AvaticaHttpClient getClient(URL url, ConnectionConfig config,
    KerberosConnection kerberosUtil) {
    FineoAvaticaAwsHttpClient client = new FineoAvaticaAwsHttpClient(URL, config);
    // kinda sorta just what the avatica thing does, but provide the config as well
    final String username = config.avaticaUser();
    final String password = config.avaticaPassword();
    client.setUsernamePassword(AuthenticationType.BASIC, username, password);
    return client;
  }
}
