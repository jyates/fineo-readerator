/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fineo.read.serve;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import io.fineo.read.FineoJdbcProperties;
import io.fineo.read.serve.driver.FineoServerDriver;
import io.fineo.read.serve.health.IsAliveHealthCheck;
import io.fineo.read.serve.health.IsFineoAliveCheck;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.dropwizard3.DropwizardMetricsSystemConfiguration;
import org.apache.calcite.avatica.metrics.dropwizard3.DropwizardMetricsSystemFactory;
import org.apache.calcite.avatica.remote.Driver.Serialization;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.ProtobufTranslation;
import org.apache.calcite.avatica.server.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * An Avatica server for HSQLDB.
 */
public class FineoServer {
  private static final Logger LOG = LoggerFactory.getLogger(FineoServer.class);

  private static final Serialization SER = Serialization.PROTOBUF;

  private static final String EMPTY = "===UNSPECIFIED===";

  // command line key
  public static final String DRILL_CONNECTION_PARAMETER_KEY = "drill-connection";
  public static final String DRILL_CATALOG_PARAMETER_KEY = "drill-catalog";

  // environment key
  private static final String ORG_ID_ENV_KEY = "FINEO_ORG_ID";
  private static final String DRILL_CONNECTION_ENV_KEY = "FINEO_DRILL_CONNECTION";
  private static final String DRILL_CONNECTION_CATALOG_KEY = "FINEO_DRILL_CATALOG";
  private static final String PORT_KEY = "PORT";

  @Parameter(names = "--org-id",
             description = "Org ID served by this server. Only 1 org per server allowed.")
  private String org = System.getenv(ORG_ID_ENV_KEY);

  @Parameter(names = {"-p", "--port" },
             description = "Port the server should bind")
  private int port = Integer.valueOf(getEnv(PORT_KEY, "0"));

  @Parameter(names = "--" + DRILL_CONNECTION_PARAMETER_KEY,
             description = "Connection string for the Drill JDBC driver")
  private String drill = System.getenv(DRILL_CONNECTION_ENV_KEY);

  @Parameter(names = "--" + DRILL_CATALOG_PARAMETER_KEY,
             description = "Override the catalog that the target jdbc connection will use")
  private String catalog = getEnv(DRILL_CONNECTION_CATALOG_KEY, "DRILL");

  private HttpServer server;
  private final MetricRegistry metrics = new MetricRegistry();


  public void start() {
    if (null != server) {
      LOG.error("The server was already started");
      System.exit(ExitCodes.ALREADY_STARTED.ordinal());
      return;
    }

    try {
      // ensure our driver is loaded
      FineoServerDriver.load();

      // setup the connection delegation properties
      Properties props = new Properties();
      props.setProperty(DRILL_CATALOG_PARAMETER_KEY, catalog);
      props.setProperty(DRILL_CONNECTION_PARAMETER_KEY, drill);
      props.setProperty(FineoJdbcProperties.COMPANY_KEY_PROPERTY, org);

      // its "just" a jdbc connection... to a driver which creates its own jdbc connection the
      // specified connection key. This ensures that we encapsulate the schema and tables from
      // the wrong user
      DropwizardMetricsSystemConfiguration metricsConf = new DropwizardMetricsSystemConfiguration
        (metrics);
      MetricsSystem metrics = new DropwizardMetricsSystemFactory().create(metricsConf);
      JdbcMeta meta = new JdbcMeta(FineoServerDriver.CONNECT_PREFIX, props, metrics);
      LocalService service = new LocalService(meta, metrics);

      // custom translator so we can interop with AWS
      ProtobufTranslation translator = new AwsStringBytesDecodingTranslator();
      AvaticaProtobufHandler handler =
        new AvaticaProtobufHandler(service, metrics, null, translator)
          .withRequestHandlers(
            new IsAliveHealthCheck(),
            new IsFineoAliveCheck(meta));

      // Construct the server
      this.server = new HttpServer.Builder()
        .withMetricsConfiguration(metricsConf)
        .withHandler(handler)
        .withPort(port)
        .build();

      // Then start it
      server.start();

      LOG.info("Started Avatica server on port {} with serialization {}", server.getPort(), SER);
    } catch (Exception e) {
      LOG.error("Failed to start Avatica server", e);
      System.exit(ExitCodes.START_FAILED.ordinal());
    }
  }

  public void stop() {
    if (server != null) {
      server.stop();
      server = null;
    }
  }

  public void join() throws InterruptedException {
    server.join();
  }

  public static void main(String[] args) {
    final FineoServer server = new FineoServer();
    new JCommander(server, args);

    Preconditions.checkNotNull(server.org, "Missing ORG ID specification!");
    Preconditions.checkNotNull(server.drill, "Missing DRILL CONNECTION specification!");
    // Try to clean up when the server is stopped.
    Runtime.getRuntime().addShutdownHook(
      new Thread(() -> {
        LOG.info("Stopping server");
        server.stop();
        LOG.info("Server stopped");
      }));

    server.start();

    try {
      server.join();
    } catch (InterruptedException e) {
      // Reset interruption
      Thread.currentThread().interrupt();
      // And exit now.
      return;
    }
  }

  /**
   * Converter from String to Serialization.
   */
  private static class SerializationConverter implements IStringConverter<Serialization> {
    @Override
    public Serialization convert(String value) {
      return Serialization.valueOf(value.toUpperCase());
    }
  }

  /**
   * Codes for exit conditions
   */
  private enum ExitCodes {
    NORMAL,
    ALREADY_STARTED, // 1
    START_FAILED;    // 2
  }

  private static String getEnv(String key, String defaultValue) {
    String s = System.getenv(key);
    if (s == null) {
      s = defaultValue;
    }
    return s;
  }
}

// End FineoServer.java
