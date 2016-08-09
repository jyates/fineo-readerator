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
import org.apache.calcite.avatica.jdbc.FineoJdbcMeta;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.dropwizard3.DropwizardMetricsSystemConfiguration;
import org.apache.calcite.avatica.metrics.dropwizard3.DropwizardMetricsSystemFactory;
import org.apache.calcite.avatica.remote.Driver.Serialization;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.ProtobufTranslation;
import org.apache.calcite.avatica.server.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Avatica server for HSQLDB.
 */
public class FineoServer {
  private static final Logger LOG = LoggerFactory.getLogger(FineoServer.class);

  private static final Serialization SER = Serialization.PROTOBUF;

  @Parameter(names = "--org-id", required = true,
             description = "Org ID served by this server. Only 1 org per server allowed.")
  private String org;

  @Parameter(names = {"-p", "--port"},
             description = "Port the server should bind")
  private int port = 0;

  @Parameter(names = "--drill-connection", required = true,
             description = "Connection string for the Drill JDBC driver")
  private String drill = "===UNSPECIFIED===";

  private HttpServer server;
  private final MetricRegistry metrics = new MetricRegistry();


  public void start() {
    if (null != server) {
      LOG.error("The server was already started");
      System.exit(ExitCodes.ALREADY_STARTED.ordinal());
      return;
    }

    try {
      // make sur we have the drill driver
      Class.forName("org.apache.drill.jdbc.Driver");

      // create our wrapping metadata
      DropwizardMetricsSystemConfiguration metricsConf = new DropwizardMetricsSystemConfiguration
        (metrics);
      MetricsSystem system = new DropwizardMetricsSystemFactory().create(metricsConf);
      FineoJdbcMeta meta = new FineoJdbcMeta(drill, system, org);
      LocalService service = new LocalService(meta, system);

      // custom translator so we can interop with AWS
      ProtobufTranslation translator = new AwsStringBytesDecodingTranslator();
      AvaticaProtobufHandler handler =
        new AvaticaProtobufHandler(service, system, null, translator);

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
    if (null != server) {
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

    server.start();

    // Try to clean up when the server is stopped.
    Runtime.getRuntime().addShutdownHook(
      new Thread(new Runnable() {
        @Override
        public void run() {
          LOG.info("Stopping server");
          server.stop();
          LOG.info("Server stopped");
        }
      }));

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
}

// End FineoServer.java
