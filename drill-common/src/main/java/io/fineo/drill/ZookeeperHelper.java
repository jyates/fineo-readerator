/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fineo.drill;

import com.google.common.base.Throwables;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.util.MiniZooKeeperCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fork of the standard zookeeper helper to allow us to set overrides
 */
public class ZookeeperHelper {
  private static final Logger logger = LoggerFactory.getLogger(ZookeeperHelper.class);
  private final File testDir;
  private final DrillConfig config;
  private final String zkUrl;
  private MiniZooKeeperCluster zkCluster;

  public ZookeeperHelper() {
    this(false);
  }

  public ZookeeperHelper(boolean failureInCancelled) {
    this(new Properties(), failureInCancelled);
  }

  public ZookeeperHelper(Properties overrideProps, boolean failureInCancelled) {
    if (failureInCancelled) {
      overrideProps
        .setProperty("drill.exec.debug.return_error_for_failure_in_cancelled_fragments", "true");
    }

    this.config = DrillConfig.create(overrideProps);
    this.zkUrl = this.config.getString("drill.exec.zk.connect");
    this.testDir = new File("target/test-data");
    if (!this.testDir.exists()) {
      this.testDir.mkdirs();
    }
  }

  public void startZookeeper(int numServers) {
    if (this.zkCluster != null) {
      throw new IllegalStateException("Zookeeper cluster already running");
    } else {
      try {
        this.zkCluster = new MiniZooKeeperCluster();
        this.zkCluster.setDefaultClientPort(Integer.parseInt(this.zkUrl.split(":")[1]));
        this.zkCluster.startup(this.testDir, numServers);
      } catch (InterruptedException | IOException var3) {
        Throwables.propagate(var3);
      }

    }
  }

  public void stopZookeeper() {
    try {
      this.zkCluster.shutdown();
      this.zkCluster = null;
    } catch (IOException e) {
      String message = "Unable to shutdown Zookeeper";
      System.err.println(message);
      logger.warn(message, e);
    }

  }

  public DrillConfig getConfig() {
    return this.config;
  }
}
