package io.fineo.read.drill.e2e;

import com.beust.jcommander.JCommander;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import io.fineo.drill.LocalDrillCluster;
import io.fineo.e2e.options.LocalSchemaStoreOptions;
import io.fineo.read.drill.BootstrapFineo;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

/**
 * End-To-End entry point to run against an in-memory drill cluster
 */
public class DrillLocalClusterE2E {

  private static final Joiner AND = Joiner.on(" AND ");

  public static void main(String[] args) throws Exception {
    DrillReadArguments drillArgs = new DrillReadArguments();
    LocalSchemaStoreOptions storeArgs = new LocalSchemaStoreOptions();
    JCommander jc = new JCommander(new Object[]{storeArgs, drillArgs});
    jc.parse(args);

    // start the cluster
    LocalDrillCluster drill = new LocalDrillCluster(1);

    // ensure the fineo plugin is setup
    BootstrapFineo bootstrap = new BootstrapFineo();
    BootstrapFineo.DrillConfigBuilder builder =
      bootstrap.builder()
               .withLocalDynamo("http://" + storeArgs.host + ":" + storeArgs.port)
               .withRepository(storeArgs.schemaTable)
               .withOrgs(drillArgs.orgId);
    builder.withLocalSource(new File(drillArgs.inputDir));
    if (!bootstrap.strap(builder)) {
      throw new RuntimeException("Bootstrap step failed!");
    }

    String from = format(" FROM fineo.%s.%s", drillArgs.orgId, drillArgs.metricType);
    String[] wheres = null;
    String where = wheres == null ? "" : " WHERE " + AND.join(wheres);
    String stmt = "SELECT *" + from + where + " ORDER BY `timestamp` ASC";

    try (
      Connection conn = drill.getConnection();
      ResultSet results = conn.createStatement().executeQuery(stmt);
      FileOutputStream os = new FileOutputStream(drillArgs.output);
      BufferedOutputStream bos = new BufferedOutputStream(os)
    ) {
      List<Map<String, Object>> events = new ArrayList<>();
      while (results.next()) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < results.getMetaData().getColumnCount(); i++) {
          String col = results.getMetaData().getColumnName(i + 1);
          map.put(col, results.getObject(col));
        }
        events.add(map);
      }

      ObjectMapper mapper = new ObjectMapper();
      mapper.writeValue(bos, events);
    }

    drill.shutdown();
  }

}
