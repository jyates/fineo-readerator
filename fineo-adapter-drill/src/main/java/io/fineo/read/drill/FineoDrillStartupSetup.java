package io.fineo.read.drill;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Setup the system options that we need to override since Drill startup doesn't include them,
 * for some reason.
 */
public class FineoDrillStartupSetup {

  private static final List<String> options = newArrayList(
    "`exec.enable_union_type` = true",
    "`drill.exec.storage.file.partition.column.label` = '_fd'"
  );

  private final Connection conn;

  public FineoDrillStartupSetup(Connection conn) {
    this.conn = conn;
  }

  public void run() throws SQLException {
    for (String option : options) {
      conn.createStatement().execute("ALTER SYSTEM SET " + option);
    }
  }
}
