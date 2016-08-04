package io.fineo.read.drill.e2e;

import io.fineo.e2e.options.LocalSchemaStoreOptions;
import io.fineo.read.drill.e2e.commands.Command;
import io.fineo.read.drill.e2e.options.DrillArguments;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import static java.lang.String.format;

public class RemoteReadCommand extends Command {

  private final JdbcOption jdbc;

  public RemoteReadCommand(DrillArguments opts, LocalSchemaStoreOptions storeOptions,
    JdbcOption jdbc) throws ClassNotFoundException {
    super(opts, storeOptions);
    this.jdbc = jdbc;
  }

  @Override
  public void run() throws Exception {
    String from = format(" FROM %s", opts.metric.get());
    String stmt = "SELECT *" + from + " ORDER BY `timestamp` ASC";
    runQuery(stmt);
  }

  @Override
  protected Connection getConnection() throws Exception {
    Class.forName("org.apache.calcite.avatica.remote.Driver");
    Properties props = new Properties();
    props.put("COMPANY_KEY", opts.org.get());
    return DriverManager.getConnection(jdbc.getUrl(), props);
  }
}
