package io.fineo.read.drill.e2e.command;

import com.beust.jcommander.ParametersDelegate;
import io.fineo.e2e.options.LocalSchemaStoreOptions;
import io.fineo.read.drill.e2e.commands.Command;
import io.fineo.read.drill.e2e.options.DrillArguments;
import io.fineo.read.drill.e2e.options.JdbcOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

import static java.lang.String.format;

public class ReadCommand extends Command {
  private static final Logger LOG = LoggerFactory.getLogger(ReadCommand.class);

  @ParametersDelegate
  private final JdbcOption jdbc = new JdbcOption();
  private Reader reader;

  public ReadCommand(DrillArguments opts, LocalSchemaStoreOptions storeOptions) {
    super(opts, storeOptions);
  }

  public void setReader(Reader read) {
    this.reader = read;
  }

  @Override
  public void run() throws Exception {
    String from = format(" FROM %s", opts.metric.get());
    String stmt = "SELECT *" + from + " ORDER BY `timestamp` ASC";
    runQuery(stmt);
  }

  @Override
  protected Connection getConnection() throws Exception {
    Class.forName(reader.getDriver());
    String url = reader.getJdbcConnection(jdbc.getUrl());
    LOG.info("Connecting to {} with url: {}", reader.getDriver(), url);
    return DriverManager.getConnection(url, reader
      .loadProperties(opts));
  }
}
