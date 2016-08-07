package io.fineo.read.jdbc.connection;

import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.UnregisteredDriver;

import java.sql.Connection;
import java.util.Properties;

/**
 * Light wrapper around an avatica connection that
 */
public abstract class FineoConnection implements Connection {

  public FineoConnection(UnregisteredDriver driver, AvaticaFactory factory, String url,
    Properties info) {
    //
  }
}
