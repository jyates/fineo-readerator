package io.fineo.read.serve.driver;

import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.NoSuchStatementException;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 */
public class FineoStatement extends AvaticaStatement {
  public FineoStatement(FineoConnection connection, Meta.StatementHandle h, int resultSetType,
    int resultSetConcurrency, int resultSetHoldability) {
    super(connection, h, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    try {
      return getConn().prepareAndExecute(this, sql, maxRowCount);
    } catch (NoSuchStatementException e) {
      throw new SQLException(e);
    }
  }

  @Override
  protected void executeInternal(String sql) throws SQLException {
    throw new UnsupportedOperationException("Should use #executeQuery() instead");
  }

  private FineoConnection getConn() {
    return (FineoConnection) this.connection;
  }
}
