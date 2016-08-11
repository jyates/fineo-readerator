package io.fineo.read.serve.driver;

import io.fineo.read.serve.util.LoggingResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class FineoStatement extends AvaticaStatement {

  private static final Logger LOG = LoggerFactory.getLogger(FineoStatement.class);

  private ResultSet result;

  public FineoStatement(FineoConnection connection, Meta.StatementHandle h, int resultSetType,
    int resultSetConcurrency, int resultSetHoldability) {
    super(connection, h, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  public boolean execute(String sql) throws SQLException {
    checkNotPreparedOrCallable("execute(String)");
    setResult(executeQuery(sql));
    // Result set is null for DML or DDL.
    // Result set is closed if user cancelled the query.
    return result != null && !result.isClosed();
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    try {
      return setResult(getConn().prepareAndExecute(this, sql, maxRowCount));
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

  // copied from avatica.AvaticaStatement
  private void checkNotPreparedOrCallable(String s) throws SQLException {
    if (this instanceof PreparedStatement
        || this instanceof CallableStatement) {
      throw connection.helper.createException("Cannot call " + s
                                              + " on prepared or callable statement");
    }
  }

  public ResultSet setResult(ResultSet result) throws SQLException {
    if(LOG.isDebugEnabled()){
      result = new LoggingResultSet(result, LOG).withStatement(this);
    }
    this.result = result;
    return result;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return this.result;
  }
}
