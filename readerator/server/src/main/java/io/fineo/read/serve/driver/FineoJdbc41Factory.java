package io.fineo.read.serve.driver;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaSpecificDatabaseMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.UnregisteredDriver;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

public class FineoJdbc41Factory implements AvaticaFactory {

  private final int major;
  private final int minor;

  /**
   * Creates a JDBC factory.
   */
  public FineoJdbc41Factory() {
    this(4, 1);
  }

  /**
   * Creates a JDBC factory with given major/minor version number.
   */
  protected FineoJdbc41Factory(int major, int minor) {
    this.major = major;
    this.minor = minor;
  }

  @Override
  public int getJdbcMajorVersion() {
    return major;
  }

  @Override
  public int getJdbcMinorVersion() {
    return minor;
  }

  @Override
  public AvaticaConnection newConnection(UnregisteredDriver driver, AvaticaFactory factory,
    String url, Properties info) throws SQLException {
    FineoServerDriver fd = (FineoServerDriver) driver;
    FineoJdbc41Factory ff = (FineoJdbc41Factory) factory;
    return new FineoConnection(fd, ff, url, info);
  }

  @Override
  public AvaticaStatement newStatement(AvaticaConnection connection, Meta.StatementHandle h,
    int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return new FineoStatement((FineoConnection) connection, h, resultSetType,
      resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public AvaticaPreparedStatement newPreparedStatement(AvaticaConnection connection,
    Meta.StatementHandle h, Meta.Signature signature, int resultSetType, int resultSetConcurrency,
    int resultSetHoldability) throws SQLException {
    return new FineoPreparedStatement(connection, h, signature, resultSetType, resultSetConcurrency,
      resultSetHoldability);
  }

  @Override
  public AvaticaResultSet newResultSet(AvaticaStatement statement, QueryState state,
    Meta.Signature signature, TimeZone timeZone, Meta.Frame firstFrame) throws SQLException {
    final ResultSetMetaData metaData =
      newResultSetMetaData(statement, signature);
    return new AvaticaResultSet(statement, state, signature, metaData, timeZone,
      firstFrame);
  }

  @Override
  public AvaticaSpecificDatabaseMetaData newDatabaseMetaData(AvaticaConnection connection) {
    return new FineoDatabaseMetaData((FineoConnection) connection);
  }

  @Override
  public ResultSetMetaData newResultSetMetaData(AvaticaStatement statement,
    Meta.Signature signature) throws SQLException {
    return new AvaticaResultSetMetaData(statement, null, signature);
  }
}
