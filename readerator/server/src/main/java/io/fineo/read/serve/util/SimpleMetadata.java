package io.fineo.read.serve.util;

import org.apache.calcite.avatica.ColumnMetaData;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 *
 */
public class SimpleMetadata implements ResultSetMetaData {

  private final String catalog;
  private final String schema;
  private final String table;
  private final List<ColumnMetaData> columns;

  public SimpleMetadata(String catalog, String schema, String table, List<ColumnMetaData> columns){
    this.catalog = catalog;
    this.schema = schema;
    this.table = table;
    this.columns = columns;
  }
  @Override
  public int getColumnCount() throws SQLException {
    return columns.size();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return col(column).autoIncrement;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return col(column).caseSensitive;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return col(column).searchable;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return col(column).searchable;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return ResultSetMetaData.columnNullableUnknown;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return col(column).signed;
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return col(column).displaySize;
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return col(column).label;
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return getColumnLabel(column);
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    return schema;
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    return col(column).precision;
  }

  @Override
  public int getScale(int column) throws SQLException {
    return col(column).scale;
  }

  @Override
  public String getTableName(int column) throws SQLException {
    return table;
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    return catalog;
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    return col(column).type.id;
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return col(column).type.name;
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    return col(column).columnClassName;
  }

  private ColumnMetaData col(int column) {
    return this.columns.get(column - 1);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}
