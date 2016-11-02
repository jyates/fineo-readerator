package io.fineo.read.serve.driver;


import com.google.common.collect.AbstractIterator;
import io.fineo.read.serve.util.IteratorResult;
import io.fineo.read.serve.util.RegexpUtil;
import io.fineo.read.serve.util.SimpleMetadata;
import org.apache.calcite.avatica.AvaticaDatabaseMetaData;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class FineoDatabaseMetaData extends AvaticaDatabaseMetaData {

  private static final Logger LOG = LoggerFactory.getLogger(FineoDatabaseMetaData.class);
  public static final String FINEO_CATALOG = "FINEO";
  static final String FINEO_INFO = "INFO";
  static final String FINEO_SCHEMA = "FINEO";

  static final String TABLE_CATALOG_COLUMN = "TABLE_CAT";
  static final String TABLE_SCHEMA_COLUMN = "TABLE_SCHEM";
  static final String TABLE_COLUMN = "TABLE";
  static final String COLUMN_COLUMN = "COLUMN_NAME";
  private final FineoConnection conn;

  protected FineoDatabaseMetaData(FineoConnection connection) {
    super(connection);
    this.conn = connection;
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    return stringResults(TABLE_CATALOG_COLUMN, FINEO_CATALOG);
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    return getDelegateConnection().getMetaData().getSchemas(catalog, schemaPattern);
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return getDelegateConnection().getMetaData().getSchemas();
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    return stringResults("TABLE_TYPE", "TABLE");
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
    String[] types) throws SQLException {
    if (!matchesCatalogAndSchema(catalog, schemaPattern)) {
      LOG.debug("Skipping actual read - schema/catalog pattern doesn't match!");
      return stringResults(TABLE_COLUMN);
    }

    // Fineo translator in Drill execution ensures that we match the Fineo schema/catalog
    return getDelegateConnection().getMetaData().getTables(catalog, schemaPattern,
      tableNamePattern, types);
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
    String columnNamePattern) throws SQLException {
    if (!matchesCatalogAndSchema(catalog, schemaPattern)) {
      return stringResults(COLUMN_COLUMN);
    }
    return getDelegateConnection().getMetaData().getColumns(catalog, schemaPattern,
      tableNamePattern, columnNamePattern);
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    Connection internal = conn.getMetaConnection();
    return internal.getMetaData().getTypeInfo();
  }

  private ResultSet stringResults(String columnName, String... values) throws SQLException {
    List<ColumnMetaData> meta = new ArrayList<>(values.length);
    meta.add(MetaImpl.columnMetaData(columnName, 1, String.class));
    return IteratorResult.fulfilledResult(new SimpleMetadata(FINEO_CATALOG, FINEO_SCHEMA,
        FINEO_INFO, meta),
      new AbstractIterator<Object[]>() {
        int index = 0;

        @Override
        protected Object[] computeNext() {
          if (index >= values.length) {
            endOfData();
            return null;
          }
          return new Object[]{values[index++]};
        }
      }, this.getConnection());
  }

  private boolean matchesCatalogAndSchema(String catalog, String schemaPattern) {
    return matches(catalog, FINEO_CATALOG) && matches(schemaPattern, FINEO_SCHEMA);
  }

  private boolean matches(String pattern, String text) {
    if (pattern == null) {
      return true;
    }
    return Pattern.matches(RegexpUtil.sqlToRegexLike(pattern), text);
  }

  private Connection getDelegateConnection() throws SQLException {
    return conn.getMetaConnection();
  }
}
