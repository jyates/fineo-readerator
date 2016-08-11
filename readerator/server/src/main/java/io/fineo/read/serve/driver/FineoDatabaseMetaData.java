package io.fineo.read.serve.driver;


import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.fineo.read.serve.util.IteratorResult;
import io.fineo.read.serve.util.SimpleMetadata;
import io.fineo.read.serve.util.TableMetadataTranslatingResultSet;
import org.apache.calcite.avatica.AvaticaDatabaseMetaData;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static java.lang.String.format;

public class FineoDatabaseMetaData extends AvaticaDatabaseMetaData {

  static final String FINEO_CATALOG = "FINEO";
  static final String FINEO_INFO = "INFO";
  static final String FINEO_SCHEMA = "FINEO";

  static final String TABLE_CATALOG_COLUMN = "TABLE_CAT";
  static final String TABLE_SCHEMA_COLUMN = "TABLE_SCHEM";
  static final String TABLE_COLUMN = "TABLE";
  static final String COLUMN_COLUMN = "COLUMN_NAME";

  // TODO this is common with the fineo-adapter-drill/.../FineoInternalProperties class, but
  // don't have the time right now to ensure they match
  static final String FINEO_DRILL_SCHEMA_NAME = "fineo";
  private final FineoConnection conn;

  protected FineoDatabaseMetaData(FineoConnection connection) {
    super(connection);
    this.conn = connection;
  }

  private String adjust(String org, String pattern) {
    if (pattern == null) {
      pattern = "";
    }
    return org + "." + pattern;
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
    String[] types) throws SQLException {
    if (!matchesSchema(schemaPattern) && FINEO_CATALOG.equals(catalog)) {
      return stringResults(TABLE_COLUMN);
    }

    Connection internal = conn.getMetaConnection();
    return new TableMetadataTranslatingResultSet(FINEO_SCHEMA, FINEO_CATALOG, internal
      .getMetaData().getTables(conn.getCatalog(), getInternalSchema(), tableNamePattern, types));
  }

  private String getInternalSchema() {
    return format("%s.%s", FINEO_DRILL_SCHEMA_NAME, getOrg());
  }

  private String getOrg() {
    return conn.getOrg();
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    return stringResults(TABLE_SCHEMA_COLUMN,
      matchesSchema(schemaPattern) ?
      new String[]{FINEO_SCHEMA} : new String[0]);
  }

  private boolean matchesSchema(String schemaPattern) {
    if (schemaPattern == null) {
      return true;
    }
    Pattern pattern = Pattern.compile(schemaPattern);
    return pattern.matcher(FINEO_SCHEMA).matches();
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    List<ColumnMetaData> meta = new ArrayList<>(2);
    meta.add(MetaImpl.columnMetaData(TABLE_CATALOG_COLUMN, 1, String.class));
    meta.add(MetaImpl.columnMetaData(TABLE_SCHEMA_COLUMN, 1, String.class));
    return new IteratorResult(new SimpleMetadata(FINEO_CATALOG, FINEO_SCHEMA, FINEO_INFO, meta),
      ImmutableList.of(new Object[]{FINEO_CATALOG, FINEO_SCHEMA}).iterator());
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    return stringResults(TABLE_CATALOG_COLUMN, FINEO_CATALOG);
  }

  private ResultSet stringResults(String columnName, String... values) {
    List<ColumnMetaData> meta = new ArrayList<>(values.length);
    meta.add(MetaImpl.columnMetaData(columnName, 1, String.class));
    return new IteratorResult(new SimpleMetadata(FINEO_CATALOG, FINEO_SCHEMA, FINEO_INFO, meta),
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
      });
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
    String columnNamePattern) throws SQLException {
    if (!matchesSchema(schemaPattern)) {
      stringResults(COLUMN_COLUMN);
    }
    return super
      .getColumns(catalog, schemaPattern, adjust(getOrg(), tableNamePattern), columnNamePattern);
  }
}
