package io.fineo.read.serve.driver;


import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.fineo.read.serve.util.FulfilledStatement;
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

  @Override
  public ResultSet getCatalogs() throws SQLException {
    return stringResults(TABLE_CATALOG_COLUMN, FINEO_CATALOG);
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    return stringResults(TABLE_SCHEMA_COLUMN,
      matchesCatalogAndSchema(catalog, schemaPattern) ?
      new String[]{FINEO_SCHEMA} : new String[0]);
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    List<ColumnMetaData> meta = new ArrayList<>(2);
    meta.add(MetaImpl.columnMetaData(TABLE_CATALOG_COLUMN, 1, String.class));
    meta.add(MetaImpl.columnMetaData(TABLE_SCHEMA_COLUMN, 1, String.class));
    return wrap(new IteratorResult(new SimpleMetadata(FINEO_CATALOG, FINEO_SCHEMA,
      FINEO_INFO, meta), ImmutableList.of(new Object[]{FINEO_CATALOG, FINEO_SCHEMA}).iterator()));
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    return stringResults("TABLE_TYPE", "TABLE");
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
    String[] types) throws SQLException {
    if (!matchesCatalogAndSchema(catalog, schemaPattern)) {
      return stringResults(TABLE_COLUMN);
    }

    Connection internal = conn.getMetaConnection();
    return wrap(new TableMetadataTranslatingResultSet(FINEO_SCHEMA, FINEO_CATALOG,
      internal.getMetaData()
              .getTables(conn.getDelegateCatalog(), getInternalSchema(), tableNamePattern, types)));
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
    String columnNamePattern) throws SQLException {
    if (!matchesCatalogAndSchema(catalog, schemaPattern)) {
      return stringResults(COLUMN_COLUMN);
    }
    return super
      .getColumns(conn.getDelegateCatalog(), getInternalSchema(), tableNamePattern,
        columnNamePattern);
  }

  private ResultSet stringResults(String columnName, String... values) throws SQLException {
    List<ColumnMetaData> meta = new ArrayList<>(values.length);
    meta.add(MetaImpl.columnMetaData(columnName, 1, String.class));
    return wrap(new IteratorResult(new SimpleMetadata(FINEO_CATALOG, FINEO_SCHEMA,
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
      }));
  }

  /**
   * Ensure the output result is wrapped in a {@link FulfilledStatement} to make jdbc meta
   * wrapper happy
   */
  private ResultSet wrap(IteratorResult result) throws SQLException {
    new FulfilledStatement(result, this.getConnection());
    return result;
  }

  private boolean matchesCatalogAndSchema(String catalog, String schemaPattern) {
    return (catalog == null || FINEO_CATALOG.equals(catalog)) &&
           (schemaPattern == null || matchesSchema(schemaPattern));
  }

  private boolean matchesSchema(String schemaPattern) {
    if (schemaPattern == null) {
      return true;
    }
    Pattern pattern = Pattern.compile(schemaPattern);
    return pattern.matcher(FINEO_SCHEMA).matches();
  }

  private String getInternalSchema() {
    return format("%s.%s", FINEO_DRILL_SCHEMA_NAME, getOrg());
  }

  private String getOrg() {
    return conn.getOrg();
  }
}
