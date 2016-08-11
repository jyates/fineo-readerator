package io.fineo.read.serve.util;


import com.google.common.collect.AbstractIterator;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Translate the result set that has internal catalog and schema into the external names
 */
public class TableMetadataTranslatingResultSet extends IteratorResult {

  private final ResultSet rs;

  public TableMetadataTranslatingResultSet(String schema, String catalog, ResultSet rs)
    throws SQLException {
    super(rs.getMetaData(), new TranslatingIter(rs, rs.getMetaData(), schema, catalog));
    this.rs = rs;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return rs.getMetaData();
  }

  private static class TranslatingIter extends AbstractIterator<Object[]> {
    private final ResultSet rs;
    private final ResultSetMetaData meta;
    private final Object[] columns;
    private final String catalog;
    private final String schema;

    private TranslatingIter(ResultSet result, ResultSetMetaData meta, String catalog, String schema)
      throws SQLException {
      this.rs = result;
      this.meta = meta;
      columns = new Object[meta.getColumnCount()];
      this.catalog = catalog;
      this.schema = schema;
    }

    @Override
    protected Object[] computeNext() {
      try {
        if (!rs.next()) {
          endOfData();
          return null;
        }

        for (int i = 0; i < columns.length; i++) {
          String name = meta.getColumnName(i + 1);
          if ("TABLE_CAT".equals(name)) {
            columns[i] = catalog;
          } else if ("TABLE_SCHEM".equals(name)) {
            columns[i] = schema;
          } else {
            columns[i] = rs.getObject(i + 1);
          }
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }

      return columns;
    }
  }
}
