package io.fineo.read.serve.util;


import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * Iterator of a result
 */
public class IteratorResult implements ResultSet {
  private final Iterator<Object[]> rows;
  private final ResultSetMetaData meta;
  private Object[] row;
  private int lastIndex = -1;
  private Statement statement;

  public static IteratorResult fulfilledResult(ResultSetMetaData metaData, Iterator<Object[]> rows,
    Connection conn){
    //create the simple result
    IteratorResult result = new IteratorResult(metaData, rows);
    // fir the result a statement, bound to a connection. Necessary to ensure that the result is
    // actually created 'correctly'
    new FulfilledStatement(result, conn);
    return result;
  }

  IteratorResult(ResultSetMetaData metaData, Iterator<Object[]> rows) {
    this.meta = metaData;
    this.rows = rows;
  }

  IteratorResult withStatement(Statement statement) {
    this.statement = statement;
    return this;
  }

  @Override
  public boolean next() throws SQLException {
    if (!rows.hasNext()) {
      return false;
    }
    row = rows.next();
    return true;
  }

  @Override
  public void close() throws SQLException {
    if (rows instanceof Closeable) {
      try {
        ((Closeable) rows).close();
      } catch (IOException e) {
        throw new SQLException(e);
      }
    }
    Objects.requireNonNull(this.statement, "Statement for result iterator was never set!");
    if (this.statement.isCloseOnCompletion()) {
      this.statement.close();
    }
  }

  @Override
  public boolean wasNull() throws SQLException {
    if (lastIndex < 0) {
      throw new SQLException("Must ask about a column before checking if it was null!");
    }
    return row[lastIndex] == null;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return getObject(columnIndex, String.class);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    return getObject(columnIndex, Boolean.class);
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    return getObject(columnIndex, Byte.class);
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    return getObject(columnIndex, Short.class);
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    return getObject(columnIndex, Integer.class);
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    return getObject(columnIndex, Long.class);
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    return getObject(columnIndex, Float.class);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    return getObject(columnIndex, Double.class);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    BigDecimal bd = getObject(columnIndex, BigDecimal.class);
    bd.setScale(scale);
    return bd;
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    return getObject(columnIndex, byte[].class);
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    return getObject(columnIndex, Date.class);
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return getObject(columnIndex, Time.class);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return getObject(columnIndex, Timestamp.class);
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    return getObject(columnIndex, InputStream.class);
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return getObject(columnLabel, String.class);
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return getObject(columnLabel, Boolean.class);
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return getObject(columnLabel, Byte.class);
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return getObject(columnLabel, Short.class);
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return getObject(columnLabel, Integer.class);
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return getObject(columnLabel, Long.class);
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return getObject(columnLabel, Float.class);
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return getObject(columnLabel, Double.class);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    BigDecimal bd = getObject(columnLabel, BigDecimal.class);
    bd.setScale(scale);
    return bd;
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return getObject(columnLabel, byte[].class);
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return getObject(columnLabel, Date.class);
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return getObject(columnLabel, Time.class);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getObject(columnLabel, Timestamp.class);
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    return getObject(columnLabel, InputStream.class);
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override
  public String getCursorName() throws SQLException {
    return "iterator-curstor";
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return meta;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    lastIndex = columnIndex;
    return row[columnIndex - 1];
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    return 0;
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return false;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    return false;
  }

  @Override
  public boolean isFirst() throws SQLException {
    return false;
  }

  @Override
  public boolean isLast() throws SQLException {
    return false;
  }

  @Override
  public void beforeFirst() throws SQLException {

  }

  @Override
  public void afterLast() throws SQLException {

  }

  @Override
  public boolean first() throws SQLException {
    return false;
  }

  @Override
  public boolean last() throws SQLException {
    return false;
  }

  @Override
  public int getRow() throws SQLException {
    return 0;
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    return false;
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    return false;
  }

  @Override
  public boolean previous() throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {

  }

  @Override
  public int getFetchDirection() throws SQLException {
    return 0;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {

  }

  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  @Override
  public int getType() throws SQLException {
    return 0;
  }

  @Override
  public int getConcurrency() throws SQLException {
    return 0;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    return false;
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    return false;
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {

  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {

  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {

  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {

  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {

  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {

  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {

  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {

  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {

  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {

  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {

  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {

  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {

  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {

  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {

  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {

  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {

  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {

  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {

  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {

  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {

  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {

  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {

  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {

  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {

  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {

  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {

  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {

  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {

  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {

  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {

  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {

  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {

  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {

  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
    throws SQLException {

  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
    throws SQLException {

  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {

  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {

  }

  @Override
  public void insertRow() throws SQLException {

  }

  @Override
  public void updateRow() throws SQLException {

  }

  @Override
  public void deleteRow() throws SQLException {

  }

  @Override
  public void refreshRow() throws SQLException {

  }

  @Override
  public void cancelRowUpdates() throws SQLException {

  }

  @Override
  public void moveToInsertRow() throws SQLException {

  }

  @Override
  public void moveToCurrentRow() throws SQLException {

  }

  @Override
  public Statement getStatement() throws SQLException {
    return this.statement;
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    return null;
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    return null;
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {

  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {

  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {

  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {

  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {

  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {

  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {

  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {

  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {

  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {

  }

  @Override
  public int getHoldability() throws SQLException {
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {

  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {

  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {

  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {

  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {

  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {

  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
    throws SQLException {

  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {

  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {

  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
    throws SQLException {

  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
    throws SQLException {

  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
    throws SQLException {

  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
    throws SQLException {

  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
    throws SQLException {

  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {

  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {

  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {

  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {

  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {

  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {

  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {

  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {

  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {

  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {

  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {

  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {

  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {

  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {

  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {

  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {

  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {

  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {

  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    return (T) getObject(columnIndex);
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    int i = 1;
    for (; i <= getMetaData().getColumnCount(); i++) {
      if (columnLabel.equals(getMetaData().getColumnName(i))) {
        break;
      }
    }
    return getObject(i, type);
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
