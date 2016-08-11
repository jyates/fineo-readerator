package io.fineo.read.serve.driver;


import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.Meta;

import java.io.InputStream;
import java.io.Reader;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;

class FineoPreparedStatement extends AvaticaPreparedStatement {
  protected FineoPreparedStatement(AvaticaConnection connection,
    Meta.StatementHandle h,
    Meta.Signature signature,
    int resultSetType,
    int resultSetConcurrency,
    int resultSetHoldability) throws SQLException {
    super(connection, h, signature, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    this.getSite(parameterIndex).setRowId(x);
  }

  public void setNString(int parameterIndex, String value) throws SQLException {
    this.getSite(parameterIndex).setNString(value);
  }

  public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
    this.getSite(parameterIndex).setNCharacterStream(value, length);
  }

  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    this.getSite(parameterIndex).setNClob(value);
  }

  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    this.getSite(parameterIndex).setClob(reader, length);
  }

  public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
    this.getSite(parameterIndex).setBlob(inputStream, length);
  }

  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    this.getSite(parameterIndex).setNClob(reader, length);
  }

  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    this.getSite(parameterIndex).setSQLXML(xmlObject);
  }

  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    this.getSite(parameterIndex).setAsciiStream(x, length);
  }

  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    this.getSite(parameterIndex).setBinaryStream(x, length);
  }

  public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
    this.getSite(parameterIndex).setCharacterStream(reader, length);
  }

  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    this.getSite(parameterIndex).setAsciiStream(x);
  }

  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    this.getSite(parameterIndex).setBinaryStream(x);
  }

  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    this.getSite(parameterIndex).setCharacterStream(reader);
  }

  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    this.getSite(parameterIndex).setNCharacterStream(value);
  }

  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    this.getSite(parameterIndex).setClob(reader);
  }

  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    this.getSite(parameterIndex).setBlob(inputStream);
  }

  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    this.getSite(parameterIndex).setNClob(reader);
  }
}
