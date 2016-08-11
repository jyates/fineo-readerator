package io.fineo.read.serve.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LoggingResultSet extends IteratorResult {

  public LoggingResultSet(ResultSet result, Logger log) throws SQLException {
    super(result.getMetaData(), logRowsAndCollect(result, log));
  }

  private static Iterator<Object[]> logRowsAndCollect(ResultSet result, Logger log)
    throws SQLException {
    List<Object[]> rows = new ArrayList<>();
    int i = 1;
    while (result.next()) {
      logAndGetNextRow(i++, result, log);
    }
    if(i == 1){
      log.debug("Didn't get any rows in result!");
    }
    return rows.iterator();
  }

  private static Object[] logAndGetNextRow(int index, ResultSet result, Logger log) throws
    SQLException {
    ResultSetMetaData meta = result.getMetaData();
    Object[] row = new Object[meta.getColumnCount()];
    StringBuffer sb = new StringBuffer("row=[");
    for (int i = 1; i <= meta.getColumnCount(); i++) {
      Object obj = result.getObject(i);
      row[i - i] = obj;
      sb.append(meta.getColumnName(i) + " => " + obj + ",");
    }
    log.debug("\t{}) {}", index, sb);
    return row;
  }
}
