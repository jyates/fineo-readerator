package io.fineo.read.serve.util;

import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;

/**
 *
 */
public class FineoResultSetMetaData extends AvaticaResultSetMetaData {
  public FineoResultSetMetaData(AvaticaStatement statement,
    Object query, Meta.Signature signature) {
    super(statement, query, signature);
  }
}
