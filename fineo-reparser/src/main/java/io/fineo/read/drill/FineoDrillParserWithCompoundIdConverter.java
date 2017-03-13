package io.fineo.read.drill;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p>
 * Copied from Drill 1.6.0 to support our custom org-prefixing parser
 */

import io.fineo.read.calcite.parser.FineoParseImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.drill.exec.planner.sql.parser.CompoundIdentifierConverter;

import java.io.Reader;

public class FineoDrillParserWithCompoundIdConverter extends FineoParseImpl {

  private final String org;

  public FineoDrillParserWithCompoundIdConverter(Reader stream, String orgId) {
    super(stream);
    this.org = orgId;
  }

  /**
   * Enable Drill-like conversions of nested expressions. See {@link CompoundIdentifierConverter}
   * docs for more information
   * @return shuttle to update expression
   */
  protected SqlVisitor<SqlNode> createConverter() {
    return new CompoundIdentifierConverter();
  }

  protected SqlVisitor<SqlNode> forceApiKeyWhere(){
    return new FineoErrorWhereForce(this.org);
  }

  @Override
  public SqlNode parseSqlExpressionEof() throws Exception {
    SqlNode originalSqlNode = super.parseSqlExpressionEof();
    return originalSqlNode.accept(createConverter()).accept(forceApiKeyWhere());
  }

  @Override
  public SqlNode parseSqlStmtEof() throws Exception {
    SqlNode originalSqlNode = super.parseSqlStmtEof();
    return originalSqlNode.accept(createConverter()).accept(forceApiKeyWhere());
  }

  public static SqlParserImplFactory Factory(String org) {
    return stream -> {
      FineoDrillParserWithCompoundIdConverter parserImpl =
        new FineoDrillParserWithCompoundIdConverter(stream, org);
      parserImpl.setOrg(org);
      return parserImpl;
    };
  }
}
