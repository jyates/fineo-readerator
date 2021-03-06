<#-- Licensed to the Apache Software Foundation (ASF) under one or more contributor
  license agreements. See the NOTICE file distributed with this work for additional
  information regarding copyright ownership. The ASF licenses this file to
  You under the Apache License, Version 2.0 (the "License"); you may not use
  this file except in compliance with the License. You may obtain a copy of
  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
  by applicable law or agreed to in writing, software distributed under the
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
  OF ANY KIND, either express or implied. See the License for the specific
  language governing permissions and limitations under the License.

  Copied from Drill java-exec/src/main/codegen/includes/parserImpls
  -->
<#--
  Add implementations of additional parser statements here.
  Each implementation should return an object of SqlNode type.
-->

/**
 * Parses statement SHOW {DATABASES | SCHEMAS} [LIKE 'pattern' | WHERE expr]
 */
SqlNode SqlShowSchemas() :
{
    SqlParserPos pos;
    SqlNode likePattern = null;
    SqlNode where = null;
}
{
    <SHOW> { pos = getPos(); }
    (<DATABASES> | <SCHEMAS>)
    [
        <LIKE> { likePattern = StringLiteral(); }
        |
        <WHERE> { where = Expression(ExprContext.ACCEPT_SUBQUERY); }
    ]
    {
        return new SqlShowSchemas(pos, likePattern, where);
    }
}

/**
 * Parses statement
 *   SHOW TABLES [{FROM | IN} db_name] [LIKE 'pattern' | WHERE expr]
 */
SqlNode SqlShowTables() :
{
    SqlParserPos pos;
    SqlIdentifier db = null;
    SqlNode likePattern = null;
    SqlNode where = null;
}
{
    <SHOW> { pos = getPos(); }
    <TABLES>
    [
        (<FROM> | <IN>) { db = CompoundIdentifier(); }
    ]
    [
        <LIKE> { likePattern = StringLiteral(); }
        |
        <WHERE> { where = Expression(ExprContext.ACCEPT_SUBQUERY); }
    ]
    {
        return new io.fineo.read.parse.SqlShowTables(pos, db, likePattern, where);
    }
}

/**
 * Parses statement
 *    USE <schema name>
 * But replaces the schema name with fineo.<org id> since that is the only schema we current support
 */
SqlNode SqlUseSchema():
{
    DrillCompoundIdentifier.Builder builder = DrillCompoundIdentifier.newBuilder();
    SqlIdentifier schema;
    SqlParserPos pos;
}
{
    <USE> { pos = getPos(); }
    schema = CompoundIdentifier()
    {
      builder.addString("fineo", schema.getComponentParserPosition(0));
      builder.addString(this.org, schema.getComponentParserPosition(0));
      // schema can only ever be set to fineo.<org>
      schema = builder.build();
      return new SqlUseSchema(pos, schema);
    }
}

/**
 * Parses statement
 *   { DESCRIBE | DESC } tblname [col_name | wildcard ]
 */
SqlNode SqlDescribeTable() :
{
    SqlParserPos pos;
    SqlIdentifier table;
    SqlIdentifier column = null;
    SqlNode columnPattern = null;
}
{
    (<DESCRIBE> | <DESC>) { pos = getPos(); }
    table = CompoundIdentifier()
    (
        column = CompoundIdentifier()
        |
        columnPattern = StringLiteral()
        |
        E()
    )
    {
        return new SqlDescribeTable(pos, table, column, columnPattern);
    }
}

