package io.fineo.read.drill;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.util.SqlVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class FineoErrorWhereForce extends SqlShuttle {
  public static final String APIKEY_FIELD = "apikey";
  private String[] errorsPrefix = new String[]{"fineo", "errors"};
  private final String org;
//  private boolean isError = false;

  public FineoErrorWhereForce(String org) {
    this.org = org;
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    return id;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    // check to see if we found an error marker
    if (!(call instanceof SqlSelect)) {
      return super.visit(call);
    }

    SqlSelect select = (SqlSelect) call;
    if (select.getFrom() == null || (select.getFrom() instanceof SqlBasicCall)) {
      return super.visit(call);
    }
    SqlIdentifier from = (SqlIdentifier) select.getFrom();
    if (!isError(from)) {
      return super.visit(call);
    }

    // ok, we definitely have a read of an error table. Do we have a where clause?
    SqlBasicCall where = (SqlBasicCall) select.getWhere();
    // create the where operands
    List<SqlNode> operands = new ArrayList<>();
    SqlParserPos opPosition = new SqlParserPos(0, 0);
    operands.add(new SqlIdentifier(APIKEY_FIELD, opPosition));
    operands.add(SqlLiteral.createCharString(org, opPosition));
    SqlBasicCall equals =
      new SqlBasicCall(SqlStdOperatorTable.EQUALS, operands.toArray(new SqlNode[0]), opPosition);
    if (where == null) {
      select.setWhere(equals);
    } else {
      select.setWhere(new SqlBasicCall(SqlStdOperatorTable.AND, new SqlNode[]{where, equals}, opPosition));
    }
    return super.visit(select);
  }

  private boolean isError(SqlIdentifier identifier) {
    if (this.errorsPrefix.length > identifier.names.size()) {
      return false;
    }
    for (int i = 0; i < this.errorsPrefix.length; i++) {
      if (!this.errorsPrefix[i].equals(identifier.names.get(i))) {
        return false;
      }
    }
    return true;
  }


  private class ComplexExpressionAware implements ArgHandler<SqlNode> {
    boolean update;
    SqlNode[] clonedOperands;
    private final SqlCall call;

    public ComplexExpressionAware(SqlCall call) {
      this.call = call;
      this.update = false;
      final List<SqlNode> operands = call.getOperandList();
      this.clonedOperands = operands.toArray(new SqlNode[operands.size()]);
    }

    @Override
    public SqlNode result() {
      if (update) {
        return call.getOperator().createCall(
          call.getFunctionQuantifier(),
          call.getParserPosition(),
          clonedOperands);
      } else {
        return call;
      }
    }

    @Override
    public SqlNode visitChild(
      SqlVisitor<SqlNode> visitor,
      SqlNode expr,
      int i,
      SqlNode operand) {
      if (operand == null) {
        return null;
      }

//      boolean localEnableComplex = enableComplex;
//      if(rewriteTypes != null){
//        switch(rewriteTypes[i]){
//          case DISABLE:
//            enableComplex = false;
//            break;
//          case ENABLE:
//            enableComplex = true;
//        }
//      }
      SqlNode newOperand = operand.accept(FineoErrorWhereForce.this);
//      enableComplex = localEnableComplex;
      if (newOperand != operand) {
        update = true;
      }
      clonedOperands[i] = newOperand;
      return newOperand;
    }
  }
}
