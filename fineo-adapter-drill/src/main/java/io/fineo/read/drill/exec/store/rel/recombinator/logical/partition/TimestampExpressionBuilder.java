package io.fineo.read.drill.exec.store.rel.recombinator.logical.partition;

import com.google.common.base.Preconditions;
import io.fineo.drill.exec.store.dynamo.filter.SingleFunctionProcessor;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;

import java.util.ArrayList;
import java.util.List;

public class TimestampExpressionBuilder {

  private static final String AND = "booleanAnd";
  private static final String OR = "booleanOr";

  private final String ts;
  private boolean scanAll = false;

  public TimestampExpressionBuilder(String timestampFieldName) {
    this.ts = timestampFieldName;
  }

  public boolean isScanAll() {
    return scanAll;
  }

  public void reset() {
    this.scanAll = false;
  }

  public RexNode lift(LogicalExpression conditionExp, RexBuilder builder, ConditionBuilder cond) {
    return conditionExp.accept(
      new AbstractExprVisitor<RexNode, Object, RuntimeException>() {
        @Override
        public RexNode visitBooleanOperator(BooleanOperator op, Object value)
          throws RuntimeException {
          List<RexNode> ands = new ArrayList<>();
          for (LogicalExpression arg : op.args) {
            RexNode ret = arg.accept(this, null);
            if (ret != null) {
              ands.add(ret);
            }
          }
          RexNode ret = null;
          switch (ands.size()) {
            case 0:
              break;
            case 1:
              ret = ands.get(0);
              break;
            default:
              switch (op.getName()) {
                case AND:
                  ret = RexUtil.composeConjunction(builder, ands, true);
                  break;
                case OR:
                  ret = RexUtil.composeDisjunction(builder, ands, true);
                  break;
              }
          }
          return ret;
        }

        @Override
        public RexNode visitFunctionCall(FunctionCall call, Object value)
          throws RuntimeException {
          String functionName = call.getName();
          // its not a compare function, so we have to scan everything
          if (!SingleFunctionProcessor.isCompareFunction(functionName)) {
            scanAll = true;
            return null;
          }
          SingleFunctionProcessor processor = SingleFunctionProcessor.process(call);
          assert processor.isSuccess() : "Processor could not evaluate: " + call;
          String key = processor.getPath().getAsUnescapedPath();
          if (!key.equals(ts)) {
            return null;
          }

          ensureNumber(processor);

          // convert into a rexcall
          switch (processor.getFunctionName()) {
            case "not_equal":
              scanAll = true;
              return null;

            case "equal":
              return cond.buildEquals(processor);
            case "greater_than":
              return cond.buildGreaterThan(processor);
            case "greater_than_or_equal_to":
              return cond.buildGreaterThanOrEquals(processor);
            case "less_than":
              return cond.buildLessThan(processor);
            case "less_than_or_equal_to":
              return cond.buildLessThanOrEquals(processor);

            case "add":
            case "subtract":
            case "divide":
            case "multiply":
            case "modulo":
              throw new IllegalArgumentException("Timestamp only suppports simple comparisions "
                                                 + "like: =, <, <=, >, >=");

              // We cannot handle any of these, but at least have good error messages
            case "booleanOr":
            case "booleanAnd":
              throw new IllegalStateException("Boolean operators should have been handled already");

            case "isnull":
            case "isnotnull":
            case "concatOperator":
            case "xor":
            case "istrue":
            case "isnottrue":
            case "isfalse":
            case "isnotfalse":
            case "similar_to":
            case "not":
            case "negative":
              throw new IllegalArgumentException("Timestamp does not support logical comparison");
            default:
              throw new IllegalArgumentException("Timestamp comparison does not support: " +
                                                 processor.getFunctionName());
          }
        }
      }, null);
  }

  private void ensureNumber(SingleFunctionProcessor processor) {
    Preconditions.checkState(processor.getValue() instanceof Number, "Timestamp must be"
                                                                     + " a number");
  }

  public interface ConditionBuilder {
    RexNode buildGreaterThan(SingleFunctionProcessor processor);

    RexNode buildGreaterThanOrEquals(SingleFunctionProcessor processor);

    RexNode buildLessThan(SingleFunctionProcessor processor);

    RexNode buildLessThanOrEquals(SingleFunctionProcessor processor);

    RexNode buildEquals(SingleFunctionProcessor processor);
  }
}
