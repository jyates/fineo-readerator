package io.fineo.read.drill.exec.store.rel.recombinator.logical.partition;

import io.fineo.drill.exec.store.dynamo.filter.SingleFunctionProcessor;
import io.fineo.lambda.dynamo.DynamoTableNameParts;
import io.fineo.read.drill.exec.store.rel.recombinator.FineoRecombinatorMarkerRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.drill.exec.planner.logical.DrillLimitRel;

import java.util.List;
import java.util.Optional;
import java.util.function.BinaryOperator;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

public class DynamoTimestampHandler implements TimestampHandler {

  private final RexBuilder rexer;
  private final FineoRecombinatorMarkerRel fmr;
  private final Filter filter;

  public DynamoTimestampHandler(RexBuilder rexer, FineoRecombinatorMarkerRel fmr,
    Filter filter) {
    this.rexer = rexer;
    this.fmr = fmr;
    this.filter = filter;
  }

  @Override
  public TimestampExpressionBuilder.ConditionBuilder getBuilder(TableScan scan) {
    return new DynamoTableNameConditionBuilder(scan, rexer);
  }

  @Override
  public RelNode handleTimestampGeneratedRex(RexNode timestamps) {
    // decide if we even need this scan by evaluating the tree of true/false expressions
    if (!evaluate(timestamps)) {
      // table is 'removed' from query to limiting the output to 0
      RexNode zero = rexer.makeZeroLiteral(rexer.getTypeFactory().createSqlType(INTEGER));
      return new DrillLimitRel(filter.getCluster(), filter.getTraitSet(), fmr,
        zero, zero);
    }
    return null;
  }

  private boolean evaluate(RexNode timestamps) {
    return timestamps.accept(new RexVisitorImpl<Boolean>(true) {
      @Override
      public Boolean visitCall(RexCall call) {
        BinaryOperator<Boolean> op;
        if (call.getOperator().equals(SqlStdOperatorTable.AND)) {
          op = (a, b) -> a && b;
        } else if (call.getOperator().equals(SqlStdOperatorTable.OR)) {
          op = (a, b) -> a || b;
        } else {
          throw new IllegalArgumentException("Built a timestmap eval tree, but didn't use "
                                             + "AND/OR. Used: " + call);
        }
        Optional<Boolean> results = call.getOperands().stream().map(node -> node.accept(this))
                                        .reduce(op);
        return results.isPresent() ? results.get() : true;
      }

      @Override
      public Boolean visitLiteral(RexLiteral literal) {
        return (Boolean) literal.getValue();
      }
    });
  }

  private class DynamoTableNameConditionBuilder
    implements TimestampExpressionBuilder.ConditionBuilder {
    private final DynamoTableNameParts parts;
    private final RexBuilder builder;

    public DynamoTableNameConditionBuilder(
      TableScan scan, RexBuilder builder) {
      List<String> name = scan.getTable().getQualifiedName();
      String actual = name.get(name.size() - 1);
      this.parts = DynamoTableNameParts.parse(actual);
      this.builder = builder;
    }


    @Override
    public RexNode buildGreaterThan(SingleFunctionProcessor processor) {
      long epoch = PushTimerangePastRecombinatorRule.asEpoch(processor);
      return parts.getStart() >= epoch ?
             builder.makeLiteral(true) :
             builder.makeLiteral(false);
    }

    @Override
    public RexNode buildGreaterThanOrEquals(SingleFunctionProcessor processor) {
      long epoch = PushTimerangePastRecombinatorRule.asEpoch(processor);
      return parts.getStart() >= epoch ?
             builder.makeLiteral(true) :
             builder.makeLiteral(false);
    }

    @Override
    public RexNode buildLessThan(SingleFunctionProcessor processor) {
      long epoch = PushTimerangePastRecombinatorRule.asEpoch(processor);
      return parts.getEnd() < epoch ?
             builder.makeLiteral(true) :
             builder.makeLiteral(false);
    }

    @Override
    public RexNode buildLessThanOrEquals(SingleFunctionProcessor processor) {
      long epoch = PushTimerangePastRecombinatorRule.asEpoch(processor);
      return parts.getEnd() < epoch ?
             builder.makeLiteral(true) :
             builder.makeLiteral(false);
    }

    @Override
    public RexNode buildEquals(SingleFunctionProcessor processor) {
      long epoch = PushTimerangePastRecombinatorRule.asEpoch(processor);
      return parts.getStart() >= epoch && parts.getEnd() < epoch ?
             builder.makeLiteral(true) :
             builder.makeLiteral(false);
    }
  }

}
