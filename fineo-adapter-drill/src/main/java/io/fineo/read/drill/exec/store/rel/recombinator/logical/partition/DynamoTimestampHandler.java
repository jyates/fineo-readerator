package io.fineo.read.drill.exec.store.rel.recombinator.logical.partition;

import io.fineo.drill.exec.store.dynamo.filter.SingleFunctionProcessor;
import io.fineo.lambda.dynamo.DynamoTableNameParts;
import io.fineo.lambda.dynamo.Range;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.BinaryOperator;

import static java.time.Instant.ofEpochMilli;

public class DynamoTimestampHandler implements TimestampHandler {

  private final RexBuilder rexer;

  public DynamoTimestampHandler(RexBuilder rexer) {
    this.rexer = rexer;
  }

  @Override
  public TimestampExpressionBuilder.ConditionBuilder getBuilder(TableScan scan) {
    return new DynamoTableNameConditionBuilder(scan, rexer);
  }

  @Override
  public RelNode translateScanFromGeneratedRex(TableScan scan, RexNode timestamps) {
    // decide if we even need this scan by evaluating the tree of true/false expressions
    if (!evaluate(timestamps)) {
      return null;
    }
    return scan;
  }

  @Override
  public Range<Instant> getTableTimeRange(TableScan scan) {
    DynamoTableNameParts parts = parseName(scan);
    return new Range<>(ofEpochMilli(parts.getStart()), ofEpochMilli(parts.getEnd()));
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

  private DynamoTableNameParts parseName(TableScan scan) {
    List<String> name = scan.getTable().getQualifiedName();
    String actual = name.get(name.size() - 1);
    return DynamoTableNameParts.parse(actual);
  }

  private class DynamoTableNameConditionBuilder
    implements TimestampExpressionBuilder.ConditionBuilder {
    private final DynamoTableNameParts parts;
    private final RexBuilder builder;

    public DynamoTableNameConditionBuilder(
      TableScan scan, RexBuilder builder) {
      this.parts = parseName(scan);
      this.builder = builder;
    }


    @Override
    public RexNode buildGreaterThan(SingleFunctionProcessor processor) {
      long epoch = asEpoch(processor);
      return parts.getEnd() > epoch ?
             builder.makeLiteral(true) :
             builder.makeLiteral(false);
    }

    @Override
    public RexNode buildGreaterThanOrEquals(SingleFunctionProcessor processor) {
      // end range of table is non-inclusive, so we do the same logic as greaterThan
      return buildGreaterThan(processor);
    }

    @Override
    public RexNode buildLessThan(SingleFunctionProcessor processor) {
      long epoch = asEpoch(processor);
      return parts.getStart() < epoch ?
             builder.makeLiteral(true) :
             builder.makeLiteral(false);
    }

    @Override
    public RexNode buildLessThanOrEquals(SingleFunctionProcessor processor) {
      long epoch = asEpoch(processor);
      return parts.getStart() <= epoch ?
             builder.makeLiteral(true) :
             builder.makeLiteral(false);
    }

    @Override
    public RexNode buildEquals(SingleFunctionProcessor processor) {
      long epoch = asEpoch(processor);
      // interval CONTAINS
      return parts.getStart() >= epoch && parts.getEnd() < epoch ?
             builder.makeLiteral(true) :
             builder.makeLiteral(false);
    }
  }

  private static long asEpoch(SingleFunctionProcessor processor) {
    Object value = processor.getValue();
    if (value instanceof Long) {
      return (long) value;
    }
    return new Long(value.toString()) * 1000;
  }
}
