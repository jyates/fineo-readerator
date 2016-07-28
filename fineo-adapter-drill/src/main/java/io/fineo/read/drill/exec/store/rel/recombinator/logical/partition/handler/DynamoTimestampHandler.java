package io.fineo.read.drill.exec.store.rel.recombinator.logical.partition.handler;

import io.fineo.drill.exec.store.dynamo.filter.SingleFunctionProcessor;
import io.fineo.lambda.dynamo.DynamoTableNameParts;
import io.fineo.lambda.dynamo.Range;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.partition.TableFilterBuilder;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.partition.TimestampExpressionBuilder;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import java.time.Instant;
import java.util.List;

import static java.time.Instant.ofEpochMilli;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;

public class DynamoTimestampHandler implements TimestampHandler {

  private final RexBuilder rexer;

  public DynamoTimestampHandler(RexBuilder rexer) {
    this.rexer = rexer;
  }

  @Override
  public TimestampExpressionBuilder.ConditionBuilder getShouldScanBuilder(TableScan scan) {
    return new DynamoTableNameIncludedConditionBuilder(scan, rexer);
  }

  @Override
  public TableFilterBuilder getFilterBuilder(TableScan scan) {
    return new TableFilterBuilder() {
      @Override
      public RexNode replaceTimestamp(SingleFunctionProcessor processor) {
        long epoch = asEpoch(processor);
        return rexer.makeLiteral(epoch, rexer.getTypeFactory().createSqlType(BIGINT), true);
      }

      @Override
      public String getFilterFieldName() {
        return Schema.SORT_KEY_NAME;
      }
    };
  }

  @Override
  public Range<Instant> getTableTimeRange(TableScan scan) {
    DynamoTableNameParts parts = parseName(scan);
    return new Range<>(ofEpochMilli(parts.getStart()), ofEpochMilli(parts.getEnd()));
  }

  private DynamoTableNameParts parseName(TableScan scan) {
    List<String> name = scan.getTable().getQualifiedName();
    String actual = name.get(name.size() - 1);
    return DynamoTableNameParts.parse(actual);
  }

  private class DynamoTableNameIncludedConditionBuilder
    implements TimestampExpressionBuilder.ConditionBuilder {
    private final DynamoTableNameParts parts;
    private final RexBuilder builder;

    public DynamoTableNameIncludedConditionBuilder(
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
