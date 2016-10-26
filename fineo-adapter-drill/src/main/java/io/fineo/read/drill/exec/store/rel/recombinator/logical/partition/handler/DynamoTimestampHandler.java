package io.fineo.read.drill.exec.store.rel.recombinator.logical.partition.handler;

import com.google.common.collect.Range;
import io.fineo.drill.exec.store.dynamo.filter.SingleFunctionProcessor;
import io.fineo.lambda.dynamo.DynamoTableNameParts;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.partition.TableFilterBuilder;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.partition.TimestampExpressionBuilder;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static java.time.Instant.ofEpochMilli;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;

public class DynamoTimestampHandler implements TimestampHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DynamoTimestampHandler.class);

  private final RexBuilder rexer;

  public DynamoTimestampHandler(RexBuilder rexer) {
    this.rexer = rexer;
  }

  @Override
  public TimestampExpressionBuilder.ConditionBuilder getShouldScanBuilder(String tableName) {
    return new DynamoTableNameIncludedConditionBuilder(tableName, rexer);
  }

  @Override
  public TableFilterBuilder getFilterBuilder() {
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
  public Range<Instant> getTableTimeRange(String tableName) {
    DynamoTableNameParts parts = DynamoTableNameParts.parse(tableName, false);
    return Range.closedOpen(ofEpochMilli(parts.getStart()), ofEpochMilli(parts.getEnd()));
  }

  private class DynamoTableNameIncludedConditionBuilder
    implements TimestampExpressionBuilder.ConditionBuilder {
    private final DynamoTableNameParts parts;
    private final RexBuilder builder;

    public DynamoTableNameIncludedConditionBuilder(String tableName, RexBuilder builder) {
      this.parts = DynamoTableNameParts.parse(tableName, true);
      this.builder = builder;
    }

    @Override
    public RexNode buildGreaterThan(SingleFunctionProcessor processor) {
      long epoch = asEpoch(processor);
      LOG.trace("Checking if epoch ts [{}] greater than end time [{}] to include table: {}",
        epoch, parts.getEnd(), parts.getName());
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
      LOG.trace("Checking if epoch ts [{}] < start time [{}] to include table: {}",
        epoch, parts.getStart(), parts.getName());
      return parts.getStart() < epoch ?
             builder.makeLiteral(true) :
             builder.makeLiteral(false);
    }

    @Override
    public RexNode buildLessThanOrEquals(SingleFunctionProcessor processor) {
      long epoch = asEpoch(processor);
      LOG.trace("Checking if epoch ts [{}] <= start time [{}] to include table: {}",
        epoch, parts.getStart(), parts.getName());
      return parts.getStart() <= epoch ?
             builder.makeLiteral(true) :
             builder.makeLiteral(false);
    }

    @Override
    public RexNode buildEquals(SingleFunctionProcessor processor) {
      long epoch = asEpoch(processor);
      // interval CONTAINS
      LOG.trace("Checking if epoch ts [{}] with within table time range[{},{}] to incl. table:{}",
        epoch, parts.getStart(), parts.getEnd(), parts.getName());
      return parts.getStart() <= epoch && epoch < parts.getEnd() ?
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
