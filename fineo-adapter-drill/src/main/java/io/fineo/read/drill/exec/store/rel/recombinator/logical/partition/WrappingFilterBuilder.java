package io.fineo.read.drill.exec.store.rel.recombinator.logical.partition;

import io.fineo.drill.exec.store.dynamo.filter.SingleFunctionProcessor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.drill.common.expression.LogicalExpression;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL;

/**
 * Create the wrapping filter, assuming there are fields to be filtered
 */
public class WrappingFilterBuilder implements TimestampExpressionBuilder.ConditionBuilder {

  private final TableScan scan;
  private final TableFilterBuilder builder;
  private final RelDataTypeField field;
  private final RexBuilder rexer;

  public WrappingFilterBuilder(TableScan scan, TableFilterBuilder builder, RexBuilder rexer) {
    this.scan = scan;
    this.builder = builder;
    this.field = scan.getRowType().getField(builder.getFilterFieldName(), true, true);
    this.rexer = rexer;
  }

  public RelNode buildFilter(LogicalExpression condition, String timestampField) {
    TimestampExpressionBuilder builder = new TimestampExpressionBuilder(timestampField, this);
    RexNode rex = builder.lift(condition, rexer);
    return LogicalFilter.create(scan, rex);
  }

  @Override
  public RexNode buildGreaterThan(SingleFunctionProcessor processor) {
    return makeCall(processor, GREATER_THAN);
  }

  @Override
  public RexNode buildGreaterThanOrEquals(SingleFunctionProcessor processor) {
    return makeCall(processor, GREATER_THAN_OR_EQUAL);
  }

  @Override
  public RexNode buildLessThan(SingleFunctionProcessor processor) {
    return makeCall(processor, LESS_THAN);
  }

  @Override
  public RexNode buildLessThanOrEquals(SingleFunctionProcessor processor) {
    return makeCall(processor, LESS_THAN_OR_EQUAL);
  }

  @Override
  public RexNode buildEquals(SingleFunctionProcessor processor) {
    return makeCall(processor, EQUALS);
  }

  private RexNode makeCall(SingleFunctionProcessor processor, SqlOperator op) {
    RexNode value = this.builder.replaceTimestamp(processor);
    RexInputRef ref = this.rexer.makeInputRef(scan, field.getIndex());
    return rexer.makeCall(op, ref, value);
  }
}
