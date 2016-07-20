package io.fineo.read.drill.exec.store.rel.recombinator.logical.partition;

import io.fineo.drill.exec.store.dynamo.filter.SingleFunctionProcessor;
import io.fineo.read.drill.exec.store.rel.recombinator.FineoRecombinatorMarkerRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.util.ImmutableNullableList.of;

public class FileSystemTimestampHandler implements TimestampHandler {

  private final RexBuilder rexer;
  private final FineoRecombinatorMarkerRel fmr;
  private final Filter filter;

  public FileSystemTimestampHandler(RexBuilder rexer,
    FineoRecombinatorMarkerRel fmr, Filter filter) {
    this.rexer = rexer;
    this.fmr = fmr;
    this.filter = filter;
  }

  @Override
  public TimestampExpressionBuilder.ConditionBuilder getBuilder(TableScan scan) {
    return new HierarchicalFsConditionBuilder(scan, rexer);
  }

  @Override
  public RelNode handleTimestampGeneratedRex(RexNode timestamps) {
    // create a new logical expression, insert it after the recombinator
    LogicalFilter tsFilter = LogicalFilter.create(fmr.getInput(0), timestamps);
    RelNode fineo = fmr.copy(fmr.getTraitSet(), of(tsFilter));
    return filter.copy(filter.getTraitSet(), fineo, filter.getCondition());
  }

  private class HierarchicalFsConditionBuilder
    implements TimestampExpressionBuilder.ConditionBuilder {
    private final RelDataTypeField to;
    private final RexBuilder builder;
    private final TableScan scan;

    public HierarchicalFsConditionBuilder(TableScan scan, RexBuilder builder) {
      this.scan = scan;
      this.to = scan.getRowType().getField("dir0", false, false);
      this.builder = builder;
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
      RexInputRef ref = builder.makeInputRef(scan, to.getIndex());
      RexNode value = asLiteral(processor, builder);
      return builder.makeCall(op, ref, value);
    }
  }

  static RexNode asLiteral(SingleFunctionProcessor processor, RexBuilder builder) {
    long epoch = PushTimerangePastRecombinatorRule.asEpoch(processor);
    return builder.makeLiteral(epoch, builder.getTypeFactory().createSqlType(BIGINT),
      true);
  }
}
