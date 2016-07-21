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
import org.apache.drill.exec.planner.sql.DrillSqlOperator;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;

public class FileSystemTimestampHandler implements TimestampHandler {

  private final RexBuilder rexer;

  public FileSystemTimestampHandler(RexBuilder rexer) {
    this.rexer = rexer;
  }

  @Override
  public TimestampExpressionBuilder.ConditionBuilder getBuilder(TableScan scan) {
    return new HierarchicalFsConditionBuilder(scan, rexer);
  }

  @Override
  public RelNode translateScanFromGeneratedRex(TableScan scan, RexNode timestamps) {
    // create a new logical expression on top of the scan
    return LogicalFilter.create(scan, timestamps);
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
      RexNode value = asValueNode(processor, builder);
      return builder.makeCall(op, ref, value);
    }
  }

  static RexNode asValueNode(SingleFunctionProcessor processor, RexBuilder builder) {
    RexNode literal = builder
      .makeLiteral(processor.getValue(), builder.getTypeFactory().createSqlType(BIGINT), true);
    DrillSqlOperator date = new DrillSqlOperator("TO_DATE", 1, true);
    return builder.makeCall(date, literal);
  }
}
