package io.fineo.read.drill.exec.store.rel.recombinator.logical.partition;

import io.fineo.drill.exec.store.dynamo.filter.SingleFunctionProcessor;
import io.fineo.lambda.dynamo.Range;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.drill.exec.planner.sql.DrillSqlOperator;

import java.time.Instant;

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

  @Override
  public Range<Instant> getTableTimeRange(TableScan scan) {
    return new Range<>(Instant.EPOCH, Instant.now());
  }

  private class HierarchicalFsConditionBuilder
    implements TimestampExpressionBuilder.ConditionBuilder {
    private final RelDataTypeField to;
    private final RexBuilder builder;
    private final TableScan scan;

    public HierarchicalFsConditionBuilder(TableScan scan, RexBuilder builder) {
      this.scan = scan;
      this.to = getTimeDir(scan);
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
      RexNode value = asValueNode(processor, builder);
      return fileScanOpToRef(builder, scan, to, op, value);
    }
  }

  static RexNode fileScanOpToRef(RexBuilder builder, RelNode scan, RelDataTypeField dirField,
    SqlOperator op, RexNode value){
    RexInputRef ref = builder.makeInputRef(scan, dirField.getIndex());
    return builder.makeCall(op, ref, value);
  }

  static RelDataTypeField getTimeDir(RelNode scan){
    return scan.getRowType().getField("dir0", false, false);
  }

  private static RexNode asValueNode(SingleFunctionProcessor processor, RexBuilder builder) {
    return asValueNode(processor.getValue(), builder);
  }

  static RexNode asValueNode(Object value, RexBuilder builder) {
    RexNode literal =
      builder.makeLiteral(value, builder.getTypeFactory().createSqlType(BIGINT), true);
    DrillSqlOperator date = new DrillSqlOperator("TO_DATE", 1, true);
    return builder.makeCall(date, literal);
  }
}
