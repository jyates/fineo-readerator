package io.fineo.read.drill.exec.store.rel.recombinator.logical.partition;

import io.fineo.drill.exec.store.dynamo.filter.SingleFunctionProcessor;
import io.fineo.lambda.dynamo.Range;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.drill.exec.planner.sql.DrillSqlOperator;

import java.time.Instant;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;

public class FileSystemTimestampHandler implements TimestampHandler {

  private final RexBuilder rexer;

  public FileSystemTimestampHandler(RexBuilder rexer) {
    this.rexer = rexer;
  }

  @Override
  public TableFilterBuilder getFilterBuilder(TableScan scan) {
    return new TableFilterBuilder() {
      @Override
      public RexNode replaceTimestamp(SingleFunctionProcessor processor) {
        return asValueNode(processor, rexer);
      }

      @Override
      public String getFilterFieldName() {
        return "dir0";
      }
    };
  }

  @Override
  public TimestampExpressionBuilder.ConditionBuilder getShouldScanBuilder(TableScan scan) {
    // we need to include this scan no matter what - it has "all" the data. Maybe we will get to
    // partition some of the data out later from the generated filter, but for now, we just do
    // the scan, if there is a timestamp
    return new TimestampExpressionBuilder.ConditionBuilder() {
      @Override
      public RexNode buildGreaterThan(SingleFunctionProcessor processor) {
        return rexer.makeLiteral(true);
      }

      @Override
      public RexNode buildGreaterThanOrEquals(SingleFunctionProcessor processor) {
        return rexer.makeLiteral(true);
      }

      @Override
      public RexNode buildLessThan(SingleFunctionProcessor processor) {
        return rexer.makeLiteral(true);
      }

      @Override
      public RexNode buildLessThanOrEquals(SingleFunctionProcessor processor) {
        return rexer.makeLiteral(true);
      }

      @Override
      public RexNode buildEquals(SingleFunctionProcessor processor) {
        return rexer.makeLiteral(true);
      }
    };
  }

  @Override
  public Range<Instant> getTableTimeRange(TableScan scan) {
    return new Range<>(Instant.EPOCH, Instant.now());
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
