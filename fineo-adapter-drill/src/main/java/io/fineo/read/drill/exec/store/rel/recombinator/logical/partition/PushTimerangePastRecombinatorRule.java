package io.fineo.read.drill.exec.store.rel.recombinator.logical.partition;

import io.fineo.drill.exec.store.dynamo.filter.SingleFunctionProcessor;
import io.fineo.read.drill.exec.store.rel.recombinator.FineoRecombinatorMarkerRel;
import io.fineo.read.drill.exec.store.schema.FineoTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Rule that pushes a timerange filter (WHERE) past the recombinator and into the actual scan
 */
public class PushTimerangePastRecombinatorRule extends RelOptRule {
  private static final Logger LOG =
    LoggerFactory.getLogger(PushTimerangePastRecombinatorRule.class);
  public static final PushTimerangePastRecombinatorRule
    INSTANCE = new PushTimerangePastRecombinatorRule();

  private PushTimerangePastRecombinatorRule() {
    super(operand(LogicalFilter.class, operand(FineoRecombinatorMarkerRel.class,
      operand(TableScan.class, RelOptRule.any()))),
      "FineoPushTimerangePastRecombinatorRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalFilter filter = call.rel(0);
    FineoRecombinatorMarkerRel fmr = call.rel(1);
    List<RelNode> scans = fmr.getInputs();
    // TODO support multiple scans
    TableScan scan = (TableScan) scans.get(0);
    String ts = FineoTable.BaseField.TIMESTAMP.getName();

    // figure out if the filter applies here
    RexBuilder rexer = filter.getCluster().getRexBuilder();
    Map<String, TimestampHandler> handlers = new HashMap<>();
    handlers.put("dfs", new FileSystemTimestampHandler(rexer, fmr, filter));
    handlers.put("dynamo", new DynamoTimestampHandler(rexer, fmr, filter));

    List<String> name = scan.getTable().getQualifiedName();
    TimestampHandler handler = handlers.get(name.get(0));
    TimestampExpressionBuilder builder =
      new TimestampExpressionBuilder(call, ts, handler.getBuilder(scan));
    RexNode timestamps = builder.lift(filter.getCondition(), fmr, rexer);
    // we have to scan everything
    if (builder.isScanAll() || timestamps == null) {
      return;
    }
    RelNode transform = handler.handleTimestampGeneratedRex(timestamps);
    if (transform != null) {
      call.transformTo(transform);
    }
  }

  static long asEpoch(SingleFunctionProcessor processor) {
    Object value = processor.getValue();
    if (value instanceof Long) {
      return (long) value;
    }
    return new Long(value.toString()) * 1000;
  }
}
