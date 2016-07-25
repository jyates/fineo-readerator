package io.fineo.read.drill.exec.store.rel.recombinator.logical.partition;

import com.google.common.base.Predicates;
import io.fineo.drill.exec.store.dynamo.filter.SingleFunctionProcessor;
import io.fineo.read.drill.exec.store.rel.recombinator.FineoRecombinatorMarkerRel;
import io.fineo.read.drill.exec.store.schema.FineoTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
      unordered(operand(TableScan.class, null, Predicates.alwaysTrue(), none())))),
      "FineoPushTimerangePastRecombinatorRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    FineoRecombinatorMarkerRel fmr = call.rel(1);
    List<RelNode> scans = call.getChildRels(fmr);
    for (RelNode scan : scans) {
      if (!(scan instanceof TableScan)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalFilter filter = call.rel(0);
    FineoRecombinatorMarkerRel fmr = call.rel(1);
    List<RelNode> scans = call.getChildRels(fmr);
    List<RelNode> translatedScans = new ArrayList<>();
    String ts = FineoTable.BaseField.TIMESTAMP.getName();
    RexBuilder rexer = filter.getCluster().getRexBuilder();
    Map<String, TimestampHandler> handlers = new HashMap<>();
    handlers.put("dfs", new FileSystemTimestampHandler(rexer));
    handlers.put("dynamo", new DynamoTimestampHandler(rexer));
    for (RelNode s : scans) {
      TableScan scan = (TableScan) s;
      List<String> name = scan.getTable().getQualifiedName();
      TimestampHandler handler = handlers.get(name.get(0));
      TimestampExpressionBuilder builder =
        new TimestampExpressionBuilder(call, ts, handler.getBuilder(scan));
      RexNode timestamps = builder.lift(filter.getCondition(), fmr, rexer);
      // we have to scan everything
      RelNode translated = scan;
      if (!builder.isScanAll() && timestamps != null) {
        translated = handler.translateScanFromGeneratedRex(scan, timestamps);
      }
      if (translated != null) {
        translatedScans.add(translated);
      }
    }

    // all the scans stay the same
    if (translatedScans.equals(scans)) {
      return;
    }

    // we need to update the scans!
    RelNode fineo = fmr.copy(fmr.getTraitSet(), translatedScans);
    RelNode transform = filter.copy(filter.getTraitSet(), fineo, filter.getCondition());
    call.transformTo(transform);
  }

  static long asEpoch(SingleFunctionProcessor processor) {
    Object value = processor.getValue();
    if (value instanceof Long) {
      return (long) value;
    }
    return new Long(value.toString()) * 1000;
  }
}
