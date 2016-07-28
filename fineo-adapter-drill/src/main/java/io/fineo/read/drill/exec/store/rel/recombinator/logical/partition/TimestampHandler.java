package io.fineo.read.drill.exec.store.rel.recombinator.logical.partition;

import io.fineo.lambda.dynamo.Range;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;

import java.time.Instant;

interface TimestampHandler {

  TimestampExpressionBuilder.ConditionBuilder getBuilder(TableScan scan);

  RelNode translateScanFromGeneratedRex(TableScan scan, RexNode timestamps);

  Range<Instant> getTableTimeRange(TableScan scan);
}
