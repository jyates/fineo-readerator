package io.fineo.read.drill.exec.store.rel.recombinator.logical.partition;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;

interface TimestampHandler {

  TimestampExpressionBuilder.ConditionBuilder getBuilder(TableScan scan);

  RelNode handleTimestampGeneratedRex(RexNode node);
}
