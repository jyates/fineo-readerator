package io.fineo.read.drill.exec.store.rel.recombinator.logical.partition.handler;

import com.google.common.collect.Range;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.partition.TableFilterBuilder;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.partition.TimestampExpressionBuilder;

import java.time.Instant;

public interface TimestampHandler {

  TableFilterBuilder getFilterBuilder();

  TimestampExpressionBuilder.ConditionBuilder getShouldScanBuilder(String tableName);

  Range<Instant> getTableTimeRange(String tableName);
}
