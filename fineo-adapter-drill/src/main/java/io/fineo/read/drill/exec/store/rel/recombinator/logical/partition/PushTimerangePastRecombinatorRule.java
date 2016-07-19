package io.fineo.read.drill.exec.store.rel.recombinator.logical.partition;

import io.fineo.drill.exec.store.dynamo.filter.SingleFunctionProcessor;
import io.fineo.lambda.dynamo.DynamoTableNameParts;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.FineoRecombinatorRel;
import io.fineo.read.drill.exec.store.schema.FineoTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.drill.exec.planner.logical.DrillLimitRel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.function.BinaryOperator;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.util.ImmutableNullableList.of;

/**
 * Rule that pushes a timerange filter (WHERE) past the recombinator and into the actual scan
 */
public class PushTimerangePastRecombinatorRule extends RelOptRule {
  private static final Logger LOG =
    LoggerFactory.getLogger(PushTimerangePastRecombinatorRule.class);
  public static final PushTimerangePastRecombinatorRule
    INSTANCE = new PushTimerangePastRecombinatorRule();

  private PushTimerangePastRecombinatorRule() {
    super(operand(LogicalFilter.class, operand(FineoRecombinatorRel.class,
      operand(TableScan.class, RelOptRule.any()))),
      "FineoPushTimerangePastRecombinatorRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalFilter filter = call.rel(0);
    FineoRecombinatorRel fmr = call.rel(1);
    TableScan scan = call.rel(2);
    String ts = FineoTable.BaseField.TIMESTAMP.getName();
    RelDataTypeField timestampField = fmr.getRowType().getField(ts, true, false);

    // figure out if the filter applies here
    RexBuilder rexer = filter.getCluster().getRexBuilder();
    List<String> name = scan.getTable().getQualifiedName();
    TimestampExpressionBuilder.ConditionBuilder cond = null;
    switch (name.get(0)) {
      case "dfs":
        cond = new HierarchicalFsConditionBuilder(timestampField, scan, rexer);
        break;
      case "dynamo":
        cond = new DynamoTableNameConditionBuilder(scan, rexer);
    }
    TimestampExpressionBuilder builder = new TimestampExpressionBuilder(call, ts, cond);
    RexNode timestamps = builder.lift(filter.getCondition(), scan, rexer);
    // we have to scan everything
    if (builder.isScanAll() || timestamps == null) {
      return;
    }

    // what we do with the information depends on the condition again
    switch (name.get(0)) {
      case "dfs":
        // create a new logical expression, insert it after the recombinator
        LogicalFilter tsFilter = LogicalFilter.create(fmr.getInput(), timestamps);
        RelNode fineo = fmr.copy(fmr.getTraitSet(), of(tsFilter));
        filter = filter.copy(filter.getTraitSet(), fineo, filter.getCondition());
        call.transformTo(filter);
        break;
      case "dynamo":
        // decide if we even need this scan by evaluating the tree of true/false expressions
        if (!evaluate(timestamps)) {
          // table is 'removed' from query to limiting the output to 0
          RexNode zero = rexer.makeZeroLiteral(rexer.getTypeFactory().createSqlType(INTEGER));
          call.transformTo(new DrillLimitRel(filter.getCluster(), filter.getTraitSet(), fmr,
            zero, zero));
        }

    }
  }

  private boolean evaluate(RexNode timestamps) {
    return timestamps.accept(new RexVisitorImpl<Boolean>(true) {
      @Override
      public Boolean visitCall(RexCall call) {
        BinaryOperator<Boolean> op;
        if (call.getOperator().equals(SqlStdOperatorTable.AND)) {
          op = (a, b) -> a && b;
        } else if (call.getOperator().equals(SqlStdOperatorTable.OR)) {
          op = (a, b) -> a || b;
        } else {
          throw new IllegalArgumentException("Built a timestmap eval tree, but didn't use "
                                             + "AND/OR. Used: " + call);
        }
        Optional<Boolean> results = call.getOperands().stream().map(node -> node.accept(this))
                                        .reduce(op);
        return results.isPresent() ? results.get() : true;
      }

      @Override
      public Boolean visitLiteral(RexLiteral literal) {
        return (Boolean) literal.getValue();
      }
    });
  }


  private class HierarchicalFsConditionBuilder
    implements TimestampExpressionBuilder.ConditionBuilder {
    private final RelDataTypeField to;
    private final RelDataTypeField ts;
    private final RexBuilder builder;
    private final TableScan scan;

    public HierarchicalFsConditionBuilder(
      RelDataTypeField timestampField, TableScan scan, RexBuilder builder) {
      this.ts = timestampField;
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

  private class DynamoTableNameConditionBuilder
    implements TimestampExpressionBuilder.ConditionBuilder {
    private final DynamoTableNameParts parts;
    private final RexBuilder builder;

    public DynamoTableNameConditionBuilder(
      TableScan scan, RexBuilder builder) {
      List<String> name = scan.getTable().getQualifiedName();
      String actual = name.get(name.size() - 1);
      this.parts = DynamoTableNameParts.parse(actual);
      this.builder = builder;
    }


    @Override
    public RexNode buildGreaterThan(SingleFunctionProcessor processor) {
      long epoch = asEpoch(processor);
      return parts.getStart() >= epoch ?
             builder.makeLiteral(true) :
             builder.makeLiteral(false);
    }

    @Override
    public RexNode buildGreaterThanOrEquals(SingleFunctionProcessor processor) {
      long epoch = asEpoch(processor);
      return parts.getStart() >= epoch ?
             builder.makeLiteral(true) :
             builder.makeLiteral(false);
    }

    @Override
    public RexNode buildLessThan(SingleFunctionProcessor processor) {
      long epoch = asEpoch(processor);
      return parts.getEnd() < epoch ?
             builder.makeLiteral(true) :
             builder.makeLiteral(false);
    }

    @Override
    public RexNode buildLessThanOrEquals(SingleFunctionProcessor processor) {
      long epoch = asEpoch(processor);
      return parts.getEnd() < epoch ?
             builder.makeLiteral(true) :
             builder.makeLiteral(false);
    }

    @Override
    public RexNode buildEquals(SingleFunctionProcessor processor) {
      long epoch = asEpoch(processor);
      return parts.getStart() >= epoch && parts.getEnd() < epoch ?
             builder.makeLiteral(true) :
             builder.makeLiteral(false);
    }
  }

  private RexNode asLiteral(SingleFunctionProcessor processor, RexBuilder builder) {
    long epoch = asEpoch(processor);
    return builder.makeLiteral(epoch, builder.getTypeFactory().createSqlType(BIGINT),
      true);
  }

  private long asEpoch(SingleFunctionProcessor processor) {
    Object value = processor.getValue();
    if (value instanceof Long) {
      return (long) value;
    }
    return new Long(value.toString()) * 1000;
  }
}
