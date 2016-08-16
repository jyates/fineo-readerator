package io.fineo.read.drill.exec.store.rel.expansion;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

/**
 * Lift any RexNode with a RexInputRef matching the specified references out of the visited
 * expression.
 */
class LiftingRexVisitor extends RexVisitorImpl<RexNode> {

  private final RexBuilder builder;
  private final List<Integer> ref;

  protected LiftingRexVisitor(RexBuilder builder, List<Integer> ref) {
    super(true);
    this.builder = builder;
    this.ref = ref;
  }

  @Override
  public RexNode visitInputRef(RexInputRef inputRef) {
    if (ref.contains(inputRef.getIndex())) {
      return inputRef;
    }
    return null;
  }

  @Override
  public RexNode visitCall(RexCall call) {
    SqlOperator op = call.getOperator();
    switch (op.getName()) {
      case "AND":
        return RexUtil.composeConjunction(builder, visitJunction(call, false), true);
      case "OR":
        return RexUtil.composeDisjunction(builder, visitJunction(call, true), true);
    }

    // ensure that all the sub-parts return non-null
    for (RexNode operand : call.operands) {
      if (operand.accept(this) == null) {
        return null;
      }
    }
    return call;
  }

  private List<RexNode> visitJunction(RexCall call, boolean allNotNull) {
    List<RexNode> nodes = new ArrayList<>();
    for (RexNode operand : call.operands) {
      RexNode node = operand.accept(this);
      if (node != null) {
        nodes.add(node);
      } else if (allNotNull) {
        return emptyList();
      }
    }
    return nodes;
  }

  @Override
  public RexNode visitOver(RexOver over) {
    return super.visitOver(over);
  }

  @Override
  public RexNode visitLiteral(RexLiteral literal) {
    return literal;
  }

  @Override
  public RexNode visitRangeRef(RexRangeRef rangeRef) {
    return rangeRef;
  }

  @Override
  public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
    return fieldAccess;
  }

  @Override
  public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
    return dynamicParam;
  }

  @Override
  public RexNode visitLocalRef(RexLocalRef localRef) {
    return localRef;
  }

  @Override
  public RexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
    return correlVariable;
  }
}
