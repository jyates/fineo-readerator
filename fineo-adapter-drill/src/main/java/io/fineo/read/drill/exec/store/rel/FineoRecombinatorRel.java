package io.fineo.read.drill.exec.store.rel;

import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.interpreter.Node;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * Recombination of sub-queries, mapped back to the original projected fields (as specified in
 * the row type {@link RelDataType}).
 */
public class FineoRecombinatorRel extends SingleRel implements BindableRel {
  private final RelDataType row;
  private final SchemaStore store;

  public FineoRecombinatorRel(RelNode subscans, RelDataType rowType, SchemaStore store) {
    super(subscans.getCluster(), subscans.getTraitSet().plus(BindableConvention.INSTANCE),
      subscans);
    this.row = rowType;
    this.store = store;
  }

  @Override
  protected RelDataType deriveRowType() {
    return this.row;
  }

  @Override
  public Node implement(InterpreterImplementor implementor) {
    return new FineoRecombinator(implementor.interpreter, this);
  }

  @Override
  public Enumerable<Object[]> bind(DataContext dataContext) {
    return new Interpreter(dataContext, this);
  }

  @Override
  public Class<Object[]> getElementType() {
    return Object[].class;
  }


  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FineoRecombinatorRel(inputs.get(0), getRowType(), store);
  }
}
