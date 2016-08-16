package io.fineo.read.drill.exec.store.rel.expansion;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Lift the conditions on fields out of a RexNode
 */
public class FilterFieldLifter {
  private final RexNode node;
  private final RelDataType type;
  private final List<String> fields;
  private final RexBuilder builder;
  private final RexNode cnf;

  public FilterFieldLifter(RexNode node, RelDataType rowType, RexBuilder builder,
    String... fields) {
    this.node = node;
    this.type = rowType;
    this.fields = newArrayList(fields);
    this.builder = builder;
    this.cnf = RexUtil.toCnf(builder, node);
  }

  public RexNode lift() {
    List<RexNode> lifted = new ArrayList<>();
    List<Integer> refs = fields.stream()
                               .map(field -> type.getField(field, true, true).getIndex())
                               .collect(Collectors.toList());
    for (RexNode and : RelOptUtil.conjunctions(cnf)) {
      RexNode lift = and.accept(new LiftingRexVisitor(builder, refs));
      if (lift != null) {
        lifted.add(lift);
      }
    }

    return RexUtil.composeConjunction(builder, lifted, true);
  }

  public RexNode getFilterCnf() {
    return cnf;
  }
}
