package io.fineo.read.drill.exec.store.rel.fixed.logical;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;

import java.util.List;

/**
 *
 */
public class FixedCommon {

  public static List<NamedExpression> getProjectExpressions(DrillParseContext context,
    List<RexNode> projects, RelNode project, RelDataType rowType) {
    List<NamedExpression> expressions = Lists.newArrayList();
    for (Pair<RexNode, RelDataTypeField> pair : Pair.zip(projects, rowType.getFieldList())) {
      // skip star column from the input
      if (StarColumnHelper.isStarColumn(pair.getValue().getName())) {
        continue;
      }
      LogicalExpression expr = DrillOptiq.toDrill(context, project, pair.left);
      expressions
        .add(new NamedExpression(expr, FieldReference.getWithQuotedRef(pair.right.getName())));
    }
    return expressions;
  }
}
