package io.fineo.read.drill.exec.store.rel.fixed.physical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.physical.PhysicalPlanCreator;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.SinglePrel;
import org.apache.drill.exec.record.BatchSchema;

import java.io.IOException;
import java.util.List;

import static io.fineo.read.drill.exec.store.rel.fixed.logical.FixedCommon.getProjectExpressions;

public class FixedSchemaPrel extends SinglePrel {

  private final List<RexNode> projects;

  public FixedSchemaPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child,
    RelDataType rowType, List<RexNode> projects) {
    super(cluster, traits, child);
    this.rowType = rowType;
    this.projects = projects;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    Project p = new Project(
      getProjectExpressions(new DrillParseContext(PrelUtil.getSettings(getCluster())), projects,
        this, getRowType()), childPOP);
    return creator.addMetadata(this, p);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FixedSchemaPrel(getCluster(), traitSet, inputs.get(0), getRowType(), projects);
  }

  @Override
  public BatchSchema.SelectionVectorMode[] getSupportedEncodings() {
    return BatchSchema.SelectionVectorMode.DEFAULT;
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return BatchSchema.SelectionVectorMode.NONE;
  }
}
