package io.fineo.read.drill.exec.store.rel.expansion;

import com.google.common.base.Predicates;
import io.fineo.read.drill.exec.store.rel.recombinator.FineoRecombinatorMarkerRel;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.SourceType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class GroupTablesAndOptionallyExpandRule extends RelOptRule {

  public static final RelOptRule INSTANCE = new GroupTablesAndOptionallyExpandRule();

  private GroupTablesAndOptionallyExpandRule() {
    super(operand(FineoRecombinatorMarkerRel.class,
      unordered(operand(TableScan.class, null, Predicates.alwaysTrue(), none()))),
      "Fineo::GroupTablesAndOptionallyExpand");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    FineoRecombinatorMarkerRel fmr = call.rel(0);
    RelOptCluster cluster = fmr.getCluster();
    List<RelNode> scans = call.getChildRels(fmr);
    List<RelNode> inputs = new ArrayList<>();
    for (RelNode scan : scans) {
      TableScan table = (TableScan) scan;
      SourceType type = getScanType(table);
      List<String> names = scan.getTable().getQualifiedName();
      String tableName = names.get(names.size() - 1);
      switch (type) {
        case DYNAMO:
          scan = wrapInDynamoExpander(scan);
      }

      inputs.add(new TableSetMarker(cluster, fmr.getTraitSet(), scan.getRowType(), scan, type,
        tableName));
    }
    call.transformTo(fmr.copy(fmr.getTraitSet(), inputs));
  }

  private RelNode wrapInDynamoExpander(RelNode scan) {
    return new DynamoRowFieldExpanderRel(scan);
  }

  private static SourceType getScanType(TableScan scan) {
    List<String> name = scan.getTable().getQualifiedName();
    return SourceType.valueOf(name.get(0).toUpperCase());
  }
}
