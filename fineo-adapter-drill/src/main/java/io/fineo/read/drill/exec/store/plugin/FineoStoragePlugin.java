package io.fineo.read.drill.exec.store.plugin;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.fineo.read.drill.exec.store.rel.fixed.physical.FixedSchemaPrule;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.FineoRecombinatorRule;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.partition
  .PushTimerangePastRecombinatorRule;
import io.fineo.read.drill.exec.store.rel.recombinator.physical.FineoRecombinatorPrule;
import io.fineo.read.drill.exec.store.schema.FineoSchemaFactory;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.calcite.plan.RelOptRule.any;
import static org.apache.calcite.plan.RelOptRule.operand;


/**
 * Simple wrapper around the schema creation
 */
public class FineoStoragePlugin extends AbstractStoragePlugin {

  public static String VERSION = "0";

  protected final FineoStoragePluginConfig config;
  private final FineoSchemaFactory factory;
  private final DrillbitContext context;
  private final Multimap<PlannerPhase, RelOptRule> rules;

  public FineoStoragePlugin(FineoStoragePluginConfig configuration, DrillbitContext c,
    String name) {
    this.config = configuration;
    this.factory = getFactory(name);
    this.context = c;

    this.rules = getRules();
  }

  private Multimap<PlannerPhase, RelOptRule> getRules() {
    Multimap<PlannerPhase, RelOptRule> rules = ArrayListMultimap.create();
    // Convert logical scans into enumerable table scans. This is usually done in the
    // RelStructuredTypeFlattener#rewriteRel for drill, but that only works for cases where
    // there is a standard DrillTable. Since we aren't a real table we have to do the conversion
    // here, as early as possible in the loop
    rules.put(PlannerPhase.LOGICAL, new RelOptRule(operand(LogicalTableScan.class, any()),
      "LogicalTableScanToEnumerable_Replace_RelStructuredTypeFlattener") {
      @Override
      public void onMatch(RelOptRuleCall call) {
        LogicalTableScan scan = call.rel(0);
        EnumerableTableScan ets = EnumerableTableScan.create(scan.getCluster(), scan.getTable());
        call.transformTo(ets);
      }
    });

    // Filter out tables/directories that are not included in requested time range
//    rules.put(PlannerPhase.LOGICAL, PushTimerangePastRecombinatorRule.INSTANCE);

    // transform FRMR -> FRR
    rules.put(PlannerPhase.LOGICAL, FineoRecombinatorRule.INSTANCE);
    // transform FRR -> FRPr
    rules.put(PlannerPhase.PHYSICAL, FineoRecombinatorPrule.INSTANCE);
    // FixedR -> FixedPr
    rules.put(PlannerPhase.PHYSICAL, FixedSchemaPrule.INSTANCE);
    return rules;
  }

  @Override
  public StoragePluginConfig getConfig() {
    return config;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    factory.registerSchemas(schemaConfig, parent);
  }

  protected FineoSchemaFactory getFactory(String name) {
    return new FineoSchemaFactory(this, name, this.config.getOrgs());
  }

  // definitely don't support a physical scan
  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection,
    List<SchemaPath> columns) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<? extends RelOptRule> getOptimizerRules(OptimizerRulesContext optimizerContext,
    PlannerPhase phase) {
    return new HashSet<>(rules.get(phase));
  }
}
