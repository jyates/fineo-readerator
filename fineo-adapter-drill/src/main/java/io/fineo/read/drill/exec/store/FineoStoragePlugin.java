package io.fineo.read.drill.exec.store;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.calcite.plan.RelOptRule.*;
import static org.apache.calcite.plan.RelOptRule.any;


/**
 * Simple wrapper around the schema creation
 */
public class FineoStoragePlugin extends AbstractStoragePlugin {

  private final FineoStoragePluginConfig config;
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
    // logical scans are converted into enumerable table scans
    rules.put(PlannerPhase.LOGICAL, new RelOptRule(operand(LogicalTableScan.class, any())) {
      @Override
      public void onMatch(RelOptRuleCall call) {
        LogicalTableScan scan = call.rel(0);
        call.transformTo(EnumerableTableScan.create(scan.getCluster(), scan.getTable()));
      }
    });
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
    return new FineoSchemaFactory(this, name);
  }

  public DrillbitContext getContext() {
    return context;
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
