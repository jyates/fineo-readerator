package io.fineo.read.drill.exec.store.plugin;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.fineo.drill.exec.store.dynamo.config.DynamoStoragePluginConfig;
import io.fineo.read.drill.exec.store.FineoCommon;
import io.fineo.read.drill.exec.store.rel.expansion.logical.DynamoRowFieldExpanderConverter;
import io.fineo.read.drill.exec.store.rel.expansion.optimize.PushFilterPastDynamoRowExpander;
import io.fineo.read.drill.exec.store.rel.expansion.phyiscal.DynamoRowFieldExpanderPrule;
import io.fineo.read.drill.exec.store.rel.fixed.physical.FixedSchemaPrule;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.FineoRecombinatorRule;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.partition
  .ConvertFineoMarkerIntoFilteredInputTables.FilterRecombinatorTablesWithNoTimestampFilter;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.partition
  .ConvertFineoMarkerIntoFilteredInputTables.PushTimerangeFilterPastRecombinator;
import io.fineo.read.drill.exec.store.rel.recombinator.physical.FineoRecombinatorPrule;
import io.fineo.read.drill.exec.store.schema.FineoSchemaFactory;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

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
  private final AbstractStoragePlugin writeErrors;
  private final DynamoStoragePluginConfig dynamo;
  private final boolean enableRadio;
  private AmazonDynamoDBAsyncClient client;
  private DynamoDB dynamoClient;


  public FineoStoragePlugin(FineoStoragePluginConfig configuration, DrillbitContext c,
    String name) throws ExecutionSetupException {
    this.config = configuration;
    this.dynamo = (DynamoStoragePluginConfig) c.getStorage().getPlugin("dynamo").getConfig();
    this.factory = getFactory(name);
    this.context = c;
    this.enableRadio = FineoCommon.isRadioEnabled();
    this.rules = getRules();
    FileSystemPlugin fs = (FileSystemPlugin) c.getStorage().getPlugin("errors");
    this.writeErrors = fs == null ? new EmptyPlugin() : fs;
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

    // Filter out tables/directories that are not included in requested time range AND add
    // filters for the input sources to ensure that we don't read overlapping data
    rules.put(PlannerPhase.DIRECTORY_PRUNING,
      FilterRecombinatorTablesWithNoTimestampFilter.INSTANCE);
    rules.put(PlannerPhase.DIRECTORY_PRUNING, PushTimerangeFilterPastRecombinator.INSTANCE);

    // transform FRMR -> FRR
    rules.put(PlannerPhase.LOGICAL, FineoRecombinatorRule.INSTANCE);

    // dynamo conversion
    rules.put(PlannerPhase.LOGICAL, DynamoRowFieldExpanderConverter.INSTANCE);

    // FixedR -> FixedPr
    rules.put(PlannerPhase.PHYSICAL, FixedSchemaPrule.INSTANCE);
    // FRR -> FRPr
    rules.put(PlannerPhase.PHYSICAL, FineoRecombinatorPrule.INSTANCE);
    // DynamoExpansionR -> Pr
    rules.put(PlannerPhase.PHYSICAL, DynamoRowFieldExpanderPrule.INSTANCE);
    // Ensure filter gets pushed down to scan
    rules.put(PlannerPhase.PHYSICAL, PushFilterPastDynamoRowExpander.INSTANCE);

    return rules;
  }

  @Override
  public StoragePluginConfig getConfig() {
    return config;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    parent = factory.registerSchemasWithNewParent(schemaConfig, parent);
    writeErrors.registerSchemas(schemaConfig, parent);
  }

  protected FineoSchemaFactory getFactory(String name) {
    OrgLoader loader =
      new OrgLoader(this.config.getOrgs(), this.config.getDynamoTenantTable(),
        // this needs to be an object for Drill to be happy. Not entirely sure why, its a JDK thing.
        new Supplier<AmazonDynamoDBAsyncClient>() {
          @Override
          public AmazonDynamoDBAsyncClient get() {
            return FineoStoragePlugin.this.getDynamoClient();
          }
        });
    return new FineoSchemaFactory(this, name, loader);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection,
    List<SchemaPath> columns) throws IOException {
    // any physical scan is delegated to the write errors. However, generally, this is not called
    // because we handle the work in the FineoTable
    return this.writeErrors.getPhysicalScan(userName, selection, columns);
  }

  @Override
  public Set<? extends RelOptRule> getOptimizerRules(OptimizerRulesContext optimizerContext,
    PlannerPhase phase) {
    Set<RelOptRule> rules = new HashSet<>(this.rules.get(phase));
    switch (phase) {
      case PHYSICAL:
        rules.addAll(this.writeErrors.getPhysicalOptimizerRules(optimizerContext));
    }
    return rules;
  }

  public DynamoDB getDynamo() {
    if (this.dynamoClient == null) {
      AmazonDynamoDBAsyncClient client = getDynamoClient();
      this.dynamoClient = new DynamoDB(client);
    }
    return dynamoClient;
  }

  @Override
  public void close() throws Exception {
    if (this.dynamoClient != null) {
      this.dynamoClient.shutdown();
    }
    super.close();
  }

  public AmazonDynamoDBAsyncClient getDynamoClient() {
    if (this.client == null) {
      this.client = new AmazonDynamoDBAsyncClient(dynamo.inflateCredentials());
      this.dynamo.getEndpoint().configure(client);
    }
    return client;
  }


  public boolean getEnableRadio() {
    return enableRadio;
  }

  private class EmptyPlugin extends AbstractStoragePlugin {

    @Override
    public StoragePluginConfig getConfig() {
      return null;
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus schemaPlus)
      throws IOException {
      // noop
    }
  }
}
