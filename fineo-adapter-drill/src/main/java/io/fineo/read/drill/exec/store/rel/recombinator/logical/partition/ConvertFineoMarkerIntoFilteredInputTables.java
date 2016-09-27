package io.fineo.read.drill.exec.store.rel.recombinator.logical.partition;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.fineo.lambda.dynamo.Range;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.read.drill.exec.store.rel.expansion.logical.DynamoRowFieldExpanderRel;
import io.fineo.read.drill.exec.store.rel.recombinator.FineoRecombinatorMarkerRel;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.SourceType;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.partition.handler
  .DynamoTimestampHandler;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.partition.handler
  .FileSystemTimestampHandler;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.partition.handler.TimestampHandler;
import io.fineo.read.drill.exec.store.schema.FineoTable;
import io.fineo.schema.store.AvroSchemaProperties;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterators.cycle;
import static com.google.common.collect.Iterators.limit;
import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.read.drill.exec.store.rel.recombinator.logical.SourceType.DFS;
import static io.fineo.read.drill.exec.store.rel.recombinator.logical.SourceType.DYNAMO;
import static java.util.stream.Collectors.toList;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.util.ImmutableNullableList.of;
import static org.apache.drill.exec.planner.logical.DrillOptiq.toDrill;
import static org.apache.drill.exec.planner.physical.PrelUtil.getPlannerSettings;

/**
 * Rule that pushes a timerange filter (WHERE) past the recombinator and into the actual scan
 */
public abstract class ConvertFineoMarkerIntoFilteredInputTables extends RelOptRule {

  private static final Logger LOG =
    LoggerFactory.getLogger(ConvertFineoMarkerIntoFilteredInputTables.class);

  private ConvertFineoMarkerIntoFilteredInputTables(RelOptRuleOperand operand, String name) {
    super(operand, name);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    FineoRecombinatorMarkerRel fmr = getRecombinator(call);
    List<RelNode> scans = call.getChildRels(fmr);
    for (RelNode scan : scans) {
      if (!(scan instanceof TableScan)) {
        return false;
      }
    }
    return true;
  }

  protected abstract FineoRecombinatorMarkerRel getRecombinator(RelOptRuleCall call);

  @Override
  public void onMatch(RelOptRuleCall call) {
    FineoRecombinatorMarkerRel fmr = getRecombinator(call);
    RelOptCluster cluster = fmr.getCluster();

    Multimap<SourceType, RelAndRange> tables = getTables(call);
    Multimap<SourceType, RelNode> nodes =
      partitionReads(tables, cluster, fmr.getTraitSet());
    TableTypeSetMarker marker =
      new TableTypeSetMarker(fmr.getCluster(), fmr.getTraitSet(), fmr.getRowType());
    List<RelNode> sources = new ArrayList<>();
    List<SourceType> types = new ArrayList<>();
    nodes.asMap().entrySet().stream()
         .forEach(e -> {
           types.addAll(newArrayList(limit(cycle(e.getKey()), e.getValue().size())));
           sources.addAll(e.getValue());
         });
    marker.setInputs(sources, types);
    RelNode fineo = fmr.copy(fmr.getTraitSet(), of(marker));
    call.transformTo(finalTransform(fineo, call));
  }

  private void setInputs(TableTypeSetMarker marker, Map.Entry<SourceType, Collection<RelNode>> e) {
    List<SourceType> typeList = newArrayList(limit(cycle(e.getKey()), e.getValue().size()));
    marker.setInputs(e.getValue(), typeList);
  }

  protected RelNode finalTransform(RelNode fineo, RelOptRuleCall call) {
    return fineo;
  }

  protected Multimap<SourceType, RelNode> partitionReads(Multimap<SourceType,
    RelAndRange> translatedScans, RelOptCluster cluster, RelTraitSet traits) {
    Preconditions.checkState(translatedScans.size() > 0,
      "Couldn't find any tables that apply to the scan!");

    RexBuilder rexer = cluster.getRexBuilder();
    Multimap<SourceType, RelNode> groups = ArrayListMultimap.create();
    // simple case, no dynamo tables
    Collection<RelAndRange> dynamo = translatedScans.get(DYNAMO);
    Collection<RelAndRange> dfs = translatedScans.get(DFS);
    if (dynamo == null || dynamo.size() == 0) {
      groups.putAll(DFS, dfs.stream().map(RelAndRange::getRel).collect(Collectors.toList()));
      return groups;
    } else if (dfs == null || dfs.size() == 0) {
      // this is really weird, but I guess it could come up...
      groups.putAll(DYNAMO, dynamo.stream().map(RelAndRange::getRel).collect(Collectors.toList()));
      return groups;
    }

    // TODO lookup the last 'convert' time from json -> parquet to see how far in the future we
    // can filter parquet scans and progressively 'lop' off dynamo tables and replace them with
    // parquet scans

    // right now, we just do the simple thing - find the oldest timerange for which we have a
    // table and then relegate the FS reads to everything just before that
    OptionalLong min = dynamo.stream().mapToLong(rr -> rr.start.toEpochMilli()).min();
    Preconditions.checkState(min.isPresent(),
      "Could not determine the nearline (Dynamo) table to read - no min timestamp found!");
    Instant absoluteStart = Instant.ofEpochMilli(min.getAsLong());
    RexNode startLiteral = rexer
      .makeLiteral(absoluteStart.toEpochMilli(), rexer.getTypeFactory().createSqlType(BIGINT),
        true);
    RexNode startDate = FileSystemTimestampHandler.asValueNode(absoluteStart.toEpochMilli(), rexer);

    List<RelNode> filteredDfs =
      dfs.stream()
         .map(RelAndRange::getRel)
         .map(node -> {
           // TODO replace this with another instance of WrappingFilterBuilder + TimestampHandler
           // timestamp is strictly less than the minimum dynamo time value
           RelDataTypeField field =
             node.getRowType().getField(AvroSchemaProperties.TIMESTAMP_KEY, false, false);
           RexInputRef ref = rexer.makeInputRef(node, field.getIndex());
           RexNode limit = rexer.makeCall(LESS_THAN, ref, startLiteral);

           // don't read more directories than necessary
           RelDataTypeField dir = FileSystemTimestampHandler.getTimeDir(node);
           assert dir != null : "Didn't find a dir0 in fs scan type!";
           RexNode dirFilter = FileSystemTimestampHandler.fileScanOpToRef(rexer, node, dir,
             LESS_THAN_OR_EQUAL, startDate);
           RexNode condition = RexUtil.composeConjunction(rexer, of(limit, dirFilter), true);

           // build the filter
           return new LogicalFilter(cluster, traits, node, condition);
         })
         .collect(toList());
    groups.putAll(DFS, filteredDfs);

    dynamo.stream().map(RelAndRange::getRel).forEach(rel -> groups.put(DYNAMO, rel));
    return groups;
  }

  public static class PushTimerangeFilterPastRecombinator extends
                                                          ConvertFineoMarkerIntoFilteredInputTables {

    public static final PushTimerangeFilterPastRecombinator
      INSTANCE = new PushTimerangeFilterPastRecombinator();

    private PushTimerangeFilterPastRecombinator() {
      super(operand(LogicalFilter.class, operand(FineoRecombinatorMarkerRel.class,
        unordered(operand(TableScan.class, none())))),
        "Fineo::PushTimerangePastRecombinatorRule");
    }

    @Override
    protected FineoRecombinatorMarkerRel getRecombinator(RelOptRuleCall call) {
      return call.rel(1);
    }

    @Override
    public Multimap<SourceType, RelAndRange> getTables(RelOptRuleCall call) {
      LogicalFilter filter = call.rel(0);
      FineoRecombinatorMarkerRel fmr = getRecombinator(call);
      List<RelNode> scans = call.getChildRels(fmr);
      Multimap<SourceType, RelAndRange> translatedScans = ArrayListMultimap.create();
      String ts = FineoTable.BaseField.TIMESTAMP.getName();

      final LogicalExpression conditionExp =
        toDrill(new DrillParseContext(getPlannerSettings(call.getPlanner())), fmr, filter
          .getCondition());

      RexBuilder rexer = filter.getCluster().getRexBuilder();
      Map<SourceType, TimestampHandler> handlers = getHandlers(rexer);
      TimestampExpressionBuilder builder = new TimestampExpressionBuilder(ts);
      WrappingFilterBuilder wfb = new WrappingFilterBuilder(rexer);
      for (RelNode s : scans) {
        builder.reset();
        GroupedScan group = group(fmr.getTraitSet(), s);
        LOG.debug("Got group: {}", group);
        TimestampHandler handler = handlers.get(group.source);

        RexNode shouldScan =
          builder.lift(conditionExp, rexer, handler.getShouldScanBuilder(group.tableName));
        Range<Instant> range = handler.getTableTimeRange(group.tableName);
        LOG.debug("With range: {} - {}", range.getStart(), range.getEnd());
        if (builder.isScanAll() || shouldScan == null) {
          // we have to scan everything b/c we didn't understand all the timestamp constraints
          //    OR
          // there is no timestamp constraint, in which case we need to scan everything
          LOG.debug("Have to scan everything!");
          translatedScans.put(group.source, new RelAndRange(group.scan, range));
        } else if (shouldScan != null && evaluate(shouldScan)) {
          // we can make a pretty good guess about the scan
          builder.reset();
          TableFilterBuilder filterBuilder = handler.getFilterBuilder();
          wfb.setup(group.scan, filterBuilder);
          RelNode translated = wfb.buildFilter(builder, conditionExp);
          if (translated != null) {
            translatedScans.put(group.source, new RelAndRange(translated, range));
          } else {
            LOG.debug("Skipping scan because filter builder ignored it.");
          }
        }
      }

      return translatedScans;
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

    @Override
    protected RelNode finalTransform(RelNode fineo, RelOptRuleCall call) {
      LogicalFilter filter = call.rel(0);
      return filter.copy(filter.getTraitSet(), fineo, filter.getCondition());
    }
  }

  protected abstract Multimap<SourceType, RelAndRange> getTables(RelOptRuleCall call);

  public static class FilterRecombinatorTablesWithNoTimestampFilter
    extends ConvertFineoMarkerIntoFilteredInputTables {
    public static final FilterRecombinatorTablesWithNoTimestampFilter INSTANCE = new
      FilterRecombinatorTablesWithNoTimestampFilter();

    private FilterRecombinatorTablesWithNoTimestampFilter() {
      super(operand(FineoRecombinatorMarkerRel.class,
        unordered(operand(TableScan.class, none()))),
        "Fineo::FilterRecombinatorTablesRule");
    }

    @Override
    protected FineoRecombinatorMarkerRel getRecombinator(RelOptRuleCall call) {
      return call.rel(0);
    }

    @Override
    public Multimap<SourceType, RelAndRange> getTables(RelOptRuleCall call) {
      // similar to what we do above, but the table scans are completely inclusive, so we just
      // separate out the fields by type
      Multimap<SourceType, RelAndRange> types = ArrayListMultimap.create();
      FineoRecombinatorMarkerRel fmr = getRecombinator(call);
      RexBuilder rexer = fmr.getCluster().getRexBuilder();
      Map<SourceType, TimestampHandler> handlers = getHandlers(rexer);
      List<RelNode> scans = call.getChildRels(fmr);
      for (RelNode node : scans) {
        GroupedScan group = group(fmr.getTraitSet(), node);
        LOG.debug("Found table group: {}", group);
        SourceType type = group.source;
        TimestampHandler handler = handlers.get(type);
        types.put(type,
          new RelAndRange(group.scan, handler.getTableTimeRange(group.tableName)));
      }

      LOG.debug("Found tables/ranges: \n{}", types);
      return types;
    }
  }

  protected static Map<SourceType, TimestampHandler> getHandlers(RexBuilder rexer) {
    Map<SourceType, TimestampHandler> handlers = new HashMap<>();
    handlers.put(DFS, new FileSystemTimestampHandler(rexer));
    handlers.put(DYNAMO, new DynamoTimestampHandler(rexer));
    return handlers;
  }

  private static class RelAndRange {
    private Instant start;
    private RelNode rel;

    public RelAndRange(RelNode translated, Range<Instant> tableTimeRange) {
      this.rel = translated;
      this.start = tableTimeRange.getStart();
    }

    public RelNode getRel() {
      return rel;
    }

    @Override
    public String toString() {
      return "RelAndRange{" +
             "start=" + start +
             ", rel=" + rel +
             '}';
    }
  }


  private static GroupedScan group(RelTraitSet traits, RelNode node) {
    TableScan scan = (TableScan) node;
    SourceType type = getScanType(scan);
    List<String> names = scan.getTable().getQualifiedName();
    String tableName = names.get(names.size() - 1);
    switch (type) {
      case DYNAMO:
        node = wrapInDynamoExpander(traits, scan);
    }
    return new GroupedScan(type, tableName, node);
  }

  private static SourceType getScanType(TableScan scan) {
    List<String> name = scan.getTable().getQualifiedName();
    return SourceType.valueOf(name.get(0).toUpperCase());
  }

  private static RelNode wrapInDynamoExpander(RelTraitSet traits, RelNode scan) {
    // add the id field
    scan.getRowType().getField(Schema.ID_FIELD, true, true);
    return new DynamoRowFieldExpanderRel(traits, scan);
  }

  private static class GroupedScan {
    private final SourceType source;
    private final String tableName;
    private final RelNode scan;

    public GroupedScan(SourceType source, String tableName, RelNode scan) {
      this.source = source;
      this.tableName = tableName;
      this.scan = scan;
    }

    @Override
    public String toString() {
      return "GroupedScan{" +
             "source=" + source +
             ", tableName='" + tableName + '\'' +
             ", scan=" + scan +
             '}';
    }
  }
}
