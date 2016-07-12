package io.fineo.drill.exec.store.dynamo;

import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import io.fineo.drill.exec.store.dynamo.config.DynamoStoragePluginConfig;
import io.fineo.drill.exec.store.dynamo.config.ParallelScanProperties;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.CompleteWork;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Logical representation of a scan of a dynamo table. Actual fragments of the scan that get
 */
@JsonTypeName(DynamoGroupScan.NAME)
public class DynamoGroupScan extends AbstractGroupScan {
  public static final String NAME = "dynamo-scan";
  // limit imposed by AWS
  private static final int MAX_DYNAMO_PARALLELIZATION = 1000000;

  private final DynamoStoragePlugin plugin;
  private final DynamoScanSpec spec;
  private List<SchemaPath> columns;
  private final StoragePluginConfig config;
  private ListMultimap<Integer, DynamoWork> assignments;

  // used to calculate the work distribution
  private ArrayList<DynamoWork> work;
  private TableDescription desc;
  private boolean filterPushedDown;

  @JsonCreator
  public DynamoGroupScan(@JsonProperty(DynamoScanSpec.NAME) DynamoScanSpec dynamoSpec,
    @JsonProperty("storage") DynamoStoragePluginConfig storagePluginConfig,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException,
    ExecutionSetupException {
    this((DynamoStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), dynamoSpec, columns);
  }

  public DynamoGroupScan(DynamoStoragePlugin plugin, DynamoScanSpec dynamoSpec,
    List<SchemaPath> columns) {
    super((String) null);
    this.plugin = plugin;
    this.spec = dynamoSpec;
    this.columns = columns;
    this.config = plugin.getConfig();
    init();
  }

  private void init() {
    try {
      this.desc = this.plugin.getModel().getTable(this.spec.getTable().getName()).waitForActive();
    } catch (InterruptedException e) {
      throw new DrillRuntimeException(e);
    }
  }

  public DynamoGroupScan(DynamoGroupScan other) {
    super((String) null);
    this.plugin = other.plugin;
    this.spec = other.spec;
    this.columns = other.columns;
    this.config = other.config;
    this.desc = other.desc;
  }

  @Override
  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints)
    throws PhysicalOperatorSetupException {
    ParallelScanProperties scan = getSpec().getScan();
    int max = getMaxParallelizationWidth();
    // determine how many segments we can add to each endpoint
    int totalPerEndpoint = scan.getSegmentsPerEndpoint() * endpoints.size();
    // more scans than the total allowed, so figure out how many we can have per endpoint
    if (totalPerEndpoint < max) {
      max = totalPerEndpoint;
    }

    // attempt to evenly portion the rows across the endpoints
    long rowsPerEndpoint = desc.getItemCount() / endpoints.size();
    long units = rowsPerEndpoint / scan.getApproximateRowsPerEndpoint();
    // we have less than approx 1 segment's worth of work
    if (units == 0) {
      units = 1;
    }

    if (units > max) {
      units = max;
    }

    // no affinity for any work unit
    this.work = new ArrayList<>();
    long portion = this.desc.getTableSizeBytes() / units;
    for (int i = 0; i < units; i++) {
      work.add(new DynamoWork(i, portion));
    }

    this.assignments = AssignmentCreator.getMappings(endpoints, work, plugin.getContext());
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    List<DynamoWork> segments = assignments.get(minorFragmentId);
    List<DynamoSubScan.DynamoSubScanSpec> subSpecs = new ArrayList<>(segments.size());
    for (DynamoWork work : segments) {
      subSpecs
        .add(new DynamoSubScan.DynamoSubScanSpec(getSpec().getTable(), assignments.size(), work
          .getSegment(), getColumns(), getSpec().getScan().getLimit()));

    }
    return new DynamoSubScan(plugin, plugin.getConfig(), subSpecs, this.columns,
      getSpec().getClient());
  }

  @Override
  public int getMaxParallelizationWidth() {
    return Math.min(getSpec().getScan().getMaxSegments(), MAX_DYNAMO_PARALLELIZATION);
  }

  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @Override
  public ScanStats getScanStats() {
    // for simple scans, we have to look at all the rows
    long recordCount = desc.getItemCount();
    // based on how much information we push down (e.g. filter turns scan in query/get) we can
    // recalculate the number of rows read

    long cpuCost = 0;
    long diskCost = 0;
    return new ScanStats(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT, recordCount,
      cpuCost, diskCost);
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    DynamoGroupScan scan = new DynamoGroupScan(this);
    scan.columns = columns;
    return scan;
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new DynamoGroupScan(this);
  }

  @JsonProperty
  public DynamoScanSpec getSpec() {
    return spec;
  }

  @JsonProperty
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty("storage")
  public StoragePluginConfig getConfig() {
    return config;
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return "DynamoGroupScan{" +
           ", spec=" + spec +
           ", columns=" + columns +
           '}';
  }

  public boolean isFilterPushedDown() {
    return filterPushedDown;
  }

  public void setFilterPushedDown(boolean filterPushedDown) {
    this.filterPushedDown = filterPushedDown;
  }

  private class DynamoWork implements CompleteWork {
    private final long bytes;
    private EndpointByteMapImpl byteMap = new EndpointByteMapImpl();
    private int segment;

    public DynamoWork(int segment, long bytes) {
      this.segment = segment;
      this.bytes = bytes;
    }

    public int getSegment() {
      return segment;
    }

    @Override
    public long getTotalBytes() {
      return bytes;
    }

    @Override
    public EndpointByteMap getByteMap() {
      return byteMap;
    }

    @Override
    public int compareTo(CompleteWork o) {
      if (o instanceof DynamoWork) {
        return Integer.compare(this.segment, ((DynamoWork) o).getSegment());
      }
      return -1;
    }
  }
}
