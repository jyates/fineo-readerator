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
import io.fineo.drill.exec.store.dynamo.physical.DynamoSubScan;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.CompleteWork;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
  private long bytes;
  private List<EndpointAffinity> affinities;
  private ArrayList<DynamoWork> work;

  @JsonCreator
  public DynamoGroupScan(@JsonProperty(DynamoScanSpec.NAME) DynamoScanSpec dynamoSpec,
    @JsonProperty("storage") DynamoStoragePluginConfig storagePluginConfig,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("credentials") Map<String, Object> credentials,
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
    TableDescription desc = this.plugin.getModel().getTable(this.spec.getTableName())
                                       .getDescription();
    this.bytes = desc.getTableSizeBytes();
  }

  public DynamoGroupScan(DynamoGroupScan other) {
    super((String) null);
    this.plugin = other.plugin;
    this.spec = other.spec;
    this.columns = other.columns;
    this.config = other.config;
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

    // no affinity for any work unit
    this.work = new ArrayList<>();
    long portion = this.bytes / max;
    for (int i = 0; i < max; i++) {
      work.add(new DynamoWork(i, portion));
    }

    this.assignments = AssignmentCreator.getMappings(endpoints, work, plugin.getContext());
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    if (affinities == null) {
      affinities = AffinityCreator.getAffinityMap(work);
    }
    return affinities;
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    List<DynamoWork> segments = assignments.get(minorFragmentId);
    List<DynamoSubScan.DynamoSubScanSpec> subSpecs = new ArrayList<>(segments.size());
    for (DynamoWork work : segments) {
      subSpecs.add(new DynamoSubScan.DynamoSubScanSpec(getSpec()
        .getTableName(), assignments.size(), work.getSegment(), getColumns()));

    }
    return new DynamoSubScan(plugin, plugin.getConfig(), subSpecs, this.columns,
      getSpec().getClient(), getSpec().getScan().getLimit());
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
