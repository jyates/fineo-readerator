package io.fineo.drill.exec.store.dynamo.physical;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import io.fineo.drill.exec.store.dynamo.DynamoStoragePlugin;
import io.fineo.drill.exec.store.dynamo.spec.DynamoTableDefinition;
import io.fineo.drill.exec.store.dynamo.config.ClientProperties;
import io.fineo.drill.exec.store.dynamo.config.DynamoEndpoint;
import io.fineo.drill.exec.store.dynamo.config.DynamoStoragePluginConfig;
import io.fineo.drill.exec.store.dynamo.config.ParallelScanProperties;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.StoragePluginRegistry;

import java.util.Iterator;
import java.util.List;

@JsonTypeName("dynamo-segment-scan")
public class DynamoSubScan extends AbstractBase implements SubScan {

  private final DynamoStoragePlugin plugin;
  private final List<DynamoSubScanSpec> specs;
  private final DynamoStoragePluginConfig storage;
  private final List<SchemaPath> columns;
  private final ClientProperties client;
  private final ParallelScanProperties scan;

  public DynamoSubScan(@JacksonInject StoragePluginRegistry registry,
    @JsonProperty("storage") StoragePluginConfig storage,
    @JsonProperty("specs") List<DynamoSubScanSpec> tabletScanSpecList,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("client") ClientProperties client,
    @JsonProperty("scan")ParallelScanProperties scan) throws ExecutionSetupException {
    this((DynamoStoragePlugin) registry.getPlugin(storage), storage, tabletScanSpecList, columns,
      client, scan);
  }

  public DynamoSubScan(DynamoStoragePlugin plugin, StoragePluginConfig config,
    List<DynamoSubScanSpec> specs, List<SchemaPath> columns, ClientProperties client,
    ParallelScanProperties scan) {
    super((String) null);
    this.plugin = plugin;
    this.specs = specs;
    this.storage = (DynamoStoragePluginConfig) config;
    this.columns = columns;
    this.client = client;
    this.scan = new ParallelScanProperties();
  }

  public DynamoSubScan(DynamoSubScan other) {
    super(other);
    this.plugin = other.plugin;
    this.specs = other.specs;
    this.storage = other.storage;
    this.columns = other.columns;
    this.client = other.client;
    scan = other.scan;
  }

  @Override
  public int getOperatorType() {
    // outside the standard operator range
    return 2000;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
    throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
    throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    return new DynamoSubScan(this);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  @Override
  public boolean isExecutable() {
    return false;
  }

  public List<DynamoSubScanSpec> getSpecs() {
    return specs;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  public ClientProperties getClient() {
    return client;
  }

  public DynamoEndpoint getEndpoint() {
    return this.storage.getEndpoint();
  }

  public ParallelScanProperties getScan() {
    return scan;
  }

  @JsonIgnore
  public AWSCredentialsProvider getCredentials() {
    return this.storage.inflateCredentials();
  }

  @JsonTypeName("dynamo-sub-scan-spec")
  public static class DynamoSubScanSpec {
    private final DynamoTableDefinition table;
    private final int totalSegments;
    private final int segmentId;
    private final List<SchemaPath> columns;

    public DynamoSubScanSpec(@JsonProperty("table") DynamoTableDefinition table,
      @JsonProperty("segments") int totalSegments, @JsonProperty("segment-id") int segmentId,
      @JsonProperty("projections") List<SchemaPath> columns) {
      this.table = table;
      this.totalSegments = totalSegments;
      this.segmentId = segmentId;
      this.columns = columns;
    }

    public DynamoTableDefinition getTable() {
      return table;
    }

    public int getTotalSegments() {
      return totalSegments;
    }

    public int getSegmentId() {
      return segmentId;
    }

    public List<SchemaPath> getColumns() {
      return columns;
    }
  }
}
