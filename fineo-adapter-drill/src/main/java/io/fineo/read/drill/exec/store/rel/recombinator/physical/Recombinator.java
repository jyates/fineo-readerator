package io.fineo.read.drill.exec.store.rel.recombinator.physical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fineo.internal.customer.Metric;
import io.fineo.read.drill.FineoInternalProperties;
import io.fineo.read.drill.exec.store.rel.MetricUtils;
import io.fineo.read.drill.exec.store.rel.recombinator.logical.SourceType;
import org.apache.drill.exec.physical.base.AbstractSingle;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.htrace.fasterxml.jackson.annotation.JsonGetter;

import java.io.IOException;

@JsonTypeName("fineo-recomb")
@JsonDeserialize
public class Recombinator extends AbstractSingle {

  private final Metric metric;
  private final SourceType source;

  @JsonCreator
  public Recombinator(@JsonProperty("child") PhysicalOperator child,
    @JsonProperty("metric") String metricString, @JsonProperty("source") String source)
    throws IOException {
    this(child, MetricUtils.parseMetric(metricString), SourceType.valueOf(source));
  }

  public Recombinator(PhysicalOperator child, Metric metric, SourceType source) {
    super(child);
    this.metric = metric;
    this.source = source;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new Recombinator(child, metric, source);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
    throws E {
    return physicalVisitor.visitOp(this, value);
  }

  @Override
  public int getOperatorType() {
    return FineoInternalProperties.OperatorIndex.RECOMBINATOR;
  }

  @JsonIgnore
  public Metric getMetricObj() {
    return metric;
  }

  @JsonIgnore
  public SourceType getSourceType() {
    return this.source;
  }

  @JsonGetter("metric")
  public String getMetric() throws IOException {
    return MetricUtils.getMetricString(metric);
  }

  @JsonProperty("source")
  public String getSource() {
    return source.name();
  }
}
