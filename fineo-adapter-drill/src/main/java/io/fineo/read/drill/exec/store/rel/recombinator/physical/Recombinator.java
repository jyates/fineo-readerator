package io.fineo.read.drill.exec.store.rel.recombinator.physical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fineo.internal.customer.Metric;
import io.fineo.read.drill.exec.store.rel.MetricUtils;
import org.apache.drill.exec.physical.base.AbstractSingle;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.htrace.fasterxml.jackson.annotation.JsonGetter;

import java.io.IOException;

@JsonTypeName("fineo-recomb")
@JsonDeserialize
public class Recombinator extends AbstractSingle {

  private final Metric metric;

  @JsonCreator
  public Recombinator(@JsonProperty("child") PhysicalOperator child, String metricString)
    throws IOException {
    this(child, MetricUtils.parseMetric(metricString));
  }

  public Recombinator(PhysicalOperator child, Metric metric) {
    super(child);
    this.metric = metric;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new Recombinator(child, metric);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
    throws E {
    return physicalVisitor.visitOp(this, value);
  }

  @Override
  public int getOperatorType() {
    // much beyond the core operator types
    return 1001;
  }

  @JsonIgnore
  public Metric getMetricObj() {
    return metric;
  }

  @JsonGetter("metric")
  public String getMetric() throws IOException {
    return MetricUtils.getMetricString(metric);
  }
}
