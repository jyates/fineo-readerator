package io.fineo.read.drill.exec.store.rel.physical;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.fineo.internal.customer.Metric;
import org.apache.drill.exec.physical.base.AbstractPhysicalVisitor;
import org.apache.drill.exec.physical.base.AbstractSingle;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;

import java.util.List;
import java.util.Map;

@JsonTypeName("fineo-recomb")
public class Recombinator extends AbstractSingle {

  private Metric metric;

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

  public Metric getMetric() {
    return metric;
  }
}
