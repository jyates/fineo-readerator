package io.fineo.read.drill.exec.store.rel.physical;

import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.exec.physical.base.AbstractSingle;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;

import java.util.List;
import java.util.Map;

@JsonTypeName("fineo-recomb")
public class Recombinator extends AbstractSingle {

  private final Map<String, List<String>> map;

  public Recombinator(PhysicalOperator child, Map<String, List<String>> map) {
    super(child);
    this.map = map;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new Recombinator(child, map);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
    throws E {
    return null;
  }

  @Override
  public int getOperatorType() {
    // much beyond the core operator types
    return 1001;
  }

  public Map<String, List<String>> getMap() {
    return map;
  }
}
