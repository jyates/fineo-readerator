package io.fineo.drill.exec.store.dynamo.filter;

import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoFilterSpec;

/**
 *
 */
public class FilterFragment {
  private DynamoFilterSpec filter;
  private boolean equals;
  private boolean equality;
  private boolean isHash;
  private boolean isRange;

  public FilterFragment(DynamoFilterSpec filter, boolean equals, boolean equality, boolean isHash,
    boolean isRange) {
    this.filter = filter;
    this.equals = equals;
    this.equality = equality;
    this.isHash = isHash;
    this.isRange = isRange;
  }

  public DynamoFilterSpec getFilter() {
    return filter;
  }

  public boolean isEquals() {
    return equals;
  }

  public void setEquals(boolean equals) {
    this.equals = equals;
  }

  public boolean isEquality() {
    return equality;
  }

  public boolean isHash() {
    return isHash;
  }

  public boolean isRange() {
    return isRange;
  }

  public boolean isAttribute() {
    return !(isHash() || isRange());
  }

  @Override
  public String toString() {
    return "FilterFragment{" +
           "filter=" + filter +
           ", equals=" + equals +
           ", equality=" + equality +
           ", isHash=" + isHash +
           ", isRange=" + isRange +
           '}';
  }
}
