package io.fineo.drill.exec.store.dynamo.filter;

import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoFilterSpec;

/**
 * Builder to construct a "between" clause for a column
 */
public class BetweenBuilder {

  private static final String GTE = "greater_than_or_equal_to";
  private static final String LTE = "less_than_or_equal_to";
  private final boolean range;

  private SingleFunctionProcessor gte;
  private SingleFunctionProcessor lte;

  public BetweenBuilder(boolean isRange) {
    this.range = isRange;
  }

  public boolean addFunction(SingleFunctionProcessor processor) {
    String name = processor.getFunctionName();
    if (name.equals(GTE)) {
      if (gte != null) {
        // check to see if the new value is greater that the previous
        assert processor.getValue() instanceof Comparable;
        if (((Comparable) gte.getValue()).compareTo(processor.getValue()) > 0) {
          return false;
        }
      }
      gte = processor;
    } else if (name.equals(LTE)) {
      if (lte != null) {
        // check to see if there new value is less than the current value
        assert processor.getValue() instanceof Comparable;
        if (((Comparable) lte.getValue()).compareTo(processor.getValue()) < 0) {
          return false;
        }
      }
      lte = processor;
    }
    return true;
  }

  public FilterFragment build() {
    if (lte == null || gte == null) {
      return null;
    }
    DynamoFilterSpec spec = DynamoFilterSpec.create("between", gte.getPath().getAsUnescapedPath()
      , gte.getValue(), lte.getValue());
    return new FilterFragment(spec, false, true, false, range);
  }
}
