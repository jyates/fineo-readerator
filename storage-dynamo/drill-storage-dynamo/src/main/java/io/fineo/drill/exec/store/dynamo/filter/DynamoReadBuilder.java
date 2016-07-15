package io.fineo.drill.exec.store.dynamo.filter;

import io.fineo.drill.exec.store.dynamo.spec.DynamoFilterSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

class DynamoReadBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(DynamoReadBuilder.class);
  private List<GetOrQuery> queries = new ArrayList<>();
  private Scan scan;

  private FilterFragment nextHash;
  private FilterFragment nextRange;
  private DynamoFilterSpec nextAttribute;

  private final boolean rangeKeyExists;

  DynamoReadBuilder(boolean rangeKeyExists) {
    this.rangeKeyExists = rangeKeyExists;
  }

  public void and(DynamoReadBuilder that) {
    // if either are scans already, then we are done
    if (this.scan != null || that.scan != null) {
      andScan(that);
      return;
    }

    boolean thisQueries = this.queries.size() > 0;
    boolean thatQueries = that.queries.size() > 0;

    // both have queries, so we need to make a scan
    if (thisQueries && thatQueries) {
      andScan(that);
      return;
    } else if (thatQueries) {
      // queries from them, but no queries from us
      this.queries = that.queries;
    }

    // try to add the attributes
    tryAndNextAttributes(that);
    // oops, it became a scan. Add the attributes that are hang around
    if (this.scan != null) {
      this.scan = buildScan();
    }
  }

  private void tryAndNextAttributes(DynamoReadBuilder build) {
    this.and(build.nextHash);
    this.and(build.nextRange);
    this.andAttribute(build.nextAttribute);
  }

  private void andScan(DynamoReadBuilder build) {
    Scan bs = build.buildScan();
    this.scan = buildScan();
    scan.and(bs);
  }


  public void or(DynamoReadBuilder build) {
    // TODO
  }

  public void and(FilterFragment fragment) {
    // we have a scan or the fragment is an attribute
    if (shouldScan(fragment)) {
      andScan(fragment.getFilter(), fragment.isAttribute());
      return;
    }

    if (fragment.isAttribute()) {
      andAttribute(fragment.getFilter());
      return;
    }

    // fragment is a hash key
    if (andHash(fragment)) {
      return;
    }

    andRange(fragment);
  }

  private boolean andHash(FilterFragment fragment) {
    if (!fragment.isHash()) {
      return false;
    }
    // if we have any gets/queries or another hash keey, AND on a hash key requires scanning
    // everything
    if (nextHash != null || queries.size() > 0) {
      if (nextHash != null) {
        LOG.warn("Two AND conditions on hash keys, must create a scan to cover them! Its "
                 + "unlikely this will ever return any valid data though, unless its "
                 + "something like 'h = 1 & h = 1', which is better served by a get, but we "
                 + "can't determine that without introspecting the query");
      }
      andScan(fragment.getFilter(), fragment.isAttribute());
      return true;
    }

    this.nextHash = fragment;

    // there is no rangePrimaryKey key, we can immediately decide what to do about this part
    if (!rangeKeyExists) {
      if (nextHash.isEquals()) {
        add(new Get(nextHash.getFilter(), nextAttribute));
      } else {
        add(new Query(nextHash.getFilter(), nextAttribute));
      }
      return true;
    }

    // we have a range key
    if (this.nextRange != null) {
      DynamoFilterSpec key = and(nextHash.getFilter(), nextRange.getFilter());
      if (nextHash.isEquals() && nextRange.isEquals()) {
        add(new Get(key, nextAttribute));
        return true;
      }
      // fall through - hash and rangePrimaryKey must be at least equalities (otherwise
      // would have gotten a scan) so we can create a query
      add(new Query(key, nextAttribute));
    }

    // there is no range key, so nothing more to do.
    return true;
  }

  private void andRange(FilterFragment fragment) {
    assert fragment.isRange() : "Supposed to have handled non-range filter fragments at this "
                                + "point! Fragment: " + fragment;
    // there is a hash, so it must be a scan (hash || sort)
    if (nextHash != null) {
      andScan(fragment.getFilter(), false);
      return;
    }

    updateRange(fragment, this::and);
  }

  @FunctionalInterface
  private interface VoidBiFunction<A, B> {
    void apply(A var1, B var2);
  }

  private void setRange(FilterFragment
    fragment, VoidBiFunction<DynamoFilterSpec, DynamoFilterSpec> func) {
    if (nextRange == null) {
      nextRange = fragment;
    } else {
      func.apply(nextRange.getFilter(), fragment.getFilter());
      // its now a multi-range request, so it cannot be an equals fragment
      nextRange.setEquals(false);
    }
  }

  private void andScan(DynamoFilterSpec spec, boolean isAttribute) {
    if (scan == null) {
      scan = buildScan();
    }
    scan.and(spec, !isAttribute);
  }

  private void andAttribute(DynamoFilterSpec spec) {
    // last "thing" we created was a get
    if (queries.size() > 0) {
      // we created a query last
      queries.get(queries.size() - 1).attribute().and(spec);
    } else {
      // nothing created yet, just combine attributes
      nextAttribute = and(nextAttribute, spec);
    }
  }

  private void add(Get get) {
    add(new GetOrQuery(get));
  }

  private void add(Query query) {
    add(new GetOrQuery(query));
  }

  private void add(GetOrQuery gq) {
    queries.add(gq);
    nextHash = null;
    nextRange = null;
    nextAttribute = null;
  }

  private DynamoFilterSpec and(DynamoFilterSpec nextAttribute, DynamoFilterSpec spec) {
    if (nextAttribute == null) {
      return spec;
    } else if (spec == null) {
      return nextAttribute;
    }
    return nextAttribute.and(spec);
  }

  public void or(FilterFragment fragment) {
    // we have to read everything anyways, so just add this spec
    // OR checking a non-equality requires scan - either is an attribute or a non-equality key
    boolean isKey = fragment.isHash() || fragment.isRange();
    if (shouldScan(fragment)) {
      orScan(fragment.getFilter(), isKey);
      return;
    }

    if (fragment.isAttribute()) {
      if (queries.size() > 0 || nextHash != null || nextRange != null) {
        orScan(fragment.getFilter(), false);
      } else {
        // just set the attribute
        if (this.nextAttribute == null) {
          this.nextAttribute = fragment.getFilter();
        } else {
          this.nextAttribute = this.nextAttribute.or(fragment.getFilter());
        }
      }
      return;
    }

    if (fragment.isHash()) {
      if (nextRange != null || nextAttribute != null) {
        orScan(fragment.getFilter(), true);
        return;
      }

      if (nextHash != null) {
        // hash = '1' || hash = '2'
        // we only get here if there was no matching sort condition (which would enable a get),
        // so it has to be a query for the key
        if (rangeKeyExists) {
          add(new Query(nextHash.getFilter(), nextAttribute));
        } else {
          add(new Get(nextHash.getFilter(), nextAttribute));
        }
      }

      nextHash = fragment;
      if (!rangeKeyExists) {
        add(new Get(nextHash.getFilter(), nextAttribute));
      }
      return;
    }

    assert fragment.isRange() : "Should have handled the fragment before here!";

    // hash = 1 || range = 2 OR attr = 'a' || range = 2
    if (nextHash != null || nextAttribute != null) {
      orScan(fragment.getFilter(), true);
      return;
    }

    updateRange(fragment, this::or);
  }

  private void updateRange(FilterFragment
    fragment, VoidBiFunction<DynamoFilterSpec, DynamoFilterSpec> func) {
    if (queries.size() > 0) {
      GetOrQuery gq = queries.get(queries.size() - 1);
      Query query = gq.get != null ?
                    new Query(gq.get.getFilter(), gq.attribute()) :
                    gq.query;
      func.apply(query.getFilter(), fragment.getFilter());
    } else {
      setRange(fragment, func);
    }
  }

  /**
   * Need to have a scan when:
   * <ul>
   * <li>Already have a scan</li>
   * <li>fragment is not checking equality AND not an attribute (is a hash or sort)</li>
   * <li>fragment is hash and not checking equals </li>
   * </ul>
   *
   * @param fragment to check
   * @return if a scan should be run
   */
  private boolean shouldScan(FilterFragment fragment) {
    return scan != null ||
           (!fragment.isAttribute() && !fragment.isEquals()) ||
           (fragment.isHash() && !fragment.isEquals());
  }

  private void orScan(DynamoFilterSpec spec, boolean isKey) {
    if (scan == null) {
      scan = buildScan();
    }
    scan.or(spec, isKey);
  }

  private DynamoFilterSpec or(DynamoFilterSpec nextAttribute, DynamoFilterSpec spec) {
    if (nextAttribute == null) {
      return spec;
    } else if (spec == null) {
      return nextAttribute;
    }
    return nextAttribute.and(spec);
  }

  // Take all the previous gets/queries and combine them into a scan
  private Scan buildScan() {
    Scan scan;
    if (this.scan != null) {
      scan = this.scan;
    } else {
      scan = new Scan();
    }
    for (GetOrQuery gq : queries) {
      LeafQuerySpec query = gq.query == null ? gq.get : gq.query;
      scan.or(query.getFilter(), true);
      scan.or(query.attribute(), false);
    }
    queries.clear();

    // add the edge attributes. They have to be AND or we would have generated a scan
    scan.and(nextHash.getFilter(), true);
    scan.and(nextRange.getFilter(), true);
    scan.and(nextAttribute, false);
    nextHash = null;
    nextRange = null;
    nextAttribute = null;
    return scan;
  }

  private class LeafQuerySpec {
    private final DynamoFilterSpec filter;
    private DynamoFilterSpec attribute;

    public LeafQuerySpec(DynamoFilterSpec filter, DynamoFilterSpec attribute) {
      this.filter = filter;
      this.attribute = attribute;
    }

    public DynamoFilterSpec getFilter() {
      return filter;
    }

    public DynamoFilterSpec attribute() {
      return attribute;
    }
  }

  private class Scan {
    private DynamoFilterSpec key;
    private DynamoFilterSpec attribute;

    public void and(DynamoFilterSpec spec, boolean isKey) {
      set(spec, isKey, DynamoReadBuilder.this::and);
    }

    public void or(DynamoFilterSpec spec, boolean isKey) {
      set(spec, isKey, DynamoReadBuilder.this::or);
    }

    private void set(DynamoFilterSpec spec, boolean isKey, BiFunction<DynamoFilterSpec,
      DynamoFilterSpec, DynamoFilterSpec> func) {
      if (isKey) {
        key = func.apply(key, spec);
      } else {
        attribute = func.apply(attribute, spec);
      }
    }

    public void and(Scan scan) {
      key = DynamoReadBuilder.this.and(key, scan.key);
      attribute = DynamoReadBuilder.this.and(attribute, scan.attribute);
    }
  }

  private class Get extends LeafQuerySpec {

    public Get(DynamoFilterSpec key, DynamoFilterSpec attribute) {
      super(key, attribute);
    }
  }

  private class Query extends LeafQuerySpec {

    public Query(DynamoFilterSpec filter, DynamoFilterSpec nextAttribute) {
      super(filter, nextAttribute);
    }
  }

  private class GetOrQuery {
    private Get get;
    private Query query;

    public GetOrQuery(Get get) {
      this.get = get;
    }

    public GetOrQuery(Query query) {
      this.query = query;
    }

    public DynamoFilterSpec attribute() {
      return get != null ? get.attribute() : query.attribute();
    }
  }
}
