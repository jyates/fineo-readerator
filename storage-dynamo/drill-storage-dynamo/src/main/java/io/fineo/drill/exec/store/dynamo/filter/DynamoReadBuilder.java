package io.fineo.drill.exec.store.dynamo.filter;

import io.fineo.drill.exec.store.dynamo.spec.DynamoFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoGetFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoGroupScanSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoQueryFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoReadFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoTableDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

class DynamoReadBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(DynamoReadBuilder.class);

  private enum CASE {
    UNSET,
    AND,
    OR;
  }

  private List<GetOrQuery> queries = new ArrayList<>();
  private Scan scan;

  private FilterFragment nextHash;
  private FilterFragment nextRange;
  private DynamoFilterSpec nextAttribute;
  private CASE AND_OR = CASE.UNSET;

  private final boolean rangeKeyExists;

  DynamoReadBuilder(boolean rangeKeyExists) {
    this.rangeKeyExists = rangeKeyExists;
  }

  /**
   * Non-typed setting of the fragment. This should only be used the first time
   *
   * @param fragment to set
   */
  public void set(FilterFragment fragment) {
    assert AND_OR == CASE.UNSET;
    assert queries.size() == 0;
    assert nextHash == null;
    assert nextRange == null;
    assert nextAttribute == null;
    if (fragment.isHash()) {
      nextHash = fragment;
      if (!this.rangeKeyExists) {
        createGetOrQuery();
      }
    } else if (fragment.isRange()) {
      nextRange = fragment;
    } else {
      assert fragment.isAttribute();
      nextAttribute = fragment.getFilter();
    }
  }

  private void and() {
    AND_OR = CASE.AND;
  }

  private void or() {
    AND_OR = CASE.OR;
  }

  /**
   * Scan & (anything) -> Scan
   * <p>
   * (Get/Query ...) && (Get/Query...) -> Scan, degenerate case of hash && hash -> scan
   * <p>
   * (Get/Query ...) && hash -> scan<br/>
   * (Get/Query ...) && sort -> update latest<br/>
   * (Get/Query ...) && attr -> update latest<br/>
   */
  public void and(DynamoReadBuilder that) {
    if (that == null) {
      return;
    }
    and();

    // if either are scans already, then just combine them
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
      this.queries.addAll(that.queries);
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

  /**
   * Scan || anything -> scan
   * <p>
   * (get/query...) || (get/query...) -> (get/query...)
   * <p>
   * (get/query...) || hash -> get/query<br/>
   * (get/query...) || sort -> scan<br/>
   * (get/query...) || attr -> scan
   */
  public void or(DynamoReadBuilder that) {
    if (that == null) {
      return;
    }
    CASE prev = AND_OR;
    or();
    // if either are scans already, then just combine them
    if (this.scan != null || that.scan != null) {
      orScan(that);
      return;
    }

    boolean hadQueries = !this.queries.isEmpty();
    this.queries.addAll(that.queries);

    /*
     * Handle the leftovers. They have to be anded together, to make this not already a scan.
     *   hash && hash -> scan
     *   hash || hash -> first => get/query; second => could be get/query
     *
     *   hash && sort -> turns in get/query
     *   hash || sort -> scan
     *
     *   hash && attr -> could be query
     *   hash || attr -> scan
     *
     *   sort && sort => sort -> could be query
     *   sort || sort => sort -> could be query
     *
     *   sort && attr -> could be query
     *   sort || attr -> scan
     *
     *   queries && hash -> scan
     *   queries || hash -> could be query/get
     *
     *   queries && sort -> added to last query
     *   queries || sort -> scan
     *
     *   queries && attr -> added to last query
     *   queries || attr -> scan
     * We essentially need to handle the cases that don't automatically turn into scan/get/query.
     */
    if (!(that.nextHash == null && that.nextRange == null && that.nextAttribute == null)) {
      // has some queries, so only thing that could be left is "|| hash"
      if (that.queries.size() > 0) {
        assert that.AND_OR == CASE.OR : "Have previous queries but not a sort and CASE = AND";
        assert that.nextRange == null : "NextRange should have been merged into queries";
        assert that.nextAttribute == null : "NextAttr should have been merged into queries";
        assert that.nextHash != null : "Should only have nextHash as the not null value!";

        or(that.nextHash);
      } else {
        switch (that.AND_OR) {
          // there are no queries, so there could be more leftovers. What we do depends on the how
          // they were added
          case UNSET:
            // could be anything, so just OR the combination
            or(that.nextHash);
            or(that.nextRange);
            orAttribute(that.nextAttribute);
            break;
          // we have more than one thing set, so combine them appropriately
          case AND:
            // could be:
            //  1. hash && attr
            //  2. sort && attr
            if (that.nextHash != null) {
              assert that.nextRange == null;
              assert that.nextAttribute != null;
              add(new Query(that.nextHash.getFilter(), that.nextAttribute));
            } else {
              // AND has priority in evaluation, so we have to create a scan to support the
              // filter on the range and attribute
              assert this.AND_OR != CASE.UNSET : "No fragment operator in attempted merge OR";
              assert that.nextRange != null;
              assert that.nextAttribute != null;
              this.scan = buildScan();
              this.scan.or(that.nextRange.getFilter().and(that.nextAttribute));
            }
            break;
          case OR:
            // could be:
            //  1. sort
            //  2. hash
            assert that.nextAttribute == null;
            or(that.nextHash);
            or(that.nextRange);
        }
      }
    }

    if (this.scan != null) {
      this.scan = buildScan();
    }
  }

  private void orScan(DynamoReadBuilder that) {
    Scan thatScan = that.buildScan();
    this.scan = buildScan();
    scan.or(thatScan);
  }

  /**
   * hash && hash -> scan
   * <p>
   * hash && sort -> get/query, dependning on fragment.equals<br/>
   * sort && hash -> "    "
   * <p>
   * attr && hash -> anything<br/>
   * hash && attr -> anything
   * <p>
   * sort && attr -> anything<br/>
   * attr && sort -> anything
   * <p>
   * sort && sort -> query or scan
   */
  public void and(FilterFragment fragment) {
    if (fragment == null) {
      return;
    }
    and();
    // we have a scan or the fragment is an attribute
    if (shouldScan(fragment)) {
      andScan(fragment.getFilter());
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
      andScan(fragment.getFilter());
      return true;
    }

    this.nextHash = fragment;

    // there is no rangePrimaryKey key, we can immediately decide what to do about this part
    if (!rangeKeyExists) {
      createGetOrQuery();
      return true;
    }

    // we have a range key
    if (this.nextRange != null) {
      createGetOrQuery();
    }

    // there is no range key, so nothing more to do.
    return true;
  }

  private void andRange(FilterFragment fragment) {
    assert fragment.isRange() : "Supposed to have handled non-range filter fragments at this "
                                + "point! Fragment: " + fragment;
    // there is a hash, so it must be a scan (hash && sort)
    if (nextHash != null) {
      setRange(fragment, this::and);
      createGetOrQuery();
      return;
    }

    updateRange(fragment, this::and);
  }

  public void andScanSpec(DynamoReadFilterSpec scan) {
    if (this.scan == null) {
      this.scan = buildScan();
    }
    andScan(scan);
  }

  public void andGetOrQuery(List<DynamoReadFilterSpec> getOrQuery) {
    // AND with queries/gets automatically creates a scan
    // GET && GET (hash = 1 && hash = 2)
    if (this.scan == null) {
      this.scan = buildScan();
    }

    for (DynamoReadFilterSpec spec : getOrQuery) {
      andScan(spec);
    }
  }

  private void andScan(DynamoReadFilterSpec spec) {
    and(scan.spec, spec.getKeyFilter());
  }

  public DynamoGroupScanSpec buildSpec(DynamoTableDefinition def) {
    DynamoReadFilterSpec scan = null;
    List<DynamoReadFilterSpec> queries = null;
    if (this.scan != null) {
      scan = new DynamoReadFilterSpec(this.scan.spec);
    } else {
      // check to see if we have any hanging fragments that would change anything.
      if (this.nextHash != null) {
        createGetOrQuery();
      } else if (this.nextRange != null || this.nextAttribute != null) {
        this.scan = buildScan();
        scan = new DynamoReadFilterSpec(this.scan.spec);
        return new DynamoGroupScanSpec(def, scan, queries);
      }

      // no more hanging attributes
      queries = new ArrayList<>();
      for (GetOrQuery gq : this.queries) {
        if (gq.get != null) {
          queries.add(new DynamoGetFilterSpec(gq.get.getFilter()));
        } else {
          queries.add(new DynamoQueryFilterSpec(gq.query.getFilter(), gq.attribute()));
        }
      }
    }

    return new DynamoGroupScanSpec(def, scan, queries);
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

  private void andScan(DynamoFilterSpec spec) {
    if (scan == null) {
      scan = buildScan();
    }
    scan.and(spec);
  }

  private void andAttribute(DynamoFilterSpec spec) {
    // last "thing" we created was a get
    if (queries.size() > 0) {
      GetOrQuery gq = queries.remove(queries.size() - 1);
      Query query = gq.query;
      if (query == null) {
        query = new Query(gq.get.getFilter(), null);
      }
      // we created a query last
      query.setAttribute(and(query.attribute(), spec));
      add(query);
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

  /*
   * hash || sort -> scan<br/>
   * sort || hash -> scan
   * <p>
   * attr || hash -> scan<br/>
   * hash || attr -> scan
   * <p>
   * hash || hash -> get/query, depending on hash key<br/>
   * sort || sort -> scan or query, but equals = false<br/>
   * attr || attr -> attr
   * <p>
   * attr || sort -> scan<br/>
   * sort || attr -> scan
   */
  private void or(FilterFragment fragment) {
    if (fragment == null) {
      return;
    }
    or();
    // we have to read everything anyways, so just add this spec
    // OR checking a non-equality requires scan - either is an attribute or a non-equality key
    boolean isKey = fragment.isHash() || fragment.isRange();
    if (shouldScan(fragment)) {
      orScan(fragment.getFilter(), isKey);
      return;
    }

    if (fragment.isAttribute()) {
      orAttribute(fragment.getFilter());
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
          createGetOrQuery();
        }
      }

      nextHash = fragment;
      if (!rangeKeyExists) {
        createGetOrQuery();
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

  /**
   * Gets cannot support a condition key, so we switch over to using a Query. This will evaluate
   * the attribute filter on the server side, but still reads the attribute; its better than
   * materializing the attribute and then sending its across the wire, transferring it to a buffer
   * and then filter on it above... I think.
   */
  private void createGetOrQuery() {
    assert nextHash != null : "Must at least have a nextHash specified when building Get/Query";
    DynamoFilterSpec range = nextRange == null ? null : nextRange.getFilter();
    DynamoFilterSpec key = and(nextHash.getFilter(), range);
    boolean validRange = !rangeKeyExists || (nextRange != null && nextRange.isEquals());
    if (nextHash.isEquals() && validRange && nextAttribute == null) {
      add(new Get(key));
    } else {
      add(new Query(key, nextAttribute));
    }
  }

  private void orAttribute(DynamoFilterSpec attr) {
    if (attr == null) {
      return;
    }
    if (queries.size() > 0 || nextHash != null || nextRange != null) {
      orScan(attr, false);
    } else {
      // just set the attribute
      if (this.nextAttribute == null) {
        this.nextAttribute = attr;
      } else {
        this.nextAttribute = this.nextAttribute.or(attr);
      }
    }
  }

  /**
   * Update the current range expression or add it to the previous query.
   */
  private boolean updateRange(FilterFragment
    fragment, VoidBiFunction<DynamoFilterSpec, DynamoFilterSpec> func) {
    if (queries.size() > 0) {
      GetOrQuery gq = queries.get(queries.size() - 1);
      Query query = gq.get != null ?
                    new Query(gq.get.getFilter(), gq.attribute()) :
                    gq.query;
      func.apply(query.getFilter(), fragment.getFilter());
      return true;
    }
    setRange(fragment, func);
    return false;
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
           (!fragment.isAttribute() && !fragment.isEquality()) ||
           (fragment.isHash() && !fragment.isEquals());
  }

  private void orScan(DynamoFilterSpec spec, boolean isKey) {
    if (scan == null) {
      scan = buildScan();
    }
    scan.or(spec);
  }

  private DynamoFilterSpec or(DynamoFilterSpec nextAttribute, DynamoFilterSpec spec) {
    if (nextAttribute == null) {
      return spec;
    } else if (spec == null) {
      return nextAttribute;
    }
    return nextAttribute.or(spec);
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
      DynamoFilterSpec spec = query.getFilter();
      if (query instanceof Query) {
        spec = and(spec, ((Query) query).attribute());
      }
      scan.or(spec);
    }
    queries.clear();

    // add the edge attributes.
    switch (AND_OR) {
      case UNSET:
      case AND:
        scan.and(nextHash);
        scan.and(nextRange);
        scan.and(nextAttribute);
        break;
      case OR:
        scan.or(nextHash);
        scan.or(nextRange);
        scan.or(nextAttribute);
    }
    nextHash = null;
    nextRange = null;
    nextAttribute = null;
    return scan;
  }

  private class Scan {
    private DynamoFilterSpec spec;

    public void and(DynamoFilterSpec spec) {
      set(spec, DynamoReadBuilder.this::and);
    }

    public void or(DynamoFilterSpec spec) {
      set(spec, DynamoReadBuilder.this::or);
    }

    public void and(FilterFragment fragment) {
      if (fragment != null) {
        and(fragment.getFilter());
      }
    }

    public void or(FilterFragment fragment) {
      if (fragment != null) {
        or(fragment.getFilter());
      }
    }

    private void set(DynamoFilterSpec spec, BiFunction<DynamoFilterSpec,
      DynamoFilterSpec, DynamoFilterSpec> func) {
      if (spec == null) {
        return;
      }
      this.spec = func.apply(this.spec, spec);
    }

    public void and(Scan scan) {
      this.spec = DynamoReadBuilder.this.and(spec, scan.spec);
    }

    public void or(Scan thatScan) {
      this.spec = DynamoReadBuilder.this.or(spec, scan.spec);
    }
  }

  private static class LeafQuerySpec {
    private final DynamoFilterSpec filter;

    public LeafQuerySpec(DynamoFilterSpec filter) {
      this.filter = filter;
    }

    public DynamoFilterSpec getFilter() {
      return filter;
    }
  }

  private static class Get extends LeafQuerySpec {

    protected DynamoFilterSpec attribute;

    public Get(DynamoFilterSpec key) {
      super(key);
    }

    public DynamoFilterSpec attribute() {
      return attribute;
    }
  }

  private static class Query extends LeafQuerySpec {

    protected DynamoFilterSpec attribute;

    public Query(DynamoFilterSpec filter, DynamoFilterSpec attr) {
      super(filter);
      this.attribute = attr;
    }

    public void setAttribute(DynamoFilterSpec attribute) {
      this.attribute = attribute;
    }

    public DynamoFilterSpec attribute() {
      return attribute;
    }
  }

  private static class GetOrQuery {
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
