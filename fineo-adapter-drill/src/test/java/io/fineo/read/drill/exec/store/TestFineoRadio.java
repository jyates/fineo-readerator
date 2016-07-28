package io.fineo.read.drill.exec.store;

import io.fineo.internal.customer.Metric;
import io.fineo.read.drill.BaseFineoTest;
import io.fineo.read.drill.FineoTestUtil;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.store.SchemaBuilder;
import io.fineo.schema.store.SchemaStore;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Maps.newHashMap;
import static io.fineo.read.drill.FineoTestUtil.withNext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Or more specifically, the _fm (Fineo Map) field and reading/using it. We should be able to
 * read 'unknown' fields as a Map of fields. The main challenge comes in combing the vectors in
 * the correct way that we can merge the schemas for the maps. This mostly goes to supporting
 * UNION and ExternalSort/TopN.
 */
public class TestFineoRadio extends BaseFineoTest {

  /**
   * We have to use this because we cannot override Drill session/system options.
   */
  @BeforeClass
  public static void enableRadio(){
    FineoCommon.RADIO_ENABLED_TEST_OVERRIDE = true;
  }

  @AfterClass
  public static void disableRadio(){
    FineoCommon.RADIO_ENABLED_TEST_OVERRIDE = false;
  }

  @Test
  public void testUnknownFieldType() throws Exception {
    TestState state = register();

    Map<String, Object> values = new HashMap<>();
    values.put(fieldname, true);
    String uk = "uk_" + UUID.randomUUID();
    values.put(uk, 1L);
    String uk2 = "uk2_" + UUID.randomUUID();
    values.put(uk2, "hello field 2");

    File tmp = folder.newFolder("drill");
    bootstrap(state.write(tmp, 1, values));

    Map<String, Object> radio = new HashMap<>();
    radio.put(uk, values.remove(uk));
    radio.put(uk2, values.remove(uk2));
    values.put(FineoCommon.MAP_FIELD, radio);
    QueryRunnable runnable = new QueryRunnable(withNext(values));
    runAndVerify(runnable);
  }

  @Test
  public void testFilterOnUnknownField() throws Exception {
    TestState state = register();

    Map<String, Object> values = new HashMap<>();
    values.put(fieldname, true);
    String uk = "uk_" + UUID.randomUUID();
    values.put(uk, 1L);

    File tmp = folder.newFolder("drill");
    bootstrap(state.write(tmp, 1, values));

    // definitely doesn't match
    String field = FineoCommon.MAP_FIELD + "['" + uk + "']";
    verifySelectStar(of(equals(field, "2")), result -> {
      assertFalse(result.next());
    });

    // matching case
    verifySelectStar(of(equals(field, Long.toString(1L))), result -> {
      assertTrue(result.next());
      Map radio = (Map) result.getObject(FineoCommon.MAP_FIELD);
      assertEquals(values.get(uk), radio.get(uk));
    });
  }

  /**
   * We can have a field named _fm, but its stored as an unknown field in the _fm map.
   *
   * @throws Exception on failure
   */
  @Test
  public void testUnknownFieldWithRadioName() throws Exception {
    TestState state = register();

    Map<String, Object> values = new HashMap<>();
    values.put(fieldname, true);
    values.put(FineoCommon.MAP_FIELD, 1L);

    File tmp = folder.newFolder("drill");
    bootstrap(state.write(tmp, 1, values));

    QueryRunnable runnable = new QueryRunnable(result -> {
      assertTrue(result.next());
      Map radio = (Map) result.getObject(FineoCommon.MAP_FIELD);
      assertEquals("Radio doesn't match!", values.get(FineoCommon.MAP_FIELD),
        radio.get(FineoCommon.MAP_FIELD));
    });
    runAndVerify(runnable);
  }

  /**
   * Ensure that we handle switching back and forth between alias and user-visible name for a
   * field based on the stored values. It can be tricky because we don't want to overwrite the
   * field value from the earlier with the later and we still want to handle nulls correctly.
   */
  @Test
  public void testKnownAliasKnownField() throws Exception {
    TestState state = register();
    // create a new alias name for the field
    Metric metric = state.getMetric();
    SchemaStore store = state.getStore();
    SchemaBuilder builder = SchemaBuilder.create();
    SchemaBuilder.OrganizationBuilder ob = builder.updateOrg(store.getOrgMetadata(org));
    Map<String, String> aliasToCname = AvroSchemaManager.getAliasRemap(metric);
    String cname = aliasToCname.get(fieldname);
    String storeFieldName = "other-field-name";
    SchemaBuilder.Organization org =
      ob.updateSchema(metric).updateField(cname).withAlias(storeFieldName).asField().build()
        .build();
    store.updateOrgMetric(org, metric);

    // write a file with the new field name
    File tmp = folder.newFolder("drill");
    Map<String, Object> v1 = new HashMap<>();
    v1.put(fieldname, true);
    Map<String, Object> v2 = new HashMap<>();
    v2.put(storeFieldName, false);
    Map<String, Object> v3 = new HashMap<>();
    v3.put(fieldname, true);
    bootstrap(state.write(tmp, 1, v1), state.write(tmp, 2, v2), state.write(tmp, 3, v3));

    // we should read this as the client visible name
    Boolean value = (Boolean) v2.remove(storeFieldName);
    v2.put(fieldname, value);

    addRadio(v1);
    addRadio(v2);
    addRadio(v3);
    QueryRunnable runnable = new QueryRunnable(withNext(v1, v2, v3));
    runAndVerify(runnable);

    // add a new row with a null value
    Map<String, Object> v4 = new HashMap<>();
    state.write(tmp, 4, v4);
    v4.put(fieldname, null);
    addRadio(v4);
    runnable = new QueryRunnable(withNext(v1, v2, v3, v4));
    runAndVerify(runnable);
  }

  @Test
  // We don't necessarily need a union, but we do need to be able to merge schemas across maps
  @Ignore("Unknown fields not supported until ExternalSort can support merging schema's with "
          + "complex types")
  public void testChangingUnknownFieldsAcrossRows() throws Exception {
    TestState state = register();

    Map<String, Object> values = new HashMap<>();
    values.put(fieldname, true);
    String uk = "uk";
    values.put(uk, 1L);
    String uk2 = "uk2";
    values.put(uk2, "hello field");

    File tmp = folder.newFolder("drill");
    FsSourceTable f1 = state.write(tmp, 1, values);
    // build the expected values
    values.remove(uk);
    values.remove(uk2);
    addRadio(values, FineoTestUtil.p(uk, 1L), FineoTestUtil.p(uk2, "hello field"));

    Map<String, Object> values2 = new HashMap<>();
    values2.put(fieldname, false);
    String uk3 = "uk3";
    values2.put(uk3, true);

    bootstrap(f1, state.write(tmp, 2, values2));

    values2.remove(uk3);
    addRadio(values2, FineoTestUtil.p(uk3, true));

    verifySelectStar(withNext(values, values2));
  }

  @Test
  @Ignore
  public void testUnionKnownAndUnknownFields() throws Exception {
    TestState state = register();

    File tmp = folder.newFolder("drill");
    Map<String, Object> values = new HashMap<>();
    values.put("uk", 1);
    FsSourceTable json = state.write(tmp, org, metrictype, 1, values);
    Map<String, Object> values2 = new HashMap<>();
    values2.put(fieldname, true);
    FsSourceTable parquet = writeParquet(state, tmp, org, metrictype, 2, values2).getKey();

    // ensure that the fineo-test plugin is enabled
    bootstrap(json, parquet);

    Map<String, Object> result = newHashMap(values2);
    Map<String, Object> radio = new HashMap<>();
    radio.put("uk", 1);
    result.put(FineoCommon.MAP_FIELD, radio);
    verifySelectStar(FineoTestUtil.withNext(result));
  }

  private void addRadio(Map<String, Object> map, Pair<String, Object>... values) {
    Map<String, Object> radio = new HashMap<>();
    for (Pair<String, Object> p : values) {
      radio.put(p.getKey(), p.getValue());
    }
    map.put(FineoCommon.MAP_FIELD, radio);
  }
}
