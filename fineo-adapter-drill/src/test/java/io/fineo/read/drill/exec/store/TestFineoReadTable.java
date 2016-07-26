package io.fineo.read.drill.exec.store;

import io.fineo.internal.customer.Metric;
import io.fineo.read.drill.BaseFineoTest;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaBuilder;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreManager;
import org.apache.avro.Schema;
import org.junit.Test;
import org.schemarepo.ValidatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestFineoReadTable extends BaseFineoTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestFineoReadTable.class);

  /**
   * Store a single row as the 'user visible' name of the field and check that we can read it
   * back as expected
   *
   * @throws Exception on failure
   */
  @Test
  public void testStoringUserVisibleName() throws Exception {
    TestState state = register();

    File tmp = folder.newFolder("drill");
    Map<String, Object> values = new HashMap<>();
    values.put(fieldname, false);
    List<Map<String, Object>> rows = newArrayList(values);
    FsSourceTable out = state.write(tmp, org, metrictype, 1, rows);

    // ensure that the fineo-test plugin is enabled
    bootstrap(out);

    verifySelectStar(result -> {
      assertNext(result, values);
    });
  }

  @Test
  public void testStoringNonUserVisibleFieldName() throws Exception {
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
    Map<String, Object> values = new HashMap<>();
    values.put(storeFieldName, false);
    FsSourceTable out = state.write(tmp, 1, values);

    bootstrap(out);

    // we should read this as the client visible name
    Boolean value = (Boolean) values.remove(storeFieldName);
    values.put(fieldname, value);

    verifySelectStar(result -> {
      assertNext(result, values);
    });
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

    verifySelectStar(withNext(v1, v2, v3));

    // add a new row with a null value
    Map<String, Object> v4 = new HashMap<>();
    bootstrap(state.write(tmp, 4, v4));
    v4.put(fieldname, null);
    verifySelectStar(withNext(v1, v2, v3, v4));
  }

  @Test
  public void testReadTwoSources() throws Exception {
    Map<String, Object> values = new HashMap<>();
    values.put(fieldname, false);
    Map<String, Object> values2 = newHashMap(values);
    values.put(fieldname, true);

    writeAndReadToIndependentFiles(values, values2);
  }

  /**
   * We don't need to go beyond three sources because this covers 'n' cases of unions over unions.
   */
  @Test
  public void testReadThreeSources() throws Exception {
    Map<String, Object> values = new HashMap<>();
    values.put(fieldname, false);
    Map<String, Object> values2 = newHashMap(values);
    values2.put(fieldname, true);
    Map<String, Object> values3 = newHashMap(values);
    values3.put(fieldname, false);

    writeAndReadToIndependentFiles(values, values2, values3);
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

    verifySelectStar(result -> {
      assertTrue(result.next());
      Map radio = (Map) result.getObject(FineoCommon.MAP_FIELD);
      assertEquals("Mismatch for radio field: " + uk, values.get(uk), radio.get(uk));
      assertEquals(values.get(uk2), radio.get(uk2).toString());
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

    verifySelectStar(result -> {
      assertTrue(result.next());
      Map radio = (Map) result.getObject(FineoCommon.MAP_FIELD);
      assertEquals("Radio doesn't match!", values.get(FineoCommon.MAP_FIELD),
        radio.get(FineoCommon.MAP_FIELD));
    });
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
      System.out.println(result);
    });

    // matching case
    verifySelectStar(of(equals(field, Long.toString(1L))), result -> {
      assertTrue(result.next());
      Map radio = (Map) result.getObject(FineoCommon.MAP_FIELD);
      assertEquals(values.get(uk), radio.get(uk));
    });
  }

  @Test
  public void testSupportedFieldTypes() throws Exception {
    Map<String, Object> values = bootstrapFileWithFields(
      f(true, Schema.Type.BOOLEAN),
      f(new byte[]{1}, Schema.Type.BYTES),
      f(2.0, Schema.Type.DOUBLE),
      f(3.0f, Schema.Type.FLOAT),
      f(4, Schema.Type.INT),
      f(5L, Schema.Type.LONG),
      f("6string", Schema.Type.STRING));

//    verify("SELECT *, CAST(f4 as FLOAT) FROM fineo."+org+"."+metrictype, result ->{});
    verifySelectStar(result -> assertNext(result, values));
  }

  @Test
  public void testSimpleCast() throws Exception {
    Map<String, Object> values = bootstrapFileWithFields(
      f(4, Schema.Type.FLOAT));
    values.put("f0", 4.0f);
    verifySelectStar(withNext(values));
  }

  @Test
  public void testCastWithMultipleFieldAliases() throws Exception {
    DynamoDBRepository repository =
      new DynamoDBRepository(ValidatorFactory.EMPTY, tables.getAsyncClient(),
        getCreateTable(tables.getTestTableName()));
    SchemaStore store = new SchemaStore(repository);
    StoreManager manager = new StoreManager(store);
    StoreManager.MetricBuilder builder = manager.newOrg(org)
                                                .newMetric().setDisplayName(metrictype);
    builder.newField().withName("f0").withType(Schema.Type.FLOAT.getName()).withAliases(of("af0"))
           .build().build().commit();

    Map<String, Object> values = new HashMap<>();
    values.put("af0", 4);

    File tmp = folder.newFolder("drill");
    bootstrap(writeJson(store, tmp, org, metrictype, 1, of(values)));

    values.remove("af0");
    values.put("f0", 4.0f);
    verifySelectStar(withNext(values));
  }


  /**
   * Write bytes json row and read it back in as bytes. This is an issue because bytes are
   * mis-mapped from json as varchar
   * <p>
   * If you update {@link io.fineo.read.drill.udf.conv.Base64Decoder}, then you need to run
   * <tt>mvn clean package</tt> again to ensure the latest source gets copied to the output
   * directory so Drill can compile the generated function from the source code.
   * </p>
   */
  @Test
  public void testBytesTypeRemap() throws Exception {
    Map<String, Object> values = bootstrapFileWithFields(f(new byte[]{1}, Schema.Type.BYTES));
    verifySelectStar(withNext(values));
  }

  @Test
  public void testFilterOnBoolean() throws Exception {
    TestState state = register();

    Map<String, Object> contents = new HashMap<>();
    contents.put(fieldname, true);

    // write two different files that occur on different days
    File tmp = folder.newFolder("drill");
    List<FsSourceTable> files = new ArrayList<>();
    Instant now = Instant.now();
    files.add(state.write(tmp, now.toEpochMilli(), contents));

    // ensure that the fineo-test plugin is enabled
    bootstrap(files.toArray(new FsSourceTable[0]));

    verifySelectStar(of(fieldname + " IS TRUE"), withNext(contents));
  }

  /**
   * Corner case where we need to ensure that we create a vector for the field that is missing
   * in the underlying file so we get the correct matching behavior in upstream filters.
   */
  @Test
  public void testFilterBooleanWhereAllFieldNotPresentInAllRecords() throws Exception {
    TestState state = register();

    Map<String, Object> contents = new HashMap<>();
    contents.put(fieldname, true);

    // write two different files that occur on different days
    File tmp = folder.newFolder("drill");
    List<FsSourceTable> files = new ArrayList<>();
    Instant now = Instant.now();
    files.add(state.write(tmp, now.toEpochMilli(), contents));

    // older record without the value
    Instant longAgo = now.minus(5, ChronoUnit.DAYS).plus(1, ChronoUnit.MILLIS);
    files.add(state.write(tmp, longAgo.toEpochMilli(), newHashMap()));

    // ensure that the fineo-test plugin is enabled
    bootstrap(files.toArray(new FsSourceTable[0]));

    verifySelectStar(of(fieldname + " IS TRUE"), withNext(contents));
  }

  @Test
  public void testFilterOnTimeRange() throws Exception {
    TestState state = register();

    Map<String, Object> contents = new HashMap<>();
    contents.put(fieldname, true);

    // write two different files that occur on different days
    File tmp = folder.newFolder("drill");
    List<FsSourceTable> files = new ArrayList<>();
    Instant now = Instant.now();
    files.add(state.write(tmp, now.toEpochMilli(), contents));
    // ensure that the fineo-test plugin is enabled
    bootstrap(files.toArray(new FsSourceTable[0]));

    verifySelectStar(of("`timestamp` > " + now.minus(5, ChronoUnit.DAYS).toEpochMilli()),
      withNext(contents));
  }

  @Test
  public void testFilterOnTimeRangeAcrossMultipleFiles() throws Exception {
    TestState state = register();

    Map<String, Object> contents = new HashMap<>();
    contents.put(fieldname, true);

    // write two different files that occur on different days
    File tmp = folder.newFolder("drill");
    List<FsSourceTable> files = new ArrayList<>();
    Instant now = Instant.now();
    files.add(state.write(tmp, now.toEpochMilli(), contents));

    Map<String, Object> contents2 = new HashMap<>();
    contents2.put(fieldname, false);
    Instant longAgo = now.minus(5, ChronoUnit.DAYS);
    files.add(state.write(tmp, longAgo.toEpochMilli(), contents2));

    // ensure that the fineo-test plugin is enabled
    bootstrap(files.toArray(new FsSourceTable[0]));

    verifySelectStar(of("`timestamp` > " + longAgo.toEpochMilli()), withNext(contents));
  }

  private Map<String, Object> bootstrapFileWithFields(FieldInstance<?>... fields)
    throws IOException, OldSchemaException {
    return bootstrapFileWithFields(1, fields);
  }

  private Map<String, Object> bootstrapFileWithFields(long timestamp, FieldInstance<?>... fields)
    throws IOException, OldSchemaException {
    // setup the schema repository
    DynamoDBRepository repository =
      new DynamoDBRepository(ValidatorFactory.EMPTY, tables.getAsyncClient(),
        getCreateTable(tables.getTestTableName()));
    SchemaStore store = new SchemaStore(repository);
    StoreManager manager = new StoreManager(store);
    StoreManager.MetricBuilder builder = manager.newOrg(org)
                                                .newMetric().setDisplayName(metrictype);
    Map<String, Object> values = new HashMap<>();
    for (int i = 0; i < fields.length; i++) {
      String name = "f" + i;
      FieldInstance<?> field = fields[i];
      builder.newField().withName(name).withType(field.type.getName()).build();
      values.put(name, field.inst);
    }
    builder.build().commit();

    File tmp = folder.newFolder("drill");
    bootstrap(writeJson(store, tmp, org, metrictype, timestamp, of(values)));

    return values;
  }

  private class FieldInstance<T> {
    private final T inst;
    private final Schema.Type type;

    public FieldInstance(T inst, Schema.Type type) {
      this.inst = inst;
      this.type = type;
    }
  }

  private <T> FieldInstance<T> f(T inst, Schema.Type type) {
    return new FieldInstance<>(inst, type);
  }

  private void writeAndReadToIndependentFiles(Map<String, Object>... fileContents)
    throws Exception {
    TestState state = register();

    File tmp = folder.newFolder("drill");
    List<FsSourceTable> files = new ArrayList<>();
    int i = 0;
    for (Map<String, Object> contents : fileContents) {
      files.add(state.write(tmp, i++, contents));
    }

    // ensure that the fineo-test plugin is enabled
    bootstrap(files.toArray(new FsSourceTable[0]));

    verifySelectStar(result -> {
      int j = 0;
      for (Map<String, Object> content : fileContents) {
        assertNext(j++, result, content);
      }
    });
  }
}
