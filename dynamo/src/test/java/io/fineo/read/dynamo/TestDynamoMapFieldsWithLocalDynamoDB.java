package io.fineo.read.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.base.Joiner;
import com.google.common.collect.Multimap;
import io.fineo.aws.AwsDependentTests;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.dynamo.DynamoTableCreator;
import io.fineo.lambda.dynamo.DynamoTableTimeManager;
import io.fineo.lambda.dynamo.Range;
import io.fineo.lambda.dynamo.ResultOrException;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.dynamo.avro.Schema;
import io.fineo.lambda.dynamo.iter.PageManager;
import io.fineo.lambda.dynamo.iter.PagingIterator;
import io.fineo.lambda.dynamo.iter.ScanPager;
import io.fineo.lambda.dynamo.rule.BaseDynamoTableTest;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.avro.SchemaTestUtils;
import io.fineo.schema.store.SchemaBuilder;
import io.fineo.schema.store.SchemaStore;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test mapping fields, but with real results from a local dynamo instance
 */
@Category(AwsDependentTests.class)
public class TestDynamoMapFieldsWithLocalDynamoDB extends BaseDynamoTableTest {

  public static final String ORG = "org1";
  public static final String METRIC = "metric1";
  public static final String FIELD = "field1";
  public static final String OTHER_ALIAS =
    "other_name";
  private static final Logger LOG = LoggerFactory
    .getLogger(TestDynamoMapFieldsWithLocalDynamoDB.class);

  public static void addAliasForField(SchemaStore store, SchemaBuilder.Organization organization)
    throws IOException, OldSchemaException {
    SchemaBuilder builder = SchemaBuilder.create();
    SchemaBuilder.OrganizationBuilder orgBuilder = builder.updateOrg(organization.getMetadata());
    Metric metricSchema = organization.getSchemas().values().iterator().next();
    Map<String, String> remap = AvroSchemaManager.getAliasRemap(metricSchema);
    String cname = remap.get(FIELD);
    SchemaBuilder.MetricBuilder metricBuilder = orgBuilder.updateSchema(metricSchema);
    organization =
      metricBuilder.updateField(cname).withAlias(OTHER_ALIAS).asField().build().build();
    store.updateOrgMetric(organization, metricSchema);
  }

  @Test
  public void testUnmappingWithReadAll() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    SchemaBuilder.Organization organization = SchemaTestUtils.addNewOrg(store, ORG, METRIC, FIELD);
    addAliasForField(store, organization);

    DynamoFieldMapper mapper = new DynamoFieldMapper(store, ORG, METRIC);
    Multimap<String, String> mapping = mapper.getFieldRemap(newArrayList(FIELD));

    // write a record into the table using dynamo
    LocalDynamo dynamo = new LocalDynamo();
    long writeTime = dynamo.write(store);
    DynamoTableTimeManager manager = dynamo.getManager();

    // read the row back from dynamo
    List<Pair<String, Range<Instant>>> tables =
      manager.getCoveringTableNames(
        new Range<>(Instant.ofEpochMilli(writeTime - 1), Instant.ofEpochMilli(writeTime + 1)));
    List<ScanPager> scanners = new ArrayList<>(tables.size());
    tables.stream().map(pair -> pair.getKey())
          .forEach(table -> {
            ScanRequest request = new ScanRequest();
            request.setTableName(table);
            request.setConsistentRead(true);
            scanners.add(
              new ScanPager(this.tables.getAsyncClient(), request, Schema.PARTITION_KEY_NAME,
                null));
          });
    Iterator<ResultOrException<Map<String, AttributeValue>>> rows =
      new PagingIterator<>(1, new PageManager(scanners));
    Map<String, AttributeValue> result = assertHasNextRow(rows);
    Map<String, AttributeValue> expected = new HashMap<>();
    expected.put(FIELD, new AttributeValue().withBOOL(true));
    expected.put(AvroSchemaEncoder.ORG_ID_KEY, new AttributeValue(ORG));
    expected.put(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, new AttributeValue(METRIC));
    expected.put(AvroSchemaEncoder.TIMESTAMP_KEY,
      new AttributeValue().withN(Long.toString(writeTime)));
    assertEquals(expected, mapper.unmap(result, mapping));
    assertFalse(rows.hasNext());
  }

  private Map<String, AttributeValue> assertHasNextRow(
    Iterator<ResultOrException<Map<String, AttributeValue>>> rows) {
    assertTrue("Didn't read any data!", rows.hasNext());
    ResultOrException<Map<String, AttributeValue>> row = rows.next();
    assertFalse("Got an exception! " + row.getException(), row.hasException());
    return row.getResult();
  }

  @Test
  public void testUnmappingWithReadExplicitFields() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    SchemaBuilder.Organization organization = SchemaTestUtils.addNewOrg(store, ORG, METRIC, FIELD);
    addAliasForField(store, organization);

    DynamoFieldMapper mapper = new DynamoFieldMapper(store, ORG, METRIC);
    Multimap<String, String> mapping = mapper.getFieldRemap(newArrayList(FIELD));

    // write a record into the table using dynamo
    LocalDynamo dynamo = new LocalDynamo();
    long writeTime = dynamo.write(store);
    DynamoTableTimeManager manager = dynamo.getManager();

    // read the row back from dynamo
    // read the row back from dynamo
    List<Pair<String, Range<Instant>>> tables =
      manager.getCoveringTableNames(
        new Range<>(Instant.ofEpochMilli(writeTime - 1), Instant.ofEpochMilli(writeTime + 1)));
    List<ScanPager> scanners = new ArrayList<>(tables.size());
    tables.stream().map(pair -> pair.getKey())
          .forEach(table -> {
            ScanRequest request = new ScanRequest();
            request.setTableName(table);
            request.setConsistentRead(true);
            request.setProjectionExpression(Joiner.on(", ").join(mapping.get(FIELD)));
            scanners.add(
              new ScanPager(this.tables.getAsyncClient(), request, Schema.PARTITION_KEY_NAME,
                null));
          });
    Iterator<ResultOrException<Map<String, AttributeValue>>> rows =
      new PagingIterator<>(1, new PageManager(scanners));
    Map<String, AttributeValue> result = assertHasNextRow(rows);
    Map<String, AttributeValue> expected = new HashMap<>();
    expected.put(FIELD, new AttributeValue().withBOOL(true));
    assertEquals(expected, mapper.unmap(result, mapping));
    assertFalse(rows.hasNext());
  }

  private class LocalDynamo {

    private final AmazonDynamoDBAsyncClient client;
    private final DynamoTableTimeManager manager;
    private final AvroToDynamoWriter writer;

    public LocalDynamo() {
      this.client = tables.getAsyncClient();
      this.manager = new DynamoTableTimeManager(client, "test-table");
      this.writer = new AvroToDynamoWriter(client, 3,
        new DynamoTableCreator(manager, new DynamoDB(client), 1, 1));
    }

    public long write(SchemaStore store) throws IOException, InterruptedException {
      long writeTime = System.currentTimeMillis();
      LOG.info("using write time: " + writeTime);

      Map<String, Object> json = new HashMap<>();
      json.put(FIELD, true);
      writer.write(IngestMockTranslateUtil.createAvroRecord(store, ORG, METRIC, writeTime, json));
      assertFalse("Got some write failures!", writer.flush().any());
      return writeTime;
    }

    public DynamoTableTimeManager getManager() {
      return manager;
    }
  }
}
