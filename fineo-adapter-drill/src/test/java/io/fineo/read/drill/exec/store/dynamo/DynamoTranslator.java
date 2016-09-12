package io.fineo.read.drill.exec.store.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.google.common.base.Joiner;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.dynamo.DynamoExpressionPlaceHolders;
import io.fineo.lambda.dynamo.DynamoTableCreator;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.schema.MapRecord;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.AvroSchemaProperties;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.format;
import static org.junit.Assert.assertFalse;

/**
 * Write the event to the table through the standard ingest {@link AvroToDynamoWriter}
 */
public class DynamoTranslator {

  private final AmazonDynamoDBAsyncClient client;

  public DynamoTranslator(AmazonDynamoDBAsyncClient asyncClient) {
    this.client = asyncClient;
  }

  public void write(DynamoTableCreator creator, SchemaStore store, Map<String, Object> item)
    throws
    SchemaNotFoundException {
    StoreClerk clerk = new StoreClerk(store,
      (String) item.get(AvroSchemaProperties.ORG_ID_KEY));
    GenericData.Record record =
      clerk.getEncoderFactory().getEncoder(new MapRecord(item)).encode();
    AvroToDynamoWriter writer = new AvroToDynamoWriter(client, 3, creator);
    writer.write(record);
    MultiWriteFailures<GenericRecord, ?> failures = writer.flush();
    assertFalse("Failed to writeToDynamo records to dynamo! " + failures.getActions(),
      failures.any());
  }
}
