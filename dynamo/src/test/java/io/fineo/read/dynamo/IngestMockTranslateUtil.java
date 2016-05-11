package io.fineo.read.dynamo;

import io.fineo.lambda.handle.raw.RawJsonToRecordHandler;
import io.fineo.lambda.kinesis.IKinesisProducer;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

/**
 *
 */
public class IngestMockTranslateUtil {

  public static GenericRecord createAvroRecord(SchemaStore store, String orgId, String metricId,
    long writeTime, Map<String, Object> fields) throws IOException, InterruptedException {
    IKinesisProducer kinesis = Mockito.mock(IKinesisProducer.class);
    BlockingQueue<GenericRecord> result = new LinkedBlockingDeque<>();
    Mockito.doAnswer(invoke -> {
      result.put((GenericRecord) invoke.getArguments()[2]);
      return null;
    }).when(kinesis).add(anyString(), anyString(), any());
    RawJsonToRecordHandler handler = new RawJsonToRecordHandler("stream", store, kinesis);
    fields.put(AvroSchemaEncoder.ORG_ID_KEY, orgId);
    fields.put(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, metricId);
    fields.put(AvroSchemaEncoder.TIMESTAMP_KEY, writeTime);
    handler.handle(fields);
    return result.take();
  }
}
