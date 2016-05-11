package io.fineo.read.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.dynamo.avro.DynamoAvroRecordEncoder;
import io.fineo.lambda.dynamo.avro.Schema;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.store.SchemaStore;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Map and unmap customer query fields to/from dynamo.
 */
public class DynamoFieldMapper {

  private static final List<String> BASE_FIELDS =
    newArrayList(AvroSchemaEncoder.ORG_ID_KEY, AvroSchemaEncoder.ORG_METRIC_TYPE_KEY,
      AvroSchemaEncoder.TIMESTAMP_KEY);
  private final SchemaStore store;
  private final String orgId;
  private final String customerMetricName;
  private final Metric metric;

  public DynamoFieldMapper(SchemaStore store, String orgId, String customerMetric) {
    this.store = store;
    this.orgId = orgId;
    this.customerMetricName = customerMetric;

    Metadata org = store.getOrgMetadata(orgId);
    Map<String, String> metricTypes = AvroSchemaManager.getAliasRemap(org);
    String metricId = metricTypes.get(customerMetricName);
    Metric metric = store.getMetricMetadata(orgId, metricId);
    this.metric = metric;
  }

  /**
   * Remap the given field names to the names that we should actually use when querying dynamo
   *
   * @param fields names of the fields
   * @return map from the original to the names by which that field could be referred.
   */
  public Multimap<String, String> getFieldRemap(List<String> fields) {
    // get the metadata for the current org
    Map<String, String> aliases = AvroSchemaManager.getAliasRemap(metric);
    // remap the field names to the ones we need
    Multimap<String, String> fieldRemap = ArrayListMultimap.create();
    fieldRemap.put(AvroSchemaEncoder.TIMESTAMP_KEY, Schema.SORT_KEY_NAME);
    for (String field : fields) {
      String cname = aliases.get(field);
      List<String> aliasNames =
        metric.getMetadata().getCanonicalNamesToAliases().get(cname);
      for (String alias : aliasNames) {
        fieldRemap.put(field, DynamoAvroRecordEncoder.getKnownFieldName(alias));
      }
    }
    return fieldRemap;
  }

  public Map<String, AttributeValue> unmap(Map<String, AttributeValue> results,
    Multimap<String, String> fieldMapping) {
    Map<String, AttributeValue> values = new HashMap<>(fieldMapping.keySet().size());

    // manage the orgId/metric type extraction, if its set
    AttributeValue key = results.get(Schema.PARTITION_KEY_NAME);
    if (key != null) {
      String cname = metric.getMetadata().getCanonicalName();

      assert key.getS().startsWith(orgId);
      assert key.getS().endsWith(cname);
      assert key.getS().length() == cname.length() + orgId.length();

      values.put(AvroSchemaEncoder.ORG_ID_KEY, new AttributeValue(orgId));
      values.put(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, new AttributeValue(customerMetricName));
    }


    for (Map.Entry<String, Collection<String>> mapping : fieldMapping.asMap().entrySet()) {
      AttributeValue value = null;
      String canonicalName = mapping.getKey();
      for (String alias : mapping.getValue()) {
        AttributeValue resultValue = results.get(alias);
        //take the first applicable value... we should never have more than one.
        if (notNull(resultValue)) {
          value = resultValue;
          break;
        }
      }
      if (value != null) {
        values.put(canonicalName, value);
      }
    }
    return values;
  }

  private boolean notNull(AttributeValue value) {
    return value != null && value.isNULL() == null;
  }
}
