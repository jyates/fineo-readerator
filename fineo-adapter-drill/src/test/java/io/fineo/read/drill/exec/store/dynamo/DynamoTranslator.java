package io.fineo.read.drill.exec.store.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.google.common.base.Joiner;
import io.fineo.lambda.dynamo.DynamoExpressionPlaceHolders;
import io.fineo.lambda.dynamo.Schema;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.format;

/**
 * Writer that mimics the writing of records to dynamo from the ingest pipeline, without the
 * overhead of creating an avro Record. It would be nice to unify the implementations, but for
 * now, this is more expedient #startup
 */
public class DynamoTranslator {

  public Item apply(Map<String, Object> itemToWrite) throws Exception {
    Item wrote = new Item();
    Map<String, Object> item = newHashMap(itemToWrite);
    wrote.with(Schema.PARTITION_KEY_NAME, item.remove(Schema.PARTITION_KEY_NAME));
    wrote.with(Schema.SORT_KEY_NAME, item.remove(Schema.SORT_KEY_NAME));
    // the remaining elements are stored by id
    String id = getId();
    wrote.with(Schema.ID_FIELD, newHashSet(id));
    for (Map.Entry<String, Object> column : item.entrySet()) {
      Map<String, Object> map = new HashMap<>();
      map.put(id, column.getValue());
      wrote.with(column.getKey(), map);
    }
    return wrote;
  }

  public UpdateItemSpec updateItem(Map<String, Object> toUpdate)
    throws UnsupportedEncodingException, NoSuchAlgorithmException {
    UpdateItemSpec spec = new UpdateItemSpec();
    Map<String, Object> item = newHashMap(toUpdate);
    spec.withPrimaryKey(Schema.PARTITION_KEY_NAME, item.remove(Schema.PARTITION_KEY_NAME),
      Schema.SORT_KEY_NAME, item.remove(Schema.SORT_KEY_NAME));
    Map<String, String> names = new HashMap<>();
    Map<String, Object> values = new HashMap<>();

    String id = getId();
    String idFieldName = DynamoExpressionPlaceHolders.asExpressionName(Schema.ID_FIELD);
    names.put(idFieldName, Schema.ID_FIELD);

    String idValueSet = DynamoExpressionPlaceHolders.asExpressionAttributeValue("idValue");
    values.put(idValueSet, newHashSet(id));

    String add = format("ADD %s %s", idFieldName, idValueSet);
    List<String> sets = new ArrayList<>();
    for (Map.Entry<String, Object> column : item.entrySet()) {
      String idFieldValueName = DynamoExpressionPlaceHolders.asExpressionName("idValue");
      names.put(idFieldValueName, id);
      String columnName = DynamoExpressionPlaceHolders.asExpressionName(column.getKey());
      names.put(columnName, column.getKey());
      String valueName = DynamoExpressionPlaceHolders.asExpressionAttributeValue("value");
      values.put(valueName, column.getValue());
      sets.add(format("%s.%s = %s", columnName, idFieldValueName, valueName));
    }
    String expr = add + " SET " + Joiner.on(", ").join(sets);
    spec.withUpdateExpression(expr);
    spec.withNameMap(names);
    spec.withValueMap(values);
    return spec;
  }

  private String getId() throws NoSuchAlgorithmException, UnsupportedEncodingException {
    String uuid = UUID.randomUUID().toString();
    return toHexString(MessageDigest.getInstance("MD5").digest(uuid.getBytes("UTF-8")));
  }

  private static String toHexString(byte[] bytes) {
    StringBuffer hexString = new StringBuffer();

    for (int i = 0; i < bytes.length; i++) {
      String hex = Integer.toHexString(0xFF & bytes[i]);
      hexString.append(hex);
    }

    return hexString.toString();
  }
}
