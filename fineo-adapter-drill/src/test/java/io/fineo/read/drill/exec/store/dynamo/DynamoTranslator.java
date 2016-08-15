package io.fineo.read.drill.exec.store.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.collect.ImmutableList;
import io.fineo.lambda.dynamo.Schema;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Maps.newHashMap;

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
    wrote.with(Schema.ID_FIELD, of(id));
    for (Map.Entry<String, Object> column : item.entrySet()) {
      Map<String, Object> map = new HashMap<>();
      map.put(id, column.getValue());
      wrote.with(column.getKey(), map);
    }
    return wrote;
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
