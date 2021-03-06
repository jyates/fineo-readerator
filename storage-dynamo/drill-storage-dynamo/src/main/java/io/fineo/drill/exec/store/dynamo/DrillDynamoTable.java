package io.fineo.drill.exec.store.dynamo;

import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.collect.ImmutableList;
import io.fineo.drill.exec.store.dynamo.key.DynamoKeyMapperSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoGroupScanSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoTableDefinition;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;

public class DrillDynamoTable extends DynamicDrillTable {

  private final TableDescription desc;
  private final DynamoKeyMapperSpec key;

  public DrillDynamoTable(DynamoStoragePlugin plugin, String tableName,
    DynamoKeyMapperSpec keyMapper) {
    super(plugin, tableName, new DynamoGroupScanSpec());
    try {
      this.desc = plugin.getModel().getTable(tableName).waitForActive();
    } catch (InterruptedException e) {
      throw new DrillRuntimeException(e);
    }
    this.key = keyMapper;

    DynamoGroupScanSpec spec = ((DynamoGroupScanSpec) this.getSelection());
    // figure out the pk map
    List<KeySchemaElement> keys = desc.getKeySchema();
    List<AttributeDefinition> attributes = desc.getAttributeDefinitions();
    Map<String, DynamoTableDefinition.PrimaryKey> map = new HashMap<>();
    for (KeySchemaElement key : keys) {
      DynamoTableDefinition.PrimaryKey pk = new DynamoTableDefinition.PrimaryKey(key
        .getAttributeName(), null, KeyType.valueOf(key.getKeyType()) == KeyType.HASH);
      map.put(key.getAttributeName(), pk);
    }
    for (AttributeDefinition elem : attributes) {
      map.get(elem.getAttributeName()).setType(elem.getAttributeType());
    }
    List<DynamoTableDefinition.PrimaryKey> pks = newArrayList(map.values());
    DynamoTableDefinition def = new DynamoTableDefinition(tableName, pks, keyMapper);
    spec.setTable(def);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataType type = super.getRowType(typeFactory);
    // force add the star field, this is a dynamic row
    type.getFieldCount();
    // add the sort/partition keys that the mapper should produce
    if (key != null) {
      for (String field : key.getKeyNames()) {
        type.getField(field, true, false);
      }
    }
    // since we don't support pushing the key combination into the filter building, we need to
    // support the key schema elements in our key schema so hash/sort can be used in the filter.
    List<KeySchemaElement> keys = desc.getKeySchema();
    for (KeySchemaElement elem : keys) {
      type.getField(elem.getAttributeName(), true, false);

    }

    return type;
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.of(desc.getItemCount(), ImmutableList.of(), ImmutableList.of());
  }

  public static boolean checkAccessible(Table table) {
    try {
      table.describe();
    } catch (AmazonDynamoDBException e) {
      // user not allowed to see this table
      if (e.getErrorCode().equals("AccessDeniedException")) {
        return false;
      }
      // lookup failed for some other reason
      throw e;
    }

    // describe was fine. assume if user has describe, they also have read.
    return true;
  }
}
