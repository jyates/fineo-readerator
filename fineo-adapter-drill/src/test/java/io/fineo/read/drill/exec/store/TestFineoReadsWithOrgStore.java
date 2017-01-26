package io.fineo.read.drill.exec.store;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import io.fineo.drill.ClusterTest;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.read.drill.BaseFineoTest;
import io.fineo.read.drill.FineoTestUtil;
import io.fineo.schema.store.StoreManager;
import io.fineo.user.info.DynamoTenantInfoStore;
import io.fineo.user.info.TenantInfo;
import io.fineo.user.info.TenantInfoStore;
import io.fineo.user.info.UserInfo;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.read.drill.FineoTestUtil.get1980;
import static io.fineo.read.drill.FineoTestUtil.p;
import static io.fineo.schema.store.AvroSchemaProperties.ORG_ID_KEY;
import static io.fineo.schema.store.AvroSchemaProperties.ORG_METRIC_TYPE_KEY;
import static io.fineo.schema.store.AvroSchemaProperties.TIMESTAMP_KEY;

@Category(ClusterTest.class)
public class TestFineoReadsWithOrgStore extends BaseFineoTest {

  @Rule
  public TestName name = new TestName();

  @Test
  public void testSingleOrgLoading() throws Exception {
    TestState state = register(p(fieldname, StoreManager.Type.INTEGER));

    List<WriteResult> wrote = newArrayList(writeOrgRow(state, org));

    // create a tenant store with with usual org
    TenantInfoStore tenantInfoStore = createTenantInfoStore();
    tenantInfoStore.createTenantInfo(org);

    readAndVerify(wrote);
  }

  @Test
  public void testMultipleOrgs() throws Exception {
    String org2 = "someotherorg";
    Pair<String, StoreManager.Type> field = p(fieldname, StoreManager.Type.INTEGER);
    TestState state = register(field);
    registerSchema(state.getStore(), true, org2, field);

    List<WriteResult> wrote = new ArrayList<>();
    wrote.add(writeOrgRow(state, org));
    wrote.add(writeOrgRow(state, org2));

    TenantInfoStore tenantInfoStore = createTenantInfoStore();
    tenantInfoStore.createTenantInfo(org);
    tenantInfoStore.createTenantInfo(org2);

    readAndVerify(wrote);
  }

  private void readAndVerify(List<WriteResult> wrote) throws Exception {
    basicBootstrap(newBootstrap(drill).builder())
      .withDynamoTenantTable(getTenantTableName())
      // dynamo
      .withDynamoKeyMapper()
      .withDynamoTable(wrote.get(0).getTable())
      .bootstrap();

    List<Map<String, Object>> list =
      wrote.stream()
           .map(w -> w.getWrote())
           .map(w -> {
             Map<String, Object> dynamoRow = new HashMap<>();
             dynamoRow.put(ORG_ID_KEY, w.get(ORG_ID_KEY));
             dynamoRow.put(ORG_METRIC_TYPE_KEY, w.get(ORG_METRIC_TYPE_KEY));
             dynamoRow.put(TIMESTAMP_KEY, w.get(TIMESTAMP_KEY));
             dynamoRow.put(fieldname, w.get(fieldname));
             return dynamoRow;
           }).collect(Collectors.toList());

    Set<String> orgs = list.stream()
                           .map(w -> (String) w.get(ORG_ID_KEY))
                           .collect(Collectors.toSet());
    for (String org : orgs) {
      List<Map<String, Object>> orgWrote =
        list.stream().filter(w -> w.get(ORG_ID_KEY).equals(org))
            .sorted(
              (w1, w2) -> ((Long) w1.get(TIMESTAMP_KEY)).compareTo((Long) w2.get(TIMESTAMP_KEY)))
            .collect(Collectors.toList());
      runAndVerify(selectStarForOrg(org, FineoTestUtil.withNext(orgWrote)));
    }
  }

  private WriteResult writeOrgRow(TestState state, String org) throws Exception {
    long ts = get1980() + new Random().nextInt(1000);
    Map<String, Object> wrote = new HashMap<>();
    wrote.put(ORG_ID_KEY, org);
    wrote.put(ORG_METRIC_TYPE_KEY, metrictype);
    wrote.put(Schema.SORT_KEY_NAME, ts);
    wrote.put(fieldname, 2);
    Table table = state.writeToDynamo(wrote);
    return new WriteResult(wrote, table);
  }

  private class WriteResult {
    private Map<String, Object> wrote;
    private Table table;

    public WriteResult(Map<String, Object> wrote, Table table) {
      this.wrote = wrote;
      this.table = table;
    }

    public Map<String, Object> getWrote() {
      return wrote;
    }

    public Table getTable() {
      return table;
    }
  }

  private String getTenantTableName() {
    return name.getMethodName() + "-tenantTable";
  }

  private TenantInfoStore createTenantInfoStore() {
    String tenant = createTable(getTenantTableName(), TenantInfo.class).getTableName();
    String user = createTable(name.getMethodName() + "-userTable", UserInfo.class).getTableName();
    return new DynamoTenantInfoStore(dynamo.getClient(), tenant, user);
  }

  private TableDescription createTable(String name, Class clazz) {
    DynamoDBMapperConfig.Builder b = new DynamoDBMapperConfig.Builder();
    b.setTableNameOverride(new DynamoDBMapperConfig.TableNameOverride(name));
    b.setConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT);
    b.setSaveBehavior(DynamoDBMapperConfig.SaveBehavior.UPDATE);
    DynamoDBMapper mapper = new DynamoDBMapper(dynamo.getClient(), b.build());
    CreateTableRequest create = mapper.generateCreateTableRequest(clazz);
    create.setProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
    return dynamo.getClient().createTable(create).getTableDescription();
  }
}
