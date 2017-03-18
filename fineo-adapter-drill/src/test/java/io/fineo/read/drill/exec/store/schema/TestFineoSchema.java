package io.fineo.read.drill.exec.store.schema;


import io.fineo.drill.ClusterTest;
import io.fineo.read.drill.BaseFineoDynamoTest;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePlugin;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePluginConfig;
import io.fineo.read.drill.exec.store.plugin.SchemaRepositoryConfig;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreManager;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.read.drill.FineoTestUtil.p;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.internal.util.collections.Sets.newSet;

@Category(ClusterTest.class)
public class TestFineoSchema extends BaseFineoDynamoTest {

  /**
   * If we reuse the schema store, we need ensure that when the tenant changes (e.g. new table)
   * we go and load that table information.
   * @throws Exception
   */
  @Test
  public void testReuseSchemaStore() throws Exception {
    Pair<String, StoreManager.Type> field = p(fieldname, StoreManager.Type.INT);
    register(field);
    SchemaRepositoryConfig repoConfig = new SchemaRepositoryConfig(getSchemaStoreTableName());
    FineoStoragePluginConfig config = new FineoStoragePluginConfig(repoConfig, newArrayList(org),
      Collections.emptyList(), Collections.emptyList(), null);
    SchemaStore store = FineoSchemaFactory.createSchemaStore(config, tables.getAsyncClient());

    FineoStoragePlugin plugin = null;
    FineoSchema schema = new FineoSchema(newArrayList("fineo"), org, plugin, null, store);
    Set<String> names = schema.getTableNames();

    // add a new metric to the store from "another client"
    SchemaStore remote = createDynamoSchemaStore();
    StoreManager manager = new StoreManager(remote);
    manager.updateOrg(org).newMetric().setDisplayName("newmetric")
      .build().commit();

    assertNotEquals(names, schema.getTableNames());
    assertEquals(newSet(metrictype, "newmetric"), schema.getTableNames());
  }
}
