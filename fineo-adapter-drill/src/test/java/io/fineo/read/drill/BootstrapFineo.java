package io.fineo.read.drill;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fineo.drill.exec.store.dynamo.config.DynamoEndpoint;
import io.fineo.drill.exec.store.dynamo.config.DynamoStoragePluginConfig;
import io.fineo.drill.exec.store.dynamo.config.StaticCredentialsConfig;
import io.fineo.drill.exec.store.dynamo.key.DynamoKeyMapperSpec;
import io.fineo.read.drill.exec.store.dynamo.DynamoFineoCompoundKeySpec;
import io.fineo.read.drill.exec.store.plugin.SchemaRepositoryConfig;
import io.fineo.read.drill.exec.store.plugin.source.DynamoSourceTable;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BootstrapFineo {

  private static final Logger LOG = LoggerFactory.getLogger(BootstrapFineo.class);
  private static final String URL = "http://127.0.0.1:8047";

  // fineo plugin configuration
  public final Map<String, Object> plugin = new HashMap<>();
  private SchemaRepositoryConfig repository = null;
  private final List<DynamoSourceTable> dynamoTables = new ArrayList<>();
  private final List<FsSourceTable> sources = new ArrayList<>();
  private final List<String> orgs = new ArrayList<>();

  // dynamo plugin configuration
  private final Map<String, Object> dynamo = new HashMap<>();
  private DynamoEndpoint dynamoEndpoint;
  private Map<String, Object> credentials;
  private Map<String, DynamoKeyMapperSpec> mappers = new HashMap<>();

  public DrillConfigBuilder builder() {
    return new DrillConfigBuilder();
  }

  public class DrillConfigBuilder {
    ObjectMapper mapper = new ObjectMapper();

    public DrillConfigBuilder withRepository(String table) {
      BootstrapFineo.this.repository = new SchemaRepositoryConfig(table);
      return this;
    }

    public DrillConfigBuilder withLocalSource(FsSourceTable table) {
      BootstrapFineo.this.sources.add(table);
      return this;
    }

    public DrillConfigBuilder withDynamoTables(String prefix, String pattern) {
      DynamoSourceTable table = new DynamoSourceTable(pattern, prefix);
      BootstrapFineo.this.dynamoTables.add(table);
      return this;
    }

    public DrillConfigBuilder withLocalDynamo(String url) {
      dynamoEndpoint = new DynamoEndpoint(url);
      return this;
    }

    public DrillConfigBuilder withDynamoKeyMapper() {
      mappers.put(".*", new DynamoFineoCompoundKeySpec());
      return this;
    }

    public DrillConfigBuilder withDynamoTable(Table table) {
      String name = table.getTableName();
      String prefix = name.substring(0, name.length() - 2);
      String regex = name;
      return withDynamoTables(prefix, regex);
    }

    public DrillConfigBuilder withCredentials(AWSCredentialsProvider provider) {
      Map<String, Object> credentials = new HashMap<>();
      credentials.put("type", "static");
      AWSCredentials creds = provider.getCredentials();
      credentials.put("static",
        new StaticCredentialsConfig(creds.getAWSAccessKeyId(), creds.getAWSSecretKey()));
      BootstrapFineo.this.credentials = credentials;
      return this;
    }

    private String build(Map<String, Object> plugin) throws IOException {
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(plugin);
    }

    public DrillConfigBuilder withOrgs(String... orgIds) {
      BootstrapFineo.this.orgs.addAll(Arrays.asList(orgIds));
      return this;
    }

    private void buildInternal() throws IOException {
      // build the plugin
      plugin.put("name", "fineo");
      Map<String, Object> config = new HashMap<>();
      plugin.put("config", config);

      config.put("type", "fineo");
      config.put("enabled", "true");
      config.put(SchemaRepositoryConfig.NAME, repository);
      config.put("dynamo-sources", dynamoTables);
      config.put("fs-sources", sources);
      config.put("orgs", orgs);

      // build dynamo
      dynamo.put("name", "dynamo");
      Map<String, Object> dynamoConfig;
      DynamoStoragePluginConfig storage = new DynamoStoragePluginConfig(credentials,
        dynamoEndpoint, null, null, mappers);
      dynamoConfig = mapper.readValue(mapper.writeValueAsString(storage), Map.class);
      dynamoConfig.put("enabled", "true");
      dynamo.put("config", dynamoConfig);
    }
  }

  public boolean strap(DrillConfigBuilder config) throws IOException {
    config.buildInternal();
    // load dynamo first so we ge the right dynamo credentials
    if (bootstrap("/storage/dynamo.json", config.build(this.dynamo))) {
      return bootstrap("/storage/fineo.json", config.build(this.plugin));
    }
    return false;
  }

  private boolean bootstrap(String storagePluginJson, String plugin) throws IOException {
    CloseableHttpClient httpclient = HttpClients.createDefault();
    HttpPost post = new HttpPost(URL + storagePluginJson);
    post.setHeader("Content-type", "application/json");
    LOG.debug("Updating " + storagePluginJson + " with config: " + plugin);
    post.setEntity(new StringEntity(plugin, ContentType.APPLICATION_JSON));
    CloseableHttpResponse response2 = httpclient.execute(post);

    boolean success = true;
    try {
      System.out.println(response2.getStatusLine());
      HttpEntity entity2 = response2.getEntity();
      if (response2.getStatusLine().getStatusCode() != 200) {
        StringWriter writer = new StringWriter();
        IOUtils.copy(entity2.getContent(), writer);
        String errorContent = writer.toString();
        System.err.println(errorContent);
        success = false;
      }
      EntityUtils.consume(entity2);
    } finally {
      response2.close();
    }
    return success;
  }
}
