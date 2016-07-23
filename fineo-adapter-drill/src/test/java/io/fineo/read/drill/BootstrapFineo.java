package io.fineo.read.drill;

import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import io.fineo.read.drill.exec.store.plugin.source.SourceTable;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BootstrapFineo {

  private static final Logger LOG = LoggerFactory.getLogger(BootstrapFineo.class);
  private static final String URL = "http://127.0.0.1:8047";

  public final Map<String, Object> plugin = new HashMap<>();
  private final Map<String, Object> config = new HashMap<>();
  private final Map<String, String> repository = new HashMap<>();
  private final Map<String, String> aws = new HashMap<>();
  private final Map<String, String> dynamo = new HashMap<>();
  private final Multimap<String, FsSourceTable> sources = ArrayListMultimap.create();
  private final List<String> orgs = new ArrayList<>();

  public DrillConfigBuilder builder() {
    return new DrillConfigBuilder();
  }

  public class DrillConfigBuilder {

    public DrillConfigBuilder withRepository(String table) {
      BootstrapFineo.this.repository.put("table", table);
      return this;
    }

    public DrillConfigBuilder withLocalSource(FsSourceTable table) {
      BootstrapFineo.this.sources.put(table.getOrg(), table);
      return this;
    }

    public DrillConfigBuilder withLocalDynamo(String url) {
      BootstrapFineo.this.dynamo.put("url", url);
      return this;
    }

    private String build() throws JsonProcessingException {
      Preconditions.checkArgument(repository.size() > 0, "Must specify a repository table name!");
      buildInternal();
      ObjectMapper mapper = new ObjectMapper();
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(plugin);
    }

    public DrillConfigBuilder withOrgs(String... orgIds) {
      BootstrapFineo.this.orgs.addAll(Arrays.asList(orgIds));
      return this;
    }

    private void buildInternal() {
      plugin.put("name", "fineo");
      plugin.put("config", config);

      config.put("type", "fineo-test");
      config.put("enabled", "true");
      config.put("repository", repository);
      config.put("aws", aws);
      config.put("dynamo", dynamo);
      config.put("sources", sources.asMap());
      config.put("orgs", orgs);

      aws.put("credentials", "provided");
      aws.put("region", "us-east-1");
    }

    public DrillConfigBuilder withDynamoKeyMapper() {
      return this;
    }

    public DrillConfigBuilder withDynamoTable(Table table) {
      return this;
    }
  }

  public boolean strap(DrillConfigBuilder config) throws IOException {
    CloseableHttpClient httpclient = HttpClients.createDefault();
    HttpPost post = new HttpPost(URL + "/storage/fineo.json");
    post.setHeader("Content-type", "application/json");
    String plugin = config.build();
    LOG.debug("Updating plugin with config: " + plugin);
    post.setEntity(new StringEntity(plugin, ContentType.APPLICATION_JSON));
    CloseableHttpResponse response2 = httpclient.execute(post);

    boolean error = false;
    try {
      System.out.println(response2.getStatusLine());
      HttpEntity entity2 = response2.getEntity();
      if (response2.getStatusLine().getStatusCode() != 200) {
        StringWriter writer = new StringWriter();
        IOUtils.copy(entity2.getContent(), writer);
        String errorContent = writer.toString();
        System.err.println(errorContent);
        error = true;
      }
      EntityUtils.consume(entity2);
    } finally {
      response2.close();
    }
    return error;
  }
}
