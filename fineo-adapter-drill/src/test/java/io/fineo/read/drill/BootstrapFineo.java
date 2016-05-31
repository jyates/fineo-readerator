package io.fineo.read.drill;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class BootstrapFineo {

  private static final String URL = "http://127.0.0.1:8047";

  public final Map<String, Object> plugin = new HashMap<>();
  private final Map<String, Object> config = new HashMap<>();
  private final Map<String, String> repository = new HashMap<>();
  private final Map<String, String> aws = new HashMap<>();
  private final Map<String, String> dynamo = new HashMap<>();
  private final Map<String, List<String>> sources = new HashMap<>();
  private final List<String> files = new ArrayList<>();

  {
    plugin.put("name", "fineo");
    plugin.put("config", config);

    config.put("type", "fineo-test");
    config.put("enabled", "true");
    config.put("repository", repository);
    config.put("aws", aws);
    config.put("dynamo", dynamo);
    config.put("sources", sources);

    aws.put("credentials", "provided");
    aws.put("region", "us-east-1");

    sources.put("dfs", files);
  }

  public DrillConfigBuilder builder() {
    return new DrillConfigBuilder();
  }

  public class DrillConfigBuilder {

    public DrillConfigBuilder withRepository(String table) {
      BootstrapFineo.this.repository.put("table", table);
      return this;
    }

    public DrillConfigBuilder withLocalSource(File file) {
      BootstrapFineo.this.files.add(file.getPath());
      return this;
    }

    public DrillConfigBuilder withLocalDynamo(String url) {
      BootstrapFineo.this.dynamo.put("url", url);
      return this;
    }

    private String build() throws JsonProcessingException {
      Preconditions.checkArgument(repository.size() > 0, "Must specify a repository table name!");
      ObjectMapper mapper = new ObjectMapper();
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(plugin);
    }
  }


  public void strap(DrillConfigBuilder config) throws IOException {
    CloseableHttpClient httpclient = HttpClients.createDefault();
    HttpPost post = new HttpPost(URL + "/storage/fineo.json");
    post.setHeader("Content-type", "application/json");
    String plugin = config.build();
    post.setEntity(new StringEntity(plugin, ContentType.APPLICATION_JSON));
    CloseableHttpResponse response2 = httpclient.execute(post);

    try {
      System.out.println(response2.getStatusLine());
      HttpEntity entity2 = response2.getEntity();
      if (response2.getStatusLine().getStatusCode() != 200) {
        StringWriter writer = new StringWriter();
        IOUtils.copy(entity2.getContent(), writer);
        String errorContent = writer.toString();
        System.err.println(errorContent);
      }
      EntityUtils.consume(entity2);
    } finally {
      response2.close();
    }
  }
}
