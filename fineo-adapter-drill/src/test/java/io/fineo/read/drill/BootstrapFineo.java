package io.fineo.read.drill;

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

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.lang.String.format;

/**
 *
 */
public class BootstrapFineo {

  private static final String URL = "http://127.0.0.1:8047";

  public static final String UPDATE =
    "{\n" +
    "  \"name\": \"fineo-test\",\n" +
    "  \"config\": {\n" +
    "    \"type\" : \"fineo-test\",\n" +
    "    \"enabled\" : \"true\",\n" +
    // actual config for the sub-components
    "    \"repository\" : {\n" +
    "       \"table\": \"%s\"\n" +
    "    },\n" +
    "    \"aws\": {\n" +
    "      \"credentials\": \"provided\",\n" +
    "      \"region\": \"us-east-1\"\n" +
    "    }" +
    // subschemas/components
    "%s" +
    "  }" +
    "}";

  private static final String DYNAMO = ", \"dynamo\": {\n"
                                       + "  \"url\": \"%s\"\n"
                                       + "  }";

  private static final String JSON = ",\n \"json\": {\n"
                                     + "    \"type\": \"file\",\n"
                                     + "    \"enabled\": true,\n"
                                     + "    \"connection\": \"file:///\",\n"
                                     + "    \"workspaces\": {\n"
                                     + "      \"root\": {\n"
                                     + "        \"location\": \"%s\",\n"
                                     + "        \"writable\": false,\n"
                                     + "        \"defaultInputFormat\": null\n"
                                     + "       }\n"
                                     + "    },\n"
                                     + "    \"formats\" : {\n"
                                     + "      \"json\" : {\n"
                                     + "        \"type\" : \"json\"\n"
                                     + "      }\n"
                                     + "    }\n"
                                     + "  }";

  public static String getJson() {
    Path currentRelativePath = Paths.get("fineo-adapter-drill", "src", "test", "resources", "json");
    return getJson(currentRelativePath);
  }

  public static String getJson(Path relativeJson) {
    String s = relativeJson.toAbsolutePath().toString();
    return format(JSON, s);
  }

  public static class DrillConfigBuilder {
    private String repository;
    private String dynamo;

    public DrillConfigBuilder withRepository(String table) {
      this.repository = table;
      return this;
    }

    public DrillConfigBuilder withLocalDynamo(String url) {
      this.dynamo = url;
      return this;
    }

    private String build() {
      Preconditions.checkNotNull(repository, "Must specify a repository table name!");
      String json = getJson();
      String dynamo = format(DYNAMO, this.dynamo);
      String components = dynamo + json;
      return format(UPDATE, repository, components);
    }
  }


  public static void bootstrap(DrillConfigBuilder config) throws IOException {
    CloseableHttpClient httpclient = HttpClients.createDefault();
    HttpPost post = new HttpPost(URL+"/storage/fineo-test.json");
    post.setHeader("Content-type", "application/json");
    String plugin = config.build();
    post.setEntity(new StringEntity(plugin, ContentType.APPLICATION_JSON));
    CloseableHttpResponse response2 = httpclient.execute(post);

    try {
      System.out.println(response2.getStatusLine());
      HttpEntity entity2 = response2.getEntity();
      if(response2.getStatusLine().getStatusCode() != 200){
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
