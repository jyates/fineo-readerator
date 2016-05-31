package io.fineo.read.drill.exec.store;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.exec.store.dfs.FileSystemConfig;

import java.util.Map;

@JsonTypeName(FineoLocalTestStoragePluginConfig.NAME)
public class FineoLocalTestStoragePluginConfig extends FineoStoragePluginConfig{
  public static final String NAME = "fineo-test";
  private final FileSystemConfig json;
  private final Map<String, String> dynamo;

  public FineoLocalTestStoragePluginConfig(
    @JsonProperty("repository") Map<String, String> repository,
    @JsonProperty("aws") Map<String, String> aws,
    @JsonProperty("json")FileSystemConfig json,
    @JsonProperty("dynamo") Map<String, String> dynamo) {
    super(repository, aws);
    this.json = json;
    this.dynamo = dynamo;
  }

  public FileSystemConfig getJson() {
    return json;
  }

  public Map<String, String> getDynamo() {
    return dynamo;
  }
}
