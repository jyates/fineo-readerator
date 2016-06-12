package io.fineo.read.drill.exec.store;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePluginConfig;

import java.util.List;
import java.util.Map;

@JsonTypeName(FineoLocalTestStoragePluginConfig.NAME)
public class FineoLocalTestStoragePluginConfig extends FineoStoragePluginConfig {
  public static final String NAME = "fineo-test";
  private final Map<String, String> dynamo;

  public FineoLocalTestStoragePluginConfig(
    @JsonProperty("repository") Map<String, String> repository,
    @JsonProperty("aws") Map<String, String> aws,
    @JsonProperty("sources")Map<String, List<String>> sources,
    @JsonProperty("dynamo") Map<String, String> dynamo,
    @JsonProperty("orgs")List<String> orgs) {
    super(repository, aws, sources, orgs);
    this.dynamo = dynamo;
  }

  public Map<String, String> getDynamo() {
    return dynamo;
  }
}
