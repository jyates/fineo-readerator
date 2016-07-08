package io.fineo.read.drill.exec.store;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.fineo.read.drill.exec.store.plugin.FineoStoragePluginConfig;
import io.fineo.read.drill.exec.store.plugin.SourceFsTable;

import java.util.List;
import java.util.Map;

@JsonTypeName(FineoLocalTestStoragePluginConfig.NAME)
public class FineoLocalTestStoragePluginConfig extends FineoStoragePluginConfig {
  public static final String NAME = "fineo-test";
  private final Map<String, String> dynamo;

  public FineoLocalTestStoragePluginConfig(
    @JsonProperty("repository") Map<String, String> repository,
    @JsonProperty("aws") Map<String, String> aws,
    @JsonProperty("sources") Map<String, List<SourceFsTable>>  sources,
    @JsonProperty("dynamo") Map<String, String> dynamo) {
    super(repository, aws, sources);
    this.dynamo = dynamo;
  }

  public Map<String, String> getDynamo() {
    return dynamo;
  }
}
