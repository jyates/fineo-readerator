package io.fineo.read.drill.exec.store.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.StoragePluginConfigBase;

import java.util.List;
import java.util.Map;

@JsonTypeName(FineoStoragePluginConfig.NAME)
public class FineoStoragePluginConfig extends StoragePluginConfigBase {

  public static final String NAME = "fineo";
  private final Map<String, String> repository;
  private final Map<String, String> aws;
  private final Map<String, List<String>> sources;
  private final List<String> orgs;

  @JsonCreator
  public FineoStoragePluginConfig(@JsonProperty("repository") Map<String, String> repository,
    @JsonProperty("aws") Map<String, String> aws,
    @JsonProperty("sources") Map<String, List<String>> sources,
    @JsonProperty("orgs") List<String> orgs) {
    this.repository = repository;
    this.aws = aws;
    this.sources = sources;
    this.orgs = orgs;
  }

  public Map<String, String> getRepository() {
    return repository;
  }

  public Map<String, String> getAws() {
    return aws;
  }

  public Map<String, List<String>> getSources() {
    return sources;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof FineoStoragePluginConfig))
      return false;

    FineoStoragePluginConfig that = (FineoStoragePluginConfig) o;

    if (!getRepository().equals(that.getRepository()))
      return false;
    return getAws().equals(that.getAws());

  }

  @Override
  public int hashCode() {
    int result = getRepository().hashCode();
    result = 31 * result + getAws().hashCode();
    return result;
  }

  public List<String> getOrgs() {
    return orgs;
  }
}
