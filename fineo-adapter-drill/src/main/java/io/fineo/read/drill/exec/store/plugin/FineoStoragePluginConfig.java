package io.fineo.read.drill.exec.store.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import org.apache.drill.common.logical.StoragePluginConfigBase;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@JsonTypeName(FineoStoragePluginConfig.NAME)
public class FineoStoragePluginConfig extends StoragePluginConfigBase {

  public static final String NAME = "fineo";
  private final Map<String, String> repository;
  private final Map<String, String> aws;
  private final  Map<String, List<FsSourceTable>> sources;

  @JsonCreator
  public FineoStoragePluginConfig(@JsonProperty("repository") Map<String, String> repository,
    @JsonProperty("aws") Map<String, String> aws,
    @JsonProperty("sources")  Map<String, List<FsSourceTable>> sources) {
    this.repository = repository;
    this.aws = aws;
    this.sources = sources;
  }

  public Map<String, String> getRepository() {
    return repository;
  }

  public Map<String, String> getAws() {
    return aws;
  }

  public Map<String, List<FsSourceTable>> getSources() {
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

  @JsonIgnore
  public Collection<String> getOrgs() {
    return sources.keySet();
  }
}
