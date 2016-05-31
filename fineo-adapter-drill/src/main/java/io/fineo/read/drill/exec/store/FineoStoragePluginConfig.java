package io.fineo.read.drill.exec.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.StoragePluginConfigBase;

import java.util.Map;

@JsonTypeName(FineoStoragePluginConfig.NAME)
public class FineoStoragePluginConfig extends StoragePluginConfigBase {

  public static final String NAME = "fineo";
  private final Map<String, String> repository;
  private final Map<String, String> aws;

  @JsonCreator
  public FineoStoragePluginConfig(@JsonProperty("repository") Map<String, String> repository,
    @JsonProperty("aws") Map<String, String> aws) {
    this.repository = repository;
    this.aws = aws;
  }

  public Map<String, String> getRepository() {
    return repository;
  }

  public Map<String, String> getAws() {
    return aws;
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
}
