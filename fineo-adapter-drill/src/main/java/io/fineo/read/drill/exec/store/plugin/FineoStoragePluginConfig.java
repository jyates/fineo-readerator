package io.fineo.read.drill.exec.store.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.fineo.read.drill.exec.store.plugin.source.DynamoSourceTable;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import org.apache.drill.common.logical.StoragePluginConfigBase;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties
@JsonTypeName(FineoStoragePluginConfig.NAME)
public class FineoStoragePluginConfig extends StoragePluginConfigBase {

  public static final String NAME = "fineo";
  private final SchemaRepositoryConfig repository;
  private final List<FsSourceTable> fsTables;
  private final List<DynamoSourceTable> dynamoSources;
  private final List<String> orgs;

  @JsonCreator
  public FineoStoragePluginConfig(
    @JsonProperty(SchemaRepositoryConfig.NAME) SchemaRepositoryConfig repository,
    @JsonProperty("orgs") List<String> orgs,
    @JsonProperty("fs-sources") List<FsSourceTable> fsSources,
    @JsonProperty("dynamo-sources") List<DynamoSourceTable> dynamoSources) {
    this.orgs = orgs;
    this.repository = repository;
    this.fsTables = fsSources;
    this.dynamoSources = dynamoSources;
  }

  @JsonProperty(SchemaRepositoryConfig.NAME)
  public SchemaRepositoryConfig getRepository() {
    return repository;
  }

  @JsonProperty("fs-sources")
  public List<FsSourceTable> getFsSources() {
    return fsTables;
  }

  @JsonProperty("dynamo-sources")
  public List<DynamoSourceTable> getDynamoSources() {
    return dynamoSources;
  }

  @JsonProperty("orgs")
  public List<String> getOrgs() {
    return orgs;
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
    if (fsTables != null ? !fsTables.equals(that.fsTables) : that.fsTables != null)
      return false;
    if (getDynamoSources() != null ? !getDynamoSources().equals(that.getDynamoSources()) :
        that.getDynamoSources() != null)
      return false;
    return getOrgs().equals(that.getOrgs());

  }

  @Override
  public int hashCode() {
    int result = getRepository().hashCode();
    result = 31 * result + (fsTables != null ? fsTables.hashCode() : 0);
    result = 31 * result + (getDynamoSources() != null ? getDynamoSources().hashCode() : 0);
    result = 31 * result + getOrgs().hashCode();
    return result;
  }
}
