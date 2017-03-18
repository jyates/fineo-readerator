package io.fineo.read.drill.exec.store.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.fineo.read.drill.exec.store.plugin.source.DynamoSourceTable;
import io.fineo.read.drill.exec.store.plugin.source.FsSourceTable;
import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.apache.drill.exec.store.dfs.FileSystemConfig;

import java.util.List;

@JsonIgnoreProperties
@JsonTypeName(FineoStoragePluginConfig.NAME)
public class FineoStoragePluginConfig extends StoragePluginConfigBase {

  public static final String NAME = "fineo";
  public static final String FS_SOURCES = "fs-sources";
  public static final String DYNAMO_SOURCES = "dynamo-sources";
  public static final String ORGS = "orgs";
  public static final String DYNAMO_TENANT_TABLE = "dynamo-tenant-table";
  public static final String WRITE_ERRORS = "write-errors";

  private final SchemaRepositoryConfig repository;
  private final List<FsSourceTable> fsTables;
  private final List<DynamoSourceTable> dynamoSources;
  private final List<String> orgs;
  private final String tenantTable;

  @JsonCreator
  public FineoStoragePluginConfig(
    @JsonProperty(SchemaRepositoryConfig.NAME) SchemaRepositoryConfig repository,
    @JsonProperty(ORGS) List<String> orgs,
    @JsonProperty(FS_SOURCES) List<FsSourceTable> fsSources,
    @JsonProperty(DYNAMO_SOURCES) List<DynamoSourceTable> dynamoSources,
    @JsonProperty(DYNAMO_TENANT_TABLE) String tenantTable) {
    this.orgs = orgs;
    this.repository = repository;
    this.fsTables = fsSources;
    this.dynamoSources = dynamoSources;
    this.tenantTable = tenantTable;
  }

  @JsonProperty(SchemaRepositoryConfig.NAME)
  public SchemaRepositoryConfig getRepository() {
    return repository;
  }

  @JsonProperty(FS_SOURCES)
  public List<FsSourceTable> getFsSources() {
    return fsTables;
  }

  @JsonProperty(DYNAMO_SOURCES)
  public List<DynamoSourceTable> getDynamoSources() {
    return dynamoSources;
  }

  @JsonProperty(ORGS)
  public List<String> getOrgs() {
    return orgs;
  }

  @JsonProperty(DYNAMO_TENANT_TABLE)
  public String getDynamoTenantTable() {
    return this.tenantTable;
  }

//  @JsonProperty(WRITE_ERRORS)
//  public FileSystemConfig getWriteErrors() {
//    return writeErrors;
//  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof FineoStoragePluginConfig))
      return false;

    FineoStoragePluginConfig that = (FineoStoragePluginConfig) o;

    if (getRepository() != null ? !getRepository().equals(that.getRepository()) :
        that.getRepository() != null)
      return false;
    if (fsTables != null ? !fsTables.equals(that.fsTables) : that.fsTables != null)
      return false;
    if (getDynamoSources() != null ? !getDynamoSources().equals(that.getDynamoSources()) :
        that.getDynamoSources() != null)
      return false;
    if (getOrgs() != null ? !getOrgs().equals(that.getOrgs()) : that.getOrgs() != null)
      return false;
    return tenantTable != null ? tenantTable.equals(that.tenantTable) : that.tenantTable == null;

  }

  @Override
  public int hashCode() {
    int result = getRepository() != null ? getRepository().hashCode() : 0;
    result = 31 * result + (fsTables != null ? fsTables.hashCode() : 0);
    result = 31 * result + (getDynamoSources() != null ? getDynamoSources().hashCode() : 0);
    result = 31 * result + (getOrgs() != null ? getOrgs().hashCode() : 0);
    result = 31 * result + (tenantTable != null ? tenantTable.hashCode() : 0);
    return result;
  }
}
