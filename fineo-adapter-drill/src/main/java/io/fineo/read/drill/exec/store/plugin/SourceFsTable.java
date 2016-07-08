package io.fineo.read.drill.exec.store.plugin;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.htrace.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.htrace.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(SourceFsTable.NAME)
public class SourceFsTable {

  @JsonIgnore
  public static final String NAME = "source";
  @JsonIgnore
  private static final String FILE_SCHEMA = "dfs";

  private final String schema;
  private final String format;
  private final String basedir;
  private final String org;

  public SourceFsTable(@JsonProperty("format") String format,
    @JsonProperty("basedir") String basedir,
    @JsonProperty("org") String org) {
    this.schema = FILE_SCHEMA;
    this.format = format;
    this.basedir = basedir;
    this.org = org;
  }

  public String getSchema() {
    return schema;
  }

  public String getFormat() {
    return format;
  }

  public String getBasedir() {
    return basedir;
  }

  public String getOrg() {
    return org;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof SourceFsTable))
      return false;

    SourceFsTable that = (SourceFsTable) o;

    if (!getSchema().equals(that.getSchema()))
      return false;
    if (!getFormat().equals(that.getFormat()))
      return false;
    if (!getBasedir().equals(that.getBasedir()))
      return false;
    return getOrg().equals(that.getOrg());

  }

  @Override
  public int hashCode() {
    int result = getSchema().hashCode();
    result = 31 * result + getFormat().hashCode();
    result = 31 * result + getBasedir().hashCode();
    result = 31 * result + getOrg().hashCode();
    return result;
  }
}
