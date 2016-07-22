package io.fineo.read.drill.exec.store.plugin.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.htrace.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.htrace.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(FsSourceTable.NAME)
public class FsSourceTable extends SourceTable {

  @JsonIgnore
  public static final String NAME = "fs-source";
  @JsonIgnore
  private static final String FILE_SCHEMA = "dfs";

  private final String format;
  private final String basedir;
  private final String org;

  public FsSourceTable(@JsonProperty("format") String format,
    @JsonProperty("basedir") String basedir,
    @JsonProperty("org") String org) {
    super(FILE_SCHEMA);
    this.format = format;
    this.basedir = basedir;
    this.org = org;
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
    if (!(o instanceof FsSourceTable))
      return false;

    FsSourceTable that = (FsSourceTable) o;

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
