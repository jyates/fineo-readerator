package io.fineo.read.drill.exec.store.plugin.source;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.htrace.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.htrace.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("fs-source")
public class FsSourceTable extends SourceTable {

  @JsonIgnore
  public static final String NAME = "fs-source";
  @JsonIgnore
  private static final String FILE_SCHEMA = "dfs";

  private final String format;
  private final String basedir;

  @JsonCreator
  public FsSourceTable(@JsonProperty("format") String format,
    @JsonProperty("basedir") String basedir) {
    super(FILE_SCHEMA);
    this.format = format;
    this.basedir = basedir;
  }

  public String getFormat() {
    return format;
  }

  public String getBasedir() {
    return basedir;
  }

  @JsonProperty
  public String getSchema() {
    return schema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof FsSourceTable))
      return false;

    FsSourceTable that = (FsSourceTable) o;

    if (!getFormat().equals(that.getFormat()))
      return false;
    return getBasedir().equals(that.getBasedir());

  }

  @Override
  public int hashCode() {
    int result = getFormat().hashCode();
    result = 31 * result + getBasedir().hashCode();
    return result;
  }
}
