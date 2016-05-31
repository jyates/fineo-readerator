package io.fineo.read.drill.exec.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.drill.common.logical.StoragePluginConfigBase;

import java.util.Map;

/**
 *
 */
@JsonTypeName(FineoStoragePluginConfig.NAME)
public class FineoStoragePluginConfig extends StoragePluginConfigBase {

  public static final String NAME = "fineo";

  private Map<String, String> config;

  @JsonCreator
  public FineoStoragePluginConfig(@JsonProperty("config") Map<String, String> props) {
    this.config = props;
    if (config == null) {
      config = Maps.newHashMap();
    }
  }

  @JsonProperty
  public Map<String, String> getConfig() {
    return ImmutableMap.copyOf(config);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FineoStoragePluginConfig that = (FineoStoragePluginConfig) o;
    return config.equals(that.config);
  }

  @Override
  public int hashCode() {
    return this.config != null ? this.config.hashCode() : 0;
  }
}
