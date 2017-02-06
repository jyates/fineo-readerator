package org.apache.calcite.avatica.jdbc;

import io.fineo.read.serve.TenantValidator;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Wrapper Meta that ensures we don't do bad things like:
 * <ol>
 * <li>setting the user/password on the connection</li>
 * <li>COMPANY_KEY is set to the server key</li>
 * </ol>
 */
public class FineoWrapperJdbcMeta extends JdbcMeta {

  private static final Logger LOG = LoggerFactory.getLogger(FineoWrapperJdbcMeta.class);
  private static final List<String> DISALLOWED_KEYS = new ArrayList<>();

  static {
    DISALLOWED_KEYS.add("user");
    DISALLOWED_KEYS.add("password");
  }

  private final TenantValidator validator;

  public FineoWrapperJdbcMeta(String url, Properties info,
    MetricsSystem metrics, TenantValidator validator) throws SQLException {
    super(url, info, metrics);
    this.validator = validator;
    LOG.debug("Creating Fineo Wrapper Metadata");
  }

  @Override
  public void openConnection(ConnectionHandle ch, Map<String, String> info) {
    if (containsDisallowedKey(info)) {
      info = info.entrySet().stream()
                 .filter(e -> !DISALLOWED_KEYS.contains(e.getKey()))
                 .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
    validator.validateConnection(info);
    super.openConnection(ch, info);
  }

  private boolean containsDisallowedKey(Map<String, String> info) {
    return DISALLOWED_KEYS.stream().anyMatch(info::containsKey);
  }
}
