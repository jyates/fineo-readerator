package io.fineo.read.serve;

import com.google.common.base.Preconditions;
import io.fineo.read.FineoJdbcProperties;

import java.util.Map;

/**
 * Encapsulate the work for validating a tenant in a request
 */
public class TenantValidator {

  private String org;
  private boolean noOrg;

  public TenantValidator(String orgId, boolean noOrg) {
    boolean hasOrgId = orgId != null && orgId.length() > 0;
    Preconditions.checkArgument((!hasOrgId && noOrg) || (hasOrgId && !noOrg),
      "Either specify an org id or that there is no org, not both!");
    this.org = orgId;
    this.noOrg = noOrg;
  }


  public void validateConnection(Map<String, String> info){
    String specifiedOrg = info.get(FineoJdbcProperties.COMPANY_KEY_PROPERTY);
    Preconditions.checkNotNull(specifiedOrg, "No API KEY present in connection!");
    if(this.noOrg){
      return;
    }
    Preconditions.checkArgument(org.equals(specifiedOrg),
      "Got an invalid API KEY: %s. Check that you copied the correct key",
      specifiedOrg);
  }
}
