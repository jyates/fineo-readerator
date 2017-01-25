package io.fineo.read.serve;

import org.apache.calcite.avatica.jdbc.FineoJdbcMeta;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

public class TestTenantValidator {

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  private static final String orgid = "orgid";

  @Test
  public void testNoOrgAndOrgId() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    new TenantValidator(orgid, true);
  }

  @Test
  public void testOrgIdOrNoOrg() throws Exception {
    new TenantValidator(orgid, false);
    new TenantValidator(null, true);
  }

  @Test
  public void testValidateOrg() throws Exception {
    Map<String, String> props = new HashMap<>();

    props.put(FineoJdbcMeta.ORG_PROPERTY_KEY, orgid);
    new TenantValidator(orgid, false).validateConnection(props);
  }

  @Test
  public void testMissingOrgInValidation() throws Exception {
    TenantValidator validator = new TenantValidator(orgid, false);
    thrown.expect(NullPointerException.class);
    validator.validateConnection(new HashMap<>());
  }

  @Test
  public void testWrongOrgInValidation() throws Exception {
    TenantValidator validator = new TenantValidator(orgid, false);
    Map<String, String> info = new HashMap<>();
    info.put(FineoJdbcMeta.ORG_PROPERTY_KEY, "another_org");
    thrown.expect(IllegalArgumentException.class);
    validator.validateConnection(info);
  }

  @Test
  public void testNoOrgButMissingOrgId() throws Exception {
    TenantValidator validator = new TenantValidator(null, true);
    thrown.expect(NullPointerException.class);
    validator.validateConnection(new HashMap<>());
  }

  @Test
  public void testHasOrgInRequestButNoId() throws Exception {
    TenantValidator validator = new TenantValidator(null, true);
    Map<String, String> info = new HashMap<>();
    info.put(FineoJdbcMeta.ORG_PROPERTY_KEY, "another_org");
    validator.validateConnection(info);
  }
}
