package io.fineo.read.drill.e2e.options;

import com.beust.jcommander.Parameter;

public class OrgArgument {

  @Parameter(names = "--org", description = "Org to read")
  private String org;

  public String get() {
    return org;
  }
}
