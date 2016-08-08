package io.fineo.read.drill.e2e.options;

import com.beust.jcommander.Parameter;

/**
 *
 */
public class ProfileAuthenticationOption {

  @Parameter(names = "--auth-profile-name",
             description = "~/.aws/credentials authentication profile name")
  public String name;
}
