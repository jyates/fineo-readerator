package io.fineo.read.drill.e2e.options;

import com.beust.jcommander.Parameter;

public class BatchOutputArgument {

  @Parameter(names = "--batch-dir", description = "Directory where batch conversion wrote data")
  private String batchDir;

  public String get() {
    return batchDir;
  }
}
