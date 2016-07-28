package io.fineo.read.drill.e2e.options;

import com.beust.jcommander.Parameter;

public class MetricArgument {

  @Parameter(names = "--metric", description = "Metric to read")
  public String metric;

  public String get() {
    return metric;
  }
}
