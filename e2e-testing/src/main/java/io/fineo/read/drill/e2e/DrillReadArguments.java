package io.fineo.read.drill.e2e;

import com.beust.jcommander.Parameter;

/**
 *
 */
public class DrillReadArguments {

  @Parameter(names = "--org", description = "Org ID from which to read", required = true)
  public String orgId;

  @Parameter(names = "--metric-type", description = "User visible name of the metric to read",
             required = true)
  public String metricType;

  @Parameter(names = "--input", description = "Directory where the input files are stored")
  public String inputDir;

  @Parameter(names = "--output", description = "File in which to store the results")
  public String output;
}
