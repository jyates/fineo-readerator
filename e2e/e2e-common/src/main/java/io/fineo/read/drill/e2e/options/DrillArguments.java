package io.fineo.read.drill.e2e.options;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class DrillArguments {
  @Parameter(names = "--output")
  public String outputFile;

  @ParametersDelegate
  public BatchOutputArgument input = new BatchOutputArgument();

  @ParametersDelegate
  public MetricArgument metric = new MetricArgument();

  @ParametersDelegate
  public OrgArgument org = new OrgArgument();

  @ParametersDelegate
  public DynamoArgument dynamo = new DynamoArgument();

  public int webPort = 8147;
}
