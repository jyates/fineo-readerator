package io.fineo.read.drill.e2e.options;


import com.beust.jcommander.Parameter;

public class DynamoArgument {
  @Parameter(names = "--dynamo-table-prefix")
  public String prefix;
}
