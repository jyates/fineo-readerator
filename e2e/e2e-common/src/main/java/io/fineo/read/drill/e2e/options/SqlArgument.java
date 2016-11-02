package io.fineo.read.drill.e2e.options;

import com.beust.jcommander.Parameter;
import com.google.common.base.Joiner;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class SqlArgument {

  @Parameter(description = "query")
  private List<String> queryParts = new ArrayList<>();

  @Parameter(names = "--sql", description = "File to read for the sql command to run")
  private String sql;

  public String getQuery() throws IOException {
    if (sql != null) {
      return new String(Files.readAllBytes(FileSystems.getDefault().getPath(sql)));
    }
    return Joiner.on(" ").join(queryParts);
  }
}
