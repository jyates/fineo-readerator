package io.fineo.read.drill.e2e;

import com.beust.jcommander.JCommander;
import io.fineo.e2e.options.LocalSchemaStoreOptions;
import io.fineo.read.drill.e2e.commands.Command;
import io.fineo.read.drill.e2e.options.DrillArguments;

public class EndToEndWrapper {
  public static void main(String[] args) throws Throwable {
    DrillArguments opts = new DrillArguments();
    LocalSchemaStoreOptions storeOptions = new LocalSchemaStoreOptions();
    JdbcOption jdbc = new JdbcOption();
    JCommander jc = new JCommander(new Object[]{opts, storeOptions, jdbc});
    jc.addCommand("remote", new RemoteReadCommand(opts, storeOptions, jdbc));
    jc.parse(args);

    String cmd = jc.getParsedCommand();
    Command command = (Command) jc.getCommands().get(cmd).getObjects().get(0);
    command.run();
  }
}
