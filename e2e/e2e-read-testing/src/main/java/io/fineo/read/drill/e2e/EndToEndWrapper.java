package io.fineo.read.drill.e2e;

import com.beust.jcommander.JCommander;
import io.fineo.e2e.options.LocalSchemaStoreOptions;
import io.fineo.read.drill.e2e.command.ReadCommand;
import io.fineo.read.drill.e2e.command.Reader;
import io.fineo.read.drill.e2e.commands.Command;
import io.fineo.read.drill.e2e.options.DrillArguments;
import io.fineo.read.drill.e2e.command.AvaticaRead;
import io.fineo.read.drill.e2e.command.FineoAwsRead;
import io.fineo.read.drill.e2e.command.FineoLocalRead;

public class EndToEndWrapper {
  public static void main(String[] args) throws Throwable {
    DrillArguments opts = new DrillArguments();
    LocalSchemaStoreOptions storeOptions = new LocalSchemaStoreOptions();

    ReadCommand read = new ReadCommand(opts, storeOptions);
    AvaticaRead avatica = new AvaticaRead();
    FineoLocalRead local = new FineoLocalRead();
    FineoAwsRead aws = new FineoAwsRead();
    JCommander jc = new JCommander(new Object[]{opts, storeOptions, read, aws});
    jc.addCommand("avatica", avatica);
    jc.addCommand("fineo-local", local);
    jc.addCommand("fineo-aws", aws);
    jc.parse(args);

    String cmd = jc.getParsedCommand();
    Reader command = (Reader) jc.getCommands().get(cmd).getObjects().get(0);
    read.setReader(command);
    read.run();
  }
}
