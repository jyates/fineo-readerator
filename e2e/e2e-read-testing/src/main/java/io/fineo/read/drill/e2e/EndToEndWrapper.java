package io.fineo.read.drill.e2e;

import com.beust.jcommander.JCommander;
import io.fineo.read.drill.e2e.command.AvaticaRead;
import io.fineo.read.drill.e2e.command.FineoAwsRead;
import io.fineo.read.drill.e2e.command.FineoLocalRead;
import io.fineo.read.drill.e2e.command.ReadCommand;
import io.fineo.read.drill.e2e.command.Reader;
import io.fineo.read.drill.e2e.options.DrillArguments;

public class EndToEndWrapper {
  public static void main(String[] args) throws Throwable {
    DrillArguments opts = new DrillArguments();

    ReadCommand read = new ReadCommand(opts);
    AvaticaRead avatica = new AvaticaRead();
    FineoLocalRead local = new FineoLocalRead();
    FineoAwsRead aws = new FineoAwsRead();
    JCommander jc = new JCommander(new Object[]{opts, read, aws});
    jc.setAcceptUnknownOptions(true);
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
