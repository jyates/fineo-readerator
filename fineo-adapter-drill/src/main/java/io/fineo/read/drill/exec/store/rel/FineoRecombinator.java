package io.fineo.read.drill.exec.store.rel;

import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.interpreter.Node;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.interpreter.Sink;
import org.apache.calcite.interpreter.Source;

/**
 * Actually does the row recombination
 */
public class FineoRecombinator implements Node {
  protected final Source source;
  protected final Sink sink;
  protected final FineoRecombinatorRel rel;

  public FineoRecombinator(Interpreter interpreter, FineoRecombinatorRel rel) {
    this.rel = rel;
    this.source = interpreter.source(rel, 0);
    this.sink = interpreter.sink(rel);
  }

  @Override
  public void run() throws InterruptedException {
    Row row;
    // iterate the rows from the source
    while ((row = source.receive()) != null) {
      // do the conversion to the actual field by dropping the null or not-applicable fields
      System.out.println("Got row: "+row);

      // send the row up the pipeline
      sink.send(row);
    }
  }
}
