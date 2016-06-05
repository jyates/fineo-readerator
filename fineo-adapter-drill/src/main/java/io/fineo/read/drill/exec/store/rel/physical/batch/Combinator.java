package io.fineo.read.drill.exec.store.rel.physical.batch;

import org.apache.drill.exec.record.BatchSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Combines multiple potential sub-fields into a single know field
 */
public class Combinator {

  private Map<String, String> fieldMapping = new HashMap<>();
  private BatchSchema inSchema;

  public void combine(int incomingRecordCount) {

  }

  public void updateSchema(BatchSchema inSchema) {
    this.inSchema = inSchema;
  }

  public void addField(String inputName, String outputName) {
    fieldMapping.put(inputName, outputName);
  }

  public void addField(List<String> inputName, String outputName){
    for(String in: inputName){
      addField(in, outputName);
    }
  }
}
