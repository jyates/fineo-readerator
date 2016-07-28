package io.fineo.read.drill.e2e.commands;

import com.google.inject.Module;

import java.util.List;
import java.util.Map;

/**
 *
 */
public abstract class BaseCommand {
  public abstract void run(List<Module> baseModules, Map<String, Object> event) throws Exception;
}
