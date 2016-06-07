package io.fineo.read.drill.exec.store.rel.physical.batch;

import com.google.common.cache.LoadingCache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class Allocator<T> {

  private final DoAllocator<T> alloc;
  private Set<T> allocated = new HashSet<>();

  public Allocator(DoAllocator<T> alloc) {
    this.alloc = alloc;
  }

  public void ensureAllocated(T field) {
    if (allocated.contains(field)) {
      return;
    }
    alloc.alloc(field);
    allocated.add(field);
  }

  public void clear() {
    this.allocated.clear();
  }

  public interface DoAllocator<V> {
    void alloc(V vector);
  }
}
