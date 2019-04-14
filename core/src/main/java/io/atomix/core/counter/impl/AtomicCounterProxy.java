package io.atomix.core.counter.impl;

import java.util.concurrent.CompletableFuture;

/**
 * Atomic counter proxy.
 */
public interface AtomicCounterProxy {

  CompletableFuture<GetResponse> get(GetRequest request);

  CompletableFuture<SetResponse> set(SetRequest request);

  CompletableFuture<IncrementResponse> increment(IncrementRequest request);

  CompletableFuture<DecrementResponse> decrement(DecrementRequest request);

}
