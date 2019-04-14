package io.atomix.core.counter.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.AbstractPrimitiveProxy;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.proxy.ProxyClient;

/**
 * Atomic counter proxy.
 */
public class DefaultAtomicCounterProxy extends AbstractPrimitiveProxy<AtomicCounterService> implements AtomicCounterProxy {
  public DefaultAtomicCounterProxy(ProxyClient<AtomicCounterService> client, PrimitiveRegistry registry) {
    super(client, registry);
  }

  @Override
  public CompletableFuture<GetResponse> get(GetRequest request) {
    return getProxyClient().applyBy(name(), CounterOperation.GET, request, GetRequest::toByteArray, GetResponse::parseFrom);
  }

  @Override
  public CompletableFuture<SetResponse> set(SetRequest request) {
    return getProxyClient().applyBy(name(), CounterOperation.SET, request, SetRequest::toByteArray, SetResponse::parseFrom);
  }

  @Override
  public CompletableFuture<IncrementResponse> increment(IncrementRequest request) {
    return getProxyClient().applyBy(name(), CounterOperation.INCREMENT, request, IncrementRequest::toByteArray, IncrementResponse::parseFrom);
  }

  @Override
  public CompletableFuture<DecrementResponse> decrement(DecrementRequest request) {
    return getProxyClient().applyBy(name(), CounterOperation.DECREMENT, request, DecrementRequest::toByteArray, DecrementResponse::parseFrom);
  }
}
