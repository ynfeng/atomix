/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.set.impl;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.utils.concurrent.Futures;

/**
 * Raw async distributed set.
 */
public class RawAsyncDistributedSet extends AbstractAsyncPrimitive<SetProxy> implements AsyncDistributedSet<String> {
  private final Map<Long, AsyncDistributedSetIterator> iterators = Maps.newConcurrentMap();
  private final Map<CollectionEventListener<String>, Executor> eventListeners = Maps.newConcurrentMap();

  public RawAsyncDistributedSet(SetProxy proxy) {
    super(proxy);
    proxy.onIterate(this::onIterate);
  }

  private void onIterate(IterateEvent event) {
    AsyncDistributedSetIterator iterator = iterators.get(event.getIndex());
    if (iterator != null) {
      iterator.next(event);
    }
  }

  @Override
  public CompletableFuture<Boolean> add(String element) {
    return getProxy().add(AddRequest.newBuilder()
        .addValues(element)
        .build())
        .thenApply(response -> response.getAdded());
  }

  @Override
  public CompletableFuture<Boolean> remove(String element) {
    return getProxy().remove(RemoveRequest.newBuilder()
        .addValues(element)
        .build())
        .thenApply(response -> response.getRemoved());
  }

  @Override
  public CompletableFuture<Integer> size() {
    return getProxy().size(SizeRequest.newBuilder().build())
        .thenApply(response -> response.getSize());
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return size().thenApply(size -> size == 0);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return getProxy().clear(ClearRequest.newBuilder().build())
        .thenApply(response -> null);
  }

  @Override
  public CompletableFuture<Boolean> contains(String element) {
    return getProxy().contains(ContainsRequest.newBuilder()
        .addValues(element)
        .build())
        .thenApply(response -> response.getContains());
  }

  @Override
  public CompletableFuture<Boolean> addAll(Collection<? extends String> c) {
    return getProxy().add(AddRequest.newBuilder()
        .addAllValues((Iterable<String>) c)
        .build())
        .thenApply(response -> response.getAdded());
  }

  @Override
  public CompletableFuture<Boolean> containsAll(Collection<? extends String> c) {
    return getProxy().contains(ContainsRequest.newBuilder()
        .addAllValues((Iterable<String>) c)
        .build())
        .thenApply(response -> response.getContains());
  }

  @Override
  public CompletableFuture<Boolean> retainAll(Collection<? extends String> c) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Boolean> removeAll(Collection<? extends String> c) {
    return getProxy().remove(RemoveRequest.newBuilder()
        .addAllValues((Iterable<String>) c)
        .build())
        .thenApply(response -> response.getRemoved());
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(CollectionEventListener<String> listener, Executor executor) {
    if (eventListeners.putIfAbsent(listener, executor) == null) {
      return getProxy().listen(ListenRequest.newBuilder().build())
          .thenApply(response -> null);
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<String> listener) {
    eventListeners.remove(listener);
    if (eventListeners.isEmpty()) {
      return getProxy().unlisten(UnlistenRequest.newBuilder().build())
          .thenApply(response -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public AsyncIterator<String> iterator() {
    return new AsyncDistributedSetIterator();
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<String>> transactionLog) {
    return getProxy().prepare(PrepareRequest.newBuilder()
        .setTransactionId(transactionLog.transactionId().id())
        .setTransaction(DistributedSetTransaction.newBuilder()
            .setVersion(transactionLog.version())
            .addAllUpdates(transactionLog.records().stream()
                .map(update -> DistributedSetUpdate.newBuilder()
                    .setType(DistributedSetUpdate.Type.valueOf(update.type().name()))
                    .setValue(update.element())
                    .build())
                .collect(Collectors.toList()))
            .build())
        .build())
        .thenApply(response -> response.getStatus() == PrepareResponse.Status.OK);
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    return getProxy().commit(CommitRequest.newBuilder()
        .setTransactionId(transactionId.id())
        .build())
        .thenApply(response -> null);
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return getProxy().rollback(RollbackRequest.newBuilder()
        .setTransactionId(transactionId.id())
        .build())
        .thenApply(response -> null);
  }

  @Override
  public DistributedSet<String> sync(Duration operationTimeout) {
    return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
  }

  private class AsyncDistributedSetIterator implements AsyncIterator<String> {
    private final CompletableFuture<Void> init;
    private final AtomicInteger received = new AtomicInteger();
    private final AtomicInteger checked = new AtomicInteger();
    private volatile long index;
    private volatile int size;
    private final Queue<CompletableFuture<String>> queue = new LinkedList<>();
    private final Queue<CompletableFuture<String>> next = new LinkedList<>();

    public AsyncDistributedSetIterator() {
      this.init = getProxy().iterate(IterateRequest.newBuilder().build())
          .thenAccept(response -> {
            index = response.getIndex();
            size = response.getSize();
            if (response.getSize() > 0) {
              iterators.put(response.getIndex(), this);
            }
          });
    }

    private void next(IterateEvent event) {
      CompletableFuture<String> future;
      synchronized (this) {
        future = next.poll();
        if (future == null) {
          future = new CompletableFuture<>();
          queue.add(future);
        }
      }
      future.complete(event.getValue());
      if (received.incrementAndGet() == size) {
        iterators.remove(index);
      }
    }

    @Override
    public CompletableFuture<Boolean> hasNext() {
      return init.thenApply(v -> checked.incrementAndGet() <= size);
    }

    @Override
    public CompletableFuture<String> next() {
      return init.thenCompose(v -> {
        CompletableFuture<String> future;
        synchronized (this) {
          future = queue.poll();
          if (future == null) {
            future = new CompletableFuture<>();
            next.add(future);
          }
        }
        return future;
      });
    }

    @Override
    public CompletableFuture<Void> close() {
      return CompletableFuture.completedFuture(null);
    }
  }
}
