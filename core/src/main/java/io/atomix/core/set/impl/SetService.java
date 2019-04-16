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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import io.atomix.primitive.session.SessionId;

/**
 * Set service.
 */
public class SetService extends AbstractSetService {
  private Set<String> set = Sets.newConcurrentHashSet();
  private Set<SessionId> listeners = new CopyOnWriteArraySet<>();
  private Set<String> lockedElements = new ConcurrentSkipListSet<>();
  private Map<String, DistributedSetTransaction> transactions = new HashMap<>();

  @Override
  public SizeResponse size(SizeRequest request) {
    return SizeResponse.newBuilder()
        .setIndex(getCurrentIndex())
        .setSize(set.size())
        .build();
  }

  @Override
  public ContainsResponse contains(ContainsRequest request) {
    boolean contains = request.getValuesCount() == 0
        || request.getValuesCount() == 1 ? set.contains(request.getValues(0)) : set.containsAll(request.getValuesList());
    return ContainsResponse.newBuilder()
        .setIndex(getCurrentIndex())
        .setContains(contains)
        .build();
  }

  @Override
  public AddResponse add(AddRequest request) {
    if (request.getValuesCount() == 0) {
      return AddResponse.newBuilder()
          .setIndex(getCurrentIndex())
          .setStatus(UpdateStatus.NOOP)
          .setAdded(false)
          .build();
    } else if (request.getValuesCount() == 1) {
      String value = request.getValues(0);
      if (lockedElements.contains(value)) {
        return AddResponse.newBuilder()
            .setIndex(getCurrentIndex())
            .setStatus(UpdateStatus.WRITE_LOCK)
            .setAdded(false)
            .build();
      } else {
        boolean added = set.add(value);
        return AddResponse.newBuilder()
            .setIndex(getCurrentIndex())
            .setStatus(UpdateStatus.OK)
            .setAdded(added)
            .build();
      }
    } else {
      for (String value : request.getValuesList()) {
        if (lockedElements.contains(value)) {
          return AddResponse.newBuilder()
              .setIndex(getCurrentIndex())
              .setStatus(UpdateStatus.WRITE_LOCK)
              .setAdded(false)
              .build();
        }
      }
      boolean added = set.addAll(request.getValuesList());
      return AddResponse.newBuilder()
          .setIndex(getCurrentIndex())
          .setStatus(UpdateStatus.OK)
          .setAdded(added)
          .build();
    }
  }

  @Override
  public RemoveResponse remove(RemoveRequest request) {
    if (request.getValuesCount() == 0) {
      return RemoveResponse.newBuilder()
          .setIndex(getCurrentIndex())
          .setStatus(UpdateStatus.NOOP)
          .setRemoved(false)
          .build();
    } else if (request.getValuesCount() == 1) {
      String value = request.getValues(0);
      if (lockedElements.contains(value)) {
        return RemoveResponse.newBuilder()
            .setIndex(getCurrentIndex())
            .setStatus(UpdateStatus.WRITE_LOCK)
            .setRemoved(false)
            .build();
      } else {
        boolean removed = set.remove(value);
        return RemoveResponse.newBuilder()
            .setIndex(getCurrentIndex())
            .setStatus(UpdateStatus.OK)
            .setRemoved(removed)
            .build();
      }
    } else {
      for (String value : request.getValuesList()) {
        if (lockedElements.contains(value)) {
          return RemoveResponse.newBuilder()
              .setIndex(getCurrentIndex())
              .setStatus(UpdateStatus.WRITE_LOCK)
              .setRemoved(false)
              .build();
        }
      }
      boolean removed = set.removeAll(request.getValuesList());
      return RemoveResponse.newBuilder()
          .setIndex(getCurrentIndex())
          .setStatus(UpdateStatus.OK)
          .setRemoved(removed)
          .build();
    }
  }

  @Override
  public ClearResponse clear(ClearRequest request) {
    set.clear();
    return ClearResponse.newBuilder()
        .setIndex(getCurrentIndex())
        .build();
  }

  @Override
  public ListenResponse listen(ListenRequest request) {
    listeners.add(getCurrentSession().sessionId());
    return ListenResponse.newBuilder()
        .setIndex(getCurrentIndex())
        .build();
  }

  @Override
  public UnlistenResponse unlisten(UnlistenRequest request) {
    listeners.remove(getCurrentSession().sessionId());
    return UnlistenResponse.newBuilder()
        .setIndex(getCurrentIndex())
        .build();
  }

  @Override
  public IterateResponse iterate(IterateRequest request) {
    SessionId sessionId = getCurrentSession().sessionId();
    long index = getCurrentIndex();
    int size = set.size();
    int position = 0;
    for (String value : set) {
      onIterate(sessionId, IterateEvent.newBuilder()
          .setIndex(index)
          .setValue(value)
          .setPosition(position++)
          .setTotal(size)
          .build());
    }
    return IterateResponse.newBuilder()
        .setIndex(index)
        .setSize(size)
        .build();
  }

  @Override
  public PrepareResponse prepare(PrepareRequest request) {
    for (DistributedSetUpdate update : request.getTransaction().getUpdatesList()) {
      if (lockedElements.contains(update.getValue())) {
        return PrepareResponse.newBuilder()
            .setIndex(getCurrentIndex())
            .setStatus(PrepareResponse.Status.CONCURRENT_TRANSACTION)
            .build();
      }
    }

    for (DistributedSetUpdate update : request.getTransaction().getUpdatesList()) {
      String element = update.getValue();
      switch (update.getType()) {
        case ADD:
        case NOT_CONTAINS:
          if (set.contains(element)) {
            return PrepareResponse.newBuilder()
                .setIndex(getCurrentIndex())
                .setStatus(PrepareResponse.Status.OPTIMISTIC_LOCK_FAILURE)
                .build();
          }
          break;
        case REMOVE:
        case CONTAINS:
          if (!set.contains(element)) {
            return PrepareResponse.newBuilder()
                .setIndex(getCurrentIndex())
                .setStatus(PrepareResponse.Status.OPTIMISTIC_LOCK_FAILURE)
                .build();
          }
          break;
      }
    }

    for (DistributedSetUpdate update : request.getTransaction().getUpdatesList()) {
      lockedElements.add(update.getValue());
    }
    transactions.put(request.getTransactionId(), request.getTransaction());
    return PrepareResponse.newBuilder()
        .setIndex(getCurrentIndex())
        .setStatus(PrepareResponse.Status.OK)
        .build();
  }

  @Override
  public PrepareResponse prepareAndCommit(PrepareRequest request) {
    PrepareResponse response = prepare(request);
    if (response.getStatus() == PrepareResponse.Status.OK) {
      commit(CommitRequest.newBuilder()
          .setTransactionId(request.getTransactionId())
          .build());
    }
    return response;
  }

  @Override
  public CommitResponse commit(CommitRequest request) {
    DistributedSetTransaction transaction = transactions.remove(request.getTransactionId());
    if (transaction == null) {
      return CommitResponse.newBuilder()
          .setIndex(getCurrentIndex())
          .setStatus(CommitResponse.Status.UNKNOWN_TRANSACTION_ID)
          .build();
    }

    for (DistributedSetUpdate update : transaction.getUpdatesList()) {
      switch (update.getType()) {
        case ADD:
          set.add(update.getValue());
          break;
        case REMOVE:
          set.remove(update.getValue());
          break;
        default:
          break;
      }
      lockedElements.remove(update.getValue());
    }
    return CommitResponse.newBuilder()
        .setIndex(getCurrentIndex())
        .setStatus(CommitResponse.Status.OK)
        .build();
  }

  @Override
  public RollbackResponse rollback(RollbackRequest request) {
    DistributedSetTransaction transaction = transactions.remove(request.getTransactionId());
    if (transaction == null) {
      return RollbackResponse.newBuilder()
          .setIndex(getCurrentIndex())
          .setStatus(RollbackResponse.Status.UNKNOWN_TRANSACTION_ID)
          .build();
    }

    for (DistributedSetUpdate update : transaction.getUpdatesList()) {
      lockedElements.remove(update.getValue());
    }
    return RollbackResponse.newBuilder()
        .setIndex(getCurrentIndex())
        .setStatus(RollbackResponse.Status.OK)
        .build();
  }

  @Override
  public void backup(OutputStream output) throws IOException {
    DistributedSetSnapshot.newBuilder()
        .addAllValues(set)
        .addAllListeners(listeners.stream()
            .map(SessionId::id)
            .collect(Collectors.toList()))
        .addAllLockedElements(lockedElements)
        .putAllTransactions(transactions)
        .build()
        .writeTo(output);
  }

  @Override
  public void restore(InputStream input) throws IOException {
    DistributedSetSnapshot snapshot = DistributedSetSnapshot.parseFrom(input);
    set = Sets.newConcurrentHashSet(snapshot.getValuesList());
    listeners = snapshot.getListenersList().stream()
        .map(SessionId::from)
        .collect(Collectors.toCollection(LinkedHashSet::new));
    lockedElements = new CopyOnWriteArraySet<>(snapshot.getLockedElementsList());
    transactions = new HashMap<>(snapshot.getTransactionsMap());
  }
}
