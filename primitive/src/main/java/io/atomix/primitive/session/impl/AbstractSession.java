/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.primitive.session.impl;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.OperationEncoder;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Abstract session.
 */
public abstract class AbstractSession implements Session {
  private final SessionId sessionId;
  private final String primitiveName;
  private final PrimitiveType primitiveType;
  private final MemberId memberId;

  @SuppressWarnings("unchecked")
  protected AbstractSession(
      SessionId sessionId,
      String primitiveName,
      PrimitiveType primitiveType,
      MemberId memberId) {
    this.sessionId = checkNotNull(sessionId);
    this.primitiveName = checkNotNull(primitiveName);
    this.primitiveType = checkNotNull(primitiveType);
    this.memberId = memberId;
  }

  @Override
  public SessionId sessionId() {
    return sessionId;
  }

  @Override
  public String primitiveName() {
    return primitiveName;
  }

  @Override
  public PrimitiveType primitiveType() {
    return primitiveType;
  }

  @Override
  public MemberId memberId() {
    return memberId;
  }

  /**
   * Encodes the given object using the given encoder.
   *
   * @param object  the object to encode
   * @param encoder the event encoder
   * @param <T>     the object type
   * @return the encoded bytes
   */
  protected <T> byte[] encode(T object, OperationEncoder<T> encoder) {
    try {
      return encoder.encode(object);
    } catch (Exception e) {
      throw new PrimitiveException.ServiceException(e);
    }
  }

  @Override
  public abstract void publish(PrimitiveEvent event);

  @Override
  public <T> void publish(EventType eventType, T event, OperationEncoder<T> encoder) {
    publish(PrimitiveEvent.event(eventType, encode(event, encoder)));
  }
}
