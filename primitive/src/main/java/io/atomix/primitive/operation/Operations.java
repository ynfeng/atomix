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
package io.atomix.primitive.operation;

import io.atomix.utils.Version;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Operation utilities.
 */
public final class Operations {

  /**
   * Returns the collection of operations provided by the given service interface.
   *
   * @param serviceInterface the service interface
   * @return the operations provided by the given service interface
   */
  @Deprecated
  public static Map<Method, OperationId> getMethodMap(Class<?> serviceInterface) {
    if (!serviceInterface.isInterface()) {
      Map<Method, OperationId> operations = new HashMap<>();
      for (Class<?> iface : serviceInterface.getInterfaces()) {
        operations.putAll(findOperationIds(iface));
      }
      return operations;
    }
    return findOperationIds(serviceInterface);
  }

  /**
   * Returns the collection of operations provided by the given service interface.
   *
   * @param serviceInterface the service interface
   * @return the operations provided by the given service interface
   */
  public static Map<Method, OperationInfo> getMethodOperationInfo(Class<?> serviceInterface) {
    if (!serviceInterface.isInterface()) {
      Map<Method, OperationInfo> operations = new HashMap<>();
      for (Class<?> iface : serviceInterface.getInterfaces()) {
        operations.putAll(findOperationInfo(iface));
      }
      return operations;
    }
    return findOperationInfo(serviceInterface);
  }

  /**
   * Recursively finds operations defined by the given type and its implemented interfaces.
   *
   * @param type the type for which to find operations
   * @return the operations defined by the given type and its parent interfaces
   */
  private static Map<Method, OperationInfo> findOperationInfo(Class<?> type) {
    Map<Method, OperationInfo> operations = new HashMap<>();
    for (Method method : type.getDeclaredMethods()) {
      OperationInfo operationInfo = getOperationInfo(method);
      if (operationInfo != null) {
        if (operations.values().stream().anyMatch(operation -> operation.operationId().id().equals(operationInfo.operationId().id()))) {
          throw new IllegalStateException("Duplicate operation name '" + operationInfo.operationId().id() + "'");
        }
        operations.put(method, operationInfo);
      }
    }
    for (Class<?> iface : type.getInterfaces()) {
      operations.putAll(findOperationInfo(iface));
    }
    return operations;
  }

  /**
   * Recursively finds operations defined by the given type and its implemented interfaces.
   *
   * @param type the type for which to find operations
   * @return the operations defined by the given type and its parent interfaces
   */
  private static Map<Method, OperationId> findOperationIds(Class<?> type) {
    return findOperationInfo(type)
        .entrySet()
        .stream()
        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().operationId()));
  }

  /**
   * Returns the collection of operations provided by the given service interface.
   *
   * @param serviceInterface the service interface
   * @return the operations provided by the given service interface
   */
  @Deprecated
  public static Map<OperationId, Method> getOperationMap(Class<?> serviceInterface) {
    if (!serviceInterface.isInterface()) {
      Class type = serviceInterface;
      Map<OperationId, Method> operations = new HashMap<>();
      while (type != Object.class) {
        for (Class<?> iface : type.getInterfaces()) {
          operations.putAll(findMethods(iface));
        }
        type = type.getSuperclass();
      }
      return operations;
    }
    return findMethods(serviceInterface);
  }

  /**
   * Returns the collection of operations provided by the given service interface.
   *
   * @param serviceInterface the service interface
   * @return the operations provided by the given service interface
   */
  public static Map<OperationId, MethodInfo> getOperationMethodInfo(Class<?> serviceInterface) {
    if (!serviceInterface.isInterface()) {
      Class type = serviceInterface;
      Map<OperationId, MethodInfo> operations = new HashMap<>();
      while (type != Object.class) {
        for (Class<?> iface : type.getInterfaces()) {
          operations.putAll(findMethodInfo(iface));
        }
        type = type.getSuperclass();
      }
      return operations;
    }
    return findMethodInfo(serviceInterface);
  }

  /**
   * Recursively finds operations defined by the given type and its implemented interfaces.
   *
   * @param type the type for which to find operations
   * @return the operations defined by the given type and its parent interfaces
   */
  private static Map<OperationId, MethodInfo> findMethodInfo(Class<?> type) {
    Map<OperationId, MethodInfo> operations = new HashMap<>();
    for (Method method : type.getDeclaredMethods()) {
      OperationInfo operationInfo = getOperationInfo(method);
      if (operationInfo != null) {
        if (operations.keySet().stream().anyMatch(operation -> operation.id().equals(operationInfo.operationId().id()))) {
          throw new IllegalStateException("Duplicate operation name '" + operationInfo.operationId().id() + "'");
        }
        operations.put(operationInfo.operationId(), new MethodInfo(operationInfo.operationId(), operationInfo.version(), method));
      }
    }
    for (Class<?> iface : type.getInterfaces()) {
      operations.putAll(findMethodInfo(iface));
    }
    return operations;
  }

  /**
   * Recursively finds operations defined by the given type and its implemented interfaces.
   *
   * @param type the type for which to find operations
   * @return the operations defined by the given type and its parent interfaces
   */
  private static Map<OperationId, Method> findMethods(Class<?> type) {
    return findMethodInfo(type)
        .entrySet()
        .stream()
        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().method()));
  }

  /**
   * Returns the operation ID for the given method.
   *
   * @param method the method for which to lookup the operation ID
   * @return the operation ID for the given method or null if the method is not annotated
   */
  private static OperationInfo getOperationInfo(Method method) {
    Command command = method.getAnnotation(Command.class);
    if (command != null) {
      String name = command.value().equals("") ? method.getName() : command.value();
      return new OperationInfo(OperationId.from(name, OperationType.COMMAND), Version.from(command.since()));
    }
    Query query = method.getAnnotation(Query.class);
    if (query != null) {
      String name = query.value().equals("") ? method.getName() : query.value();
      return new OperationInfo(OperationId.from(name, OperationType.QUERY), Version.from(query.since()));
    }
    Operation operation = method.getAnnotation(Operation.class);
    if (operation != null) {
      String name = operation.value().equals("") ? method.getName() : operation.value();
      return new OperationInfo(OperationId.from(name, operation.type()), Version.from(operation.since()));
    }
    return null;
  }

  private Operations() {
  }
}
