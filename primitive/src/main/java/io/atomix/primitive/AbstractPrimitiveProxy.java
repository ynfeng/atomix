package io.atomix.primitive;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.atomix.primitive.session.SessionClient;
import io.atomix.utils.concurrent.ThreadContext;

/**
 * Base class for primitive proxies.
 */
public abstract class AbstractPrimitiveProxy {
  private final SessionClient client;

  public AbstractPrimitiveProxy(SessionClient client) {
    this.client = client;
  }

  /**
   * Returns the primitive name.
   *
   * @return the primitive name
   */
  public String name() {
    return client.name();
  }

  /**
   * Returns the primitive type.
   *
   * @return the primitive type
   */
  public PrimitiveType type() {
    return client.type();
  }

  /**
   * Returns the proxy thread context.
   *
   * @return the proxy thread context
   */
  public ThreadContext context() {
    return client.context();
  }

  /**
   * Returns the primitive client.
   *
   * @return the primitive client
   */
  protected SessionClient getClient() {
    return client;
  }

  /**
   * Connects the primitive.
   *
   * @return a future to be completed once the primitive has been connected
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> connect() {
    return client.connect().thenApply(v -> null);
  }

  /**
   * Closes the primitive.
   *
   * @return a future to be completed once the primitive has been closed
   */
  public CompletableFuture<Void> close() {
    return client.close();
  }

  /**
   * Deletes the primitive.
   *
   * @return a future to be completed once the primitive has been deleted
   */
  public CompletableFuture<Void> delete() {
    return client.delete();
  }

  /**
   * Returns the current primitive state.
   *
   * @return the current primitive state
   */
  public PrimitiveState getState() {
    return client.getState();
  }

  /**
   * Adds a state change listener to the proxy.
   *
   * @param listener the listener to add
   */
  public void onStateChange(Consumer<PrimitiveState> listener) {
    client.addStateChangeListener(listener);
  }
}
