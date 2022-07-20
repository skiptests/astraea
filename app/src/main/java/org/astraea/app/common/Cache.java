/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.common;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Cache<K, V> {

  @FunctionalInterface
  public interface CacheLoader<K, V> {
    V load(K key);
  }

  @FunctionalInterface
  public interface RemovalListener<K, V> {
    void onRemove(K k, V v);
  }

  public static <K, V> CacheBuilder<K, V> builder(CacheLoader<? super K, V> cacheLoader) {
    return new CacheBuilder<>(cacheLoader);
  }

  public static final class CacheBuilder<A, B> {
    private final CacheLoader<? super A, B> cacheLoader;
    private long expireAfterAccessNanos = -1;
    private int maxCapacity = -1;
    private RemovalListener<? super A, ? super B> removalListener =
        (RemovalListener<A, B>) (a, b) -> {};

    private CacheBuilder(CacheLoader<? super A, B> cacheLoader) {
      this.cacheLoader = requireNonNull(cacheLoader);
    }

    public CacheBuilder<A, B> expireAfterAccess(Duration duration) {
      this.expireAfterAccessNanos = duration.toNanos();
      return this;
    }

    public CacheBuilder<A, B> maxCapacity(int maxCapacity) {
      this.maxCapacity = maxCapacity;
      return this;
    }

    public CacheBuilder<A, B> removalListener(
        RemovalListener<? super A, ? super B> removalListener) {
      this.removalListener = requireNonNull(removalListener);
      return this;
    }

    public Cache<A, B> build() {
      return new Cache<>(this);
    }
  }

  private final Map<K, Wrapper<V>> map = new HashMap<>();
  private final CacheLoader<? super K, V> cacheLoader;
  private final long expireAfterAccessNanos;
  private final int maxCapacity;
  private final RemovalListener<? super K, ? super V> removalListener;

  private Cache(CacheBuilder<K, V> builder) {
    this.cacheLoader = builder.cacheLoader;
    this.expireAfterAccessNanos = builder.expireAfterAccessNanos;
    this.maxCapacity = builder.maxCapacity;
    this.removalListener = builder.removalListener;
  }

  public synchronized V get(K key) {
    cleanup();
    return map.computeIfAbsent(
            key,
            k -> {
              if (maxCapacity == map.size()) {
                throw new RuntimeException("cache is full");
              }
              return new Wrapper<>(cacheLoader.load((k)));
            })
        .get();
  }

  public synchronized int size() {
    return map.size();
  }

  private void cleanup() {
    map.entrySet().stream()
        .filter(e -> e.getValue().isExpired(System.nanoTime()))
        .collect(Collectors.toList())
        .forEach(
            entry -> {
              try {
                removalListener.onRemove(entry.getKey(), entry.getValue().get());
              } catch (Throwable err) {
                System.err.printf("Exception thrown by removal listener, %s%n", err.getMessage());
              }
              map.remove(entry.getKey());
            });
  }

  private class Wrapper<T> {
    private final T value;

    private long timestamp;

    Wrapper(T value) {
      this.value = requireNonNull(value);
      this.timestamp = System.nanoTime();
    }

    T get() {
      this.timestamp = System.nanoTime();
      return value;
    }

    boolean isExpired(long now) {
      return expireAfterAccessNanos > 0 && (now - timestamp >= expireAfterAccessNanos);
    }
  }
}
