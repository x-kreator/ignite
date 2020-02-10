/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import javax.cache.Cache;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class ProducerConsumerOperations<K, V> {
    /**
     * @param cache Cache.
     * @param key Key.
     * @return {@code true} if key was added or updated, {@code false} otherwise.
     */
    public boolean produce(CacheOps<K, List<V>> cache, K key, Supplier<V> valSupplier) {
        V val = valSupplier.get();

        boolean updated = true;

        while (true) {
            List<V> oldVal = cache.get(key);
            List<V> newVal;

            if (oldVal == null)
                newVal = new ArrayList<>();
            else if (oldVal.size() < 20)
                newVal = new ArrayList<>(oldVal);
            else {
                updated = false;

                break;
            }

            newVal.add(val);

            if (oldVal == null) {
                if (cache.putIfAbsent(key, newVal))
                    break;
            }
            else if (cache.replace(key, oldVal, newVal))
                break;
        }

        return updated;
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @return List of messages read.
     */
    @NotNull public List<V> consume(CacheOps<K, List<V>> cache, K key) {
        List<V> messages = cache.getAndRemove(key);

        return messages != null ? messages : Collections.emptyList();
    }

    /**
     * @param cache Cache.
     */
    public static <K, V> CacheOps<K, V> ofCache(Cache<K, V> cache) {
        return new CacheOps<K, V>() {
            @Override public V get(K key) {
                return cache.get(key);
            }

            @Override public V getAndRemove(K key) {
                return cache.getAndRemove(key);
            }

            @Override public boolean putIfAbsent(K key, V val) {
                return cache.putIfAbsent(key, val);
            }

            @Override public boolean replace(K key, V oldVal, V newVal) {
                return cache.replace(key, oldVal, newVal);
            }
        };
    }

    /**
     * @param clientCache Client cache.
     */
    public static <K, V> CacheOps<K, V> ofClientCache(ClientCache<K, V> clientCache) {
        return new CacheOps<K, V>() {
            @Override public V get(K key) {
                return clientCache.get(key);
            }

            @Override public V getAndRemove(K key) {
                return clientCache.getAndRemove(key);
            }

            @Override public boolean putIfAbsent(K key, V val) {
                return clientCache.putIfAbsent(key, val);
            }

            @Override public boolean replace(K key, V oldVal, V newVal) {
                return clientCache.replace(key, oldVal, newVal);
            }
        };
    }

    /**
     * @param <K>
     * @param <V>
     */
    public static interface CacheOps<K, V> {
        /**
         * Gets an entry from the cache.
         *
         * @param key the key whose associated value is to be returned
         * @return the element, or null, if it does not exist.
         */
        public V get(K key);

        /**
         * Atomically removes the entry for a key only if currently mapped to some value.
         *
         * @param key Key with which the specified value is associated.
         * @return The value if one existed or null if no mapping existed for this key.
         */
        public V getAndRemove(K key);

        /**
         * Atomically associates the specified key with the given value if it is not already associated with a value.
         *
         * @param key Key with which the specified value is to be associated.
         * @param val Value to be associated with the specified key.
         * @return <tt>true</tt> if a value was set.
         */
        public boolean putIfAbsent(K key, V val);

        /**
         * Atomically replaces the entry for a key only if currently mapped to a given value.
         *
         * @param key Key with which the specified value is associated.
         * @param oldVal Value expected to be associated with the specified key.
         * @param newVal Value to be associated with the specified key.
         * @return <tt>true</tt> if the value was replaced
         */
        public boolean replace(K key, V oldVal, V newVal);
    }
}
