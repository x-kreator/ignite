package com.sbt.cache;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import javax.cache.Cache;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;

/**
 *
 */
public class CacheImpl<K, V> implements Cache<K, V> {
    /** */
    private final String name;

    /** */
    private final CacheManager mgr;

    /** */
    private final CompleteConfiguration<K, V> cfg;

    /** */
    private final ConcurrentMap<K, V> map = new ConcurrentHashMap<>();

    public CacheImpl(String name, CacheManager mgr, CompleteConfiguration<K, V> cfg) {
        this.name = name;
        this.mgr = mgr;
        this.cfg = cfg;
    }

    /** {@inheritDoc} */
    @Override public V get(K key) {
        return map.get(key);
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(Set<? extends K> keys) {
        return new AbstractMap<K, V>() {
            @Override public Set<Entry<K, V>> entrySet() {
                return keys.stream().map(EntryImpl::new).collect(Collectors.toSet());
            }
        };
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionLsnr) {
        CacheLoader<K, V> loader = cfg.getCacheLoaderFactory().create();
        // TODO use CompletableFuture!
    }

    /** {@inheritDoc} */
    @Override public void put(K key, V val) {
        map.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(K key, V val) {
        return map.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> map) {
        this.map.putAll(map);
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) {
        return !map.containsKey(key) && map.putIfAbsent(key, val) == null;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) {
        return map.containsKey(key) && map.remove(key) != null;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V oldVal) {
        return map.remove(key, oldVal);
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(K key) {
        return map.remove(key);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) {
        return map.replace(key, oldVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) {
        return map.replace(key, val) != null;
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(K key, V val) {
        return map.replace(key, val);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Set<? extends K> keys) {
        map.keySet().removeAll(keys);
    }

    /** {@inheritDoc} */
    @Override public void removeAll() {
        map.clear();
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        map.clear();
    }

    @Override public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        return null;
    }

    @Override public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor,
        Object... arguments) throws EntryProcessorException {
        return null;
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor,
        Object... arguments) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    @Override public CacheManager getCacheManager() {
        return mgr;
    }

    @Override public void close() {

    }

    @Override public boolean isClosed() {
        return false;
    }

    @Override public <T> T unwrap(Class<T> clazz) {
        return null;
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryLsnrCfg) {

    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryLsnrCfg) {

    }

    /** {@inheritDoc} */
    @Override public Iterator<Entry<K, V>> iterator() {
        final Iterator<K> keyItr = map.keySet().iterator();

        return new Iterator<Entry<K, V>>() {
            @Override public boolean hasNext() {
                return keyItr.hasNext();
            }

            @Override public Entry<K, V> next() {
                return new EntryImpl(keyItr.next());
            }

            @Override public void remove() {
                keyItr.remove();
            }
        };
    }

    /**
     *
     */
    class EntryImpl implements Entry<K, V>, Map.Entry<K, V> {
        /** */
        private final K key;

        /**
         * @param key Key.
         */
        EntryImpl(K key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public V setValue(V val) {
            return map.put(key, val);
        }

        /** {@inheritDoc} */
        @Override public K getKey() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public V getValue() {
            return map.get(key);
        }

        /** {@inheritDoc} */
        @Override public <T> T unwrap(Class<T> clazz) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(key);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return (obj instanceof CacheImpl.EntryImpl)
                && (cacheImpl() == ((EntryImpl)obj).cacheImpl())
                && Objects.equals(key, ((EntryImpl)obj).key);
        }

        /**
         *
         */
        private CacheImpl<K, V> cacheImpl() {
            return CacheImpl.this;
        }
    }
}
