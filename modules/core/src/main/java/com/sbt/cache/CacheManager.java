package com.sbt.cache;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import javax.cache.configuration.Configuration;

/**
 *
 */
public class CacheManager implements javax.cache.CacheManager{
    /** */
    private final CachingProvider cachingProvider;

    /** */
    private final URI uri;

    /** */
    private final ClassLoader clsLdr;

    /** */
    private final Properties props;

    /** */
    private final Map<String, Cache> caches = new ConcurrentHashMap<>();

    /** */
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * @param cachingProvider Caching provider.
     * @param uri Uri.
     * @param clsLdr Class loader.
     * @param props Properties.
     */
    public CacheManager(CachingProvider cachingProvider, URI uri, ClassLoader clsLdr, Properties props) {
        this.cachingProvider = cachingProvider;
        this.uri = uri;
        this.clsLdr = clsLdr;
        this.props = props == null ? new Properties() : props;
    }

    /** {@inheritDoc} */
    @Override public CachingProvider getCachingProvider() {
        return cachingProvider;
    }

    /** {@inheritDoc} */
    @Override public URI getURI() {
        return uri;
    }

    /** {@inheritDoc} */
    @Override public ClassLoader getClassLoader() {
        return clsLdr;
    }

    /** {@inheritDoc} */
    @Override public Properties getProperties() {
        return props;
    }

    @Override public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C cfg)
        throws IllegalArgumentException {
        // TODO create cache here!
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valType) {
        Cache<K, V> cache = getCache(cacheName);

        if (cache != null) {
            if(!keyType.isAssignableFrom(cache.getConfiguration(Configuration.class).getKeyType()))
                throw new ClassCastException();

            if(!valType.isAssignableFrom(cache.getConfiguration(Configuration.class).getValueType()))
                throw new ClassCastException();
        }

        return cache;
    }

    /** {@inheritDoc} */
    @Override public <K, V> Cache<K, V> getCache(String cacheName) {
        return (Cache<K, V>)caches.get(cacheName);
    }

    /** {@inheritDoc} */
    @Override public Iterable<String> getCacheNames() {
        return Collections.unmodifiableSet(caches.keySet());
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(String cacheName) {
        Cache cache = caches.remove(cacheName);

        if (cache != null)
            cache.close(); // TODO clear before?
    }

    @Override public void enableManagement(String cacheName, boolean enabled) {
        // TODO
    }

    @Override public void enableStatistics(String cacheName, boolean enabled) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (!closed.compareAndSet(false, true))
            return;

        for (Cache cache : caches.values()) {
            try {
                cache.close();
            }
            catch (Exception ignored) {
                // No-op
            }
        }

        caches.clear();
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return closed.get();
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(getClass()))
            return clazz.cast(this);

        throw new IllegalArgumentException();
    }
}
