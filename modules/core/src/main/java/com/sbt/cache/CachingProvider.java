package com.sbt.cache;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;
import javax.cache.configuration.OptionalFeature;

/**
 * Implementation of JSR-107 {@link javax.cache.spi.CachingProvider}.
 */
public class CachingProvider implements javax.cache.spi.CachingProvider {
    /** */
    public static final URI DEFAULT_URI = URI.create("cache://default");

    /** */
    public static final Properties DEFAULT_PROPS = new Properties();

    /** */
    private final Map<ClassLoader, Map<URI, CacheManager>> cacheManagers = new WeakHashMap<>();

    /** {@inheritDoc} */
    @Override public CacheManager getCacheManager(URI uri, ClassLoader clsLdr, Properties props) {
        if (uri == null)
            uri = getDefaultURI();

        if (clsLdr == null)
            clsLdr = getDefaultClassLoader();

        Map<URI, CacheManager> uriMap;

        synchronized (cacheManagers) {
            uriMap = cacheManagers.computeIfAbsent(clsLdr, k -> new HashMap<>());

            CacheManager cacheMgr = uriMap.get(uri);

            if (cacheMgr == null) {
                cacheMgr = new CacheManager(this, uri, clsLdr, props);

                uriMap.put(uri, cacheMgr);
            }

            return cacheMgr;
        }
    }

    /** {@inheritDoc} */
    @Override public ClassLoader getDefaultClassLoader() {
        return getClass().getClassLoader();
    }

    /** {@inheritDoc} */
    @Override public URI getDefaultURI() {
        return DEFAULT_URI;
    }

    /** {@inheritDoc} */
    @Override public Properties getDefaultProperties() {
        return DEFAULT_PROPS;
    }

    /** {@inheritDoc} */
    @Override public javax.cache.CacheManager getCacheManager(URI uri, ClassLoader clsLdr) {
        return getCacheManager(uri, clsLdr, getDefaultProperties());
    }

    /** {@inheritDoc} */
    @Override public javax.cache.CacheManager getCacheManager() {
        return getCacheManager(getDefaultURI(), getDefaultClassLoader());
    }

    /** {@inheritDoc} */
    @Override public void close() {
        List<CacheManager> cacheMgrList = new ArrayList<>();

        synchronized (cacheManagers) {
            cacheManagers.values().forEach(uriMap -> cacheMgrList.addAll(uriMap.values()));
            cacheManagers.clear();
        }

        cacheMgrList.forEach(CacheManager::close);
    }

    /** {@inheritDoc} */
    @Override public void close(ClassLoader clsLdr) {
        Map<URI, CacheManager> uriMap;

        synchronized (cacheManagers) {
            uriMap = cacheManagers.remove(clsLdr);

            if (uriMap == null)
                return;
        }

        uriMap.values().forEach(CacheManager::close);
    }

    /** {@inheritDoc} */
    @Override public void close(URI uri, ClassLoader clsLdr) {
        CacheManager cacheMgr;

        synchronized (cacheManagers) {
            Map<URI, CacheManager> uriMap = cacheManagers.get(clsLdr);

            if (uriMap == null)
                return;

            cacheMgr = uriMap.remove(uri);

            if (cacheMgr == null)
                return;

            if (uriMap.isEmpty())
                cacheManagers.remove(clsLdr);
        }

        cacheMgr.close();
    }

    /** {@inheritDoc} */
    @Override public boolean isSupported(OptionalFeature optionalFeature) {
        return false;
    }
}
