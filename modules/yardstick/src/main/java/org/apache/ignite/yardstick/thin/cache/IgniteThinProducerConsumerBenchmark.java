package org.apache.ignite.yardstick.thin.cache;

import java.util.List;
import java.util.Map;
import org.apache.ignite.client.ClientCache;
import org.yardstickframework.BenchmarkConfiguration;

/**
 *
 */
public class IgniteThinProducerConsumerBenchmark extends IgniteThinCacheAbstractBenchmark<String, List<String>> {
    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected ClientCache<String, List<String>> cache() {
        return client().cache("atomic");
    }
}
