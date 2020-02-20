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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class IgniteCacheTxLoadConcurrentEvictionTest extends GridCommonAbstractTest {
    /** */
    private static final ThreadLocal<IgniteCache<Integer, Object>> TL_CACHE = new ThreadLocal<>();
    /** */
    private static final int KEY_RANGE = 1_000_000;
    /** */
    private boolean clientMode;
    /** */
    private volatile boolean stopLoad;
    /** */
    private final AtomicInteger txLoadAliveThreads = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setClientMode(clientMode);

        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setCacheMode(PARTITIONED)
            .setAtomicityMode(TRANSACTIONAL)
            .setWriteSynchronizationMode(PRIMARY_SYNC)
            .setBackups(1);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testTxLoadConcurrentEviction() throws Exception {
        IgniteEx ignite1 = startGrid(1);
        IgniteEx ignite2 = startGrid(2);

        clientMode = true;

        IgniteEx[] clients = new IgniteEx[4];

        for (int i = 0; i < clients.length; i++)
            clients[i] = startGrid(i + 10);

        clientMode = false;

        GridCompoundIdentityFuture<Long> txLoadFinishFut = new GridCompoundIdentityFuture<>();

        for (int i = 0; i < clients.length; i++) {
            IgniteEx c = clients[i];

            txLoadFinishFut.add(GridTestUtils.runMultiThreadedAsync(() -> txLoadCycle(c), 64, "tx-load-" + i));
        }

        txLoadFinishFut.markInitialized();

        doSleep(10_000);

        LT.info(log, ">>> Half test time passed...");

        IgniteEx ignite3 = startGrid(3);
        IgniteEx ignite4 = startGrid(4);

        doSleep(10_000);

        stopLoad = true;

        try {
            txLoadFinishFut.get(5_000);
        }
        catch (IgniteFutureTimeoutCheckedException e) {
            LT.warn(log, "!!! Some txs still not been finished!");
        }
        finally {
            LT.info(log, "Tx load alive threads: " + txLoadAliveThreads);
        }
    }

    /**
     * @param client Client.
     */
    private void txLoadCycle(IgniteEx client) {
        txLoadAliveThreads.incrementAndGet();

        try {
            txLoadCycle0(client);
        }
        finally {
            txLoadAliveThreads.decrementAndGet();
        }
    }

    /**
     * @param client Client.
     */
    private void txLoadCycle0(IgniteEx client) {
        IgniteCache<Integer, Object> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        Callable<Void> clo = () -> {
            cacheOp(cache);

            return null;
        };

        while (!stopLoad) {
            try {
                doInTransaction(client, OPTIMISTIC, SERIALIZABLE, clo);
            }
            catch (TransactionOptimisticException ignored) {
                // No-op
            }
            catch (Exception e) {
                U.error(log, "Operation error", e);
            }
        }
    }

    /**
     * @param cache Cache.
     */
    private void cacheOp(IgniteCache<Integer, Object> cache) {
        int key = nextRandom(0, KEY_RANGE / 2);

        Object val = cache.get(key);

        if (val != null)
            key = nextRandom(KEY_RANGE / 2, KEY_RANGE);

        cache.put(key, new SampleValue(key));
    }

    /**
     * @param min Minimum key in range.
     * @param max Maximum key in range.
     * @return Next key.
     */
    protected static int nextRandom(int min, int max) {
        return ThreadLocalRandom.current().nextInt(max - min) + min;
    }

    /**
     *
     */
    private static class SampleValue implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        private SampleValue(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof SampleValue && ((SampleValue)obj).val == val;
        }
    }
}
