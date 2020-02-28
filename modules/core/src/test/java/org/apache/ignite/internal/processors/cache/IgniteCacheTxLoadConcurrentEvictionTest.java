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
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedSingleGetFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
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
    private static final int KEY_RANGE = 1_000_000
        ;
    /** */
    private boolean clientMode;
    /** */
    private volatile int keyType = 1 // 0 - sequental, 1 - random, 2 - saved random
        ;
    /** */
    private volatile boolean stopLoad;
    /** */
    private final AtomicInteger txLoadAliveThreads = new AtomicInteger();
    /** */
    private final LongAdder totalTxs = new LongAdder();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setClientMode(clientMode);

        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setCacheMode(PARTITIONED)
            .setAtomicityMode(TRANSACTIONAL)
            .setWriteSynchronizationMode(PRIMARY_SYNC)
            .setBackups(1)
            ;

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

        //randomKeys = false;

        IgniteEx ignite3 = startGrid(3);
        LT.info(log, ">>> Node started: " + ignite3.name());

        //IgniteEx ignite4 = startGrid(4);
        //LT.info(log, ">>> Node started: " + ignite4.name());

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
            LT.info(log, "getRequestTrackRoutes: " + GridPartitionedSingleGetFuture.getRequestTrackRoutes);
            LT.info(log, "Total txs: " + totalTxs.sum());
            LT.info(log, "SingleGetRequests created/processed: " +
                GridPartitionedSingleGetFuture.singleGetRequestsCreated.sum() + "/" +
                GridDhtPartitionTopologyImpl.locPart0ReqCnt.sum());

            if (keyType == 1) {
                int[] keys1 = new int[rndSeq1Idx.get()];
                int[] keys2 = new int[rndSeq2Idx.get()];

                System.arraycopy(rndSeq1, 0, keys1, 0, keys1.length);
                System.arraycopy(rndSeq2, 0, keys2, 0, keys2.length);

                LT.info(log, Arrays.toString(keys1));
                LT.info(log, Arrays.toString(keys2));
            }
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

                totalTxs.increment();
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
        int key = nextKey1();

        Object val = cache.get(key);

        if (val != null)
            key = nextKey2();

        cache.put(key, new SampleValue(key));
    }

    /** */
    private final ThreadLocal<CyclicSequence> seq1 = ThreadLocal.withInitial(() -> new CyclicSequence(0, KEY_RANGE / 2));

    /** */
    private final ThreadLocal<CyclicSequence> seq2 = ThreadLocal.withInitial(() -> new CyclicSequence(KEY_RANGE / 2, KEY_RANGE));

    /** */
    private final int[] rndSeq1 = new int[KEY_RANGE / 2];
    /** */
    private final int[] rndSeq2 = new int[KEY_RANGE / 2];

    /** */
    private final AtomicInteger rndSeq1Idx = new AtomicInteger();
    /** */
    private final AtomicInteger rndSeq2Idx = new AtomicInteger();

    /**
     *
     */
    private int nextKey1() {
        switch (keyType) {
            case 0:
                return seq1.get().next();

            case 1:
                return rndSeq1[rndSeq1Idx.getAndIncrement()] = nextRandom(0, KEY_RANGE / 2);

            /*case 2:
                int i = rndSeq1Idx.getAndIncrement();

                if (i >= SAVED_RANDOM_KEYS1.length)
                    rndSeq1Idx.set(i = 0);

                return SAVED_RANDOM_KEYS1[i];*/

            default:
                throw new IllegalStateException("Invalid keyType: " + keyType);
        }
    }

    /**
     *
     */
    private int nextKey2() {
        switch (keyType) {
            case 0:
                return seq2.get().next();

            case 1:
                return rndSeq2[rndSeq2Idx.getAndIncrement()] = nextRandom(KEY_RANGE / 2, KEY_RANGE);

            /*case 2:
                int i = rndSeq2Idx.getAndIncrement();

                if (i >= SAVED_RANDOM_KEYS2.length)
                    rndSeq2Idx.set(i = 0);

                return SAVED_RANDOM_KEYS2[i];*/

            default:
                throw new IllegalStateException("Invalid keyType: " + keyType);
        }
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

    /**
     *
     */
    private static class CyclicSequence {
        /** */
        private final AtomicInteger val = new AtomicInteger();
        /** */
        private final int min;
        /** */
        private final int max;

        /**
         * @param min Min.
         * @param max Max.
         */
        private CyclicSequence(int min, int max) {
            this.min = min;
            this.max = max;

            val.set(min);
        }

        /**
         *
         */
        public int next() {
            while (true) {
                int v = val.get();

                if (val.compareAndSet(v, v == max ? min : v + 1))
                    return v;
            }
        }
    }
}
