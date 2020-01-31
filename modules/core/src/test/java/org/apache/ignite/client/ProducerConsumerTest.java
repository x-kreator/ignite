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
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class ProducerConsumerTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 3;

    /** Cache name. */
    private static final String CACHE_NAME = "test_cache";

    /** Cluster IP pool. */
    private static final String [] CLUSTER_IP_POOL = {"10.36.8.68", "10.36.8.69", "10.36.8.70"};

    /** Client connector address. */
    private static final String CLIENT_CONN_ADDR ="127.0.0.1:" + ClientConnectorConfiguration.DFLT_PORT;

    /** Chars. */
    private static final char[] CHARS = new char[40];

    static {
        "ABCDEFGHI JKLMNOPQR STUVWXYZ0 123456789 ".getChars(0, CHARS.length, CHARS, 0);
    }

    /** Warm-up time. */
    private static final long WARMUP_TIME = 60_000;

    /** Keys. */
    private final String[] keys = generateKeys(1_000_000, 40, 50);

    /** Data. */
    private final byte[][] data = generateData(100_000, 500, 1500);

    /** Messages. */
    private final String[] messages = generateKeys(100_000, 512, 1024);

    /** Keys updated. */
    private final LongAdder keysUpdated = new LongAdder();

    /** Keys read. */
    private final LongAdder keysRead = new LongAdder();

    /** Stopped. */
    private volatile boolean stopped;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration(CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setBackups(1));
    }

    /** {@inheritDoc} */
    /*@Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODES_CNT);

        awaitPartitionMapExchange();
    }*/

    /** {@inheritDoc} */
    /*@Override protected boolean isMultiJvm() {
        return true;
    }*/

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 15 * 60_000;
    }

    /**
     *
     */
    @Test
    public void testThroughput() throws Exception {
        Worker producer = new Worker(this::produce, 0);
        Worker consumer = new Worker(this::consume, 20);

        IgniteInternalFuture<Long> prodFut = GridTestUtils.runMultiThreadedAsync(producer, 32, "producer");

        doSleep(WARMUP_TIME);

        IgniteInternalFuture<Long> consFut = GridTestUtils.runMultiThreadedAsync(consumer, 32, "consumer");

        doSleep(10 * 60_000);

        stopped = true;

        prodFut.get();
        consFut.get();

        log.info("Keys updated/read: " + keysUpdated + "/" + keysRead);
        log.info("Producer stat: " + producer.stat());
        log.info("Consumer stat: " + consumer.stat());
    }

    /**
     *
     */
    @Test
    public void testCacheContent() throws Exception {
        try (IgniteClient client = startClient(CLUSTER_IP_POOL[0])) {
            ClientCache<String, List<String>> cache = client.getOrCreateCache(CACHE_NAME);

            cache.clear();
            log.info("Cache [name=" + cache.getName() + ", size=" + cache.size() + "]");
        }
    }

    /**
     *
     */
    @Test
    public void testThinClientCasOps() throws Exception {
        try (IgniteClient client = startClient(CLUSTER_IP_POOL[0])) {
            ClientCache<String, List<String>> cache = client.getOrCreateCache(CACHE_NAME);

            List<String> val0 = new ArrayList<>();
            val0.add(messages[0]);
            val0.add(messages[2]);
            val0.add(messages[3]);

            cache.put(keys[0], val0);
            List<String> val0a = cache.get(keys[0]);

            List<String> val1 = new ArrayList<>();
            val1.add(messages[4]);
            val1.add(messages[5]);

            boolean replaced = cache.replace(keys[0], val0a, val1);

            log.info("replaced: " + replaced + ", val0a: " + val0a);
        }
    }

    /**
     *
     */
    private IgniteClient startClient(String host) {
        return Ignition.startClient(new ClientConfiguration().setAddresses(host + ":" + ClientConnectorConfiguration.DFLT_PORT));
    }

    /**
     * @param cnt Key count.
     */
    private static String[] generateKeys(int cnt, int minKeySize, int maxKeySize) {
        Random rnd = ThreadLocalRandom.current();

        String[] keys = new String[cnt];

        for (int i = 0; i < keys.length; i++) {
            int keySize = rnd.nextInt(maxKeySize - minKeySize) + minKeySize + 1;

            char[] keyChars = new char[keySize];

            for (int j = 0; j < keySize; j++)
                keyChars[j] = CHARS[rnd.nextInt(CHARS.length)];

            keys[i] = new String(keyChars);
        }

        return keys;
    }

    /**
     * @param cnt Row count.
     * @param minDataSize Min data size.
     * @param maxDataSize Max data size.
     */
    private static byte[][] generateData(int cnt, int minDataSize, int maxDataSize) {
        Random rnd = ThreadLocalRandom.current();

        byte[][] data = new byte[cnt][];

        for (int i = 0; i < data.length; i++) {
            int dataSize = rnd.nextInt(maxDataSize - minDataSize) + minDataSize + 1;

            data[i] = new byte[dataSize];

            rnd.nextBytes(data[i]);
        }

        return data;
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    private void produce(ClientCache<String, List<byte[]>> cache, String key) {
        // String msg = messages[ThreadLocalRandom.current().nextInt(messages.length)];
        byte[] msg = data[ThreadLocalRandom.current().nextInt(data.length)];

        boolean updated = true;

        while (true) {
            List<byte[]> val = cache.get(key);
            List<byte[]> newVal;

            if (val == null)
                newVal = new ArrayList<>();
            else if (val.size() < 20)
                newVal = new ArrayList<>(val);
            else {
                updated = false;

                break;
            }

            newVal.add(msg);

            if (val == null) {
                if (cache.putIfAbsent(key, newVal))
                    break;
            }
            else if (cache.replace(key, val, newVal))
                break;
        }

        if (updated)
            keysUpdated.increment();
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    private void consume(ClientCache<String, List<byte[]>> cache, String key) {
        List<byte[]> messages = cache.getAndRemove(key);

        if (messages != null)
            keysRead.add(messages.size());
    }

    /**
     * @param connAddr Connection address.
     * @param cacheOp Cache operation.
     */
    private <K, V> void doCacheOp(String connAddr, Consumer<ClientCache<K, V>> cacheOp) {
        try (IgniteClient client = startClient(connAddr)) {
            log.info("Client [connAddr=" + connAddr + "] created.");

            ClientCache<K, V> cache = client.getOrCreateCache(CACHE_NAME);

            cacheOp.accept(cache);
        }
        catch (Throwable e) {
            U.error(log, "Unexpected operation error", e);
        }
    }

    /**
     *
     */
    private class Worker implements Runnable {
        /** */
        private final BiConsumer<ClientCache<String, List<byte[]>>, String> proc;
        /** */
        private final long procDelay;
        /** */
        private final AtomicInteger workerCnt = new AtomicInteger();
        /** */
        private final Stat stat = new Stat();
        /** */
        private final long createTime = System.currentTimeMillis();
        /** */
        private int n;

        /**
         *
         */
        private Worker(BiConsumer<ClientCache<String, List<byte[]>>, String> proc, long procDelay) {
            this.proc = proc;
            this.procDelay = procDelay;
        }

        /**
         * @param n N.
         */
        private Consumer<ClientCache<String, List<byte[]>>> nCacheOp(int n) {
            return cache -> {
                int keyIdx = n;

                while (!stopped) {
                    if (procDelay > 0)
                        doSleep(procDelay);

                    long startNanos = System.nanoTime();

                    proc.accept(cache, keys[keyIdx]);

                    if (System.currentTimeMillis() - createTime > WARMUP_TIME)
                        stat.register(System.nanoTime() - startNanos);

                    int wCnt = workerCnt.get();

                    if (keyIdx == n)
                        log.info("Worker #" + (n + 1) + " of " + wCnt + " 1st operation done.");

                    keyIdx += wCnt;

                    if (keyIdx >= keys.length)
                        keyIdx = n;
                }
            };
        }

        /** {@inheritDoc} */
        @Override public void run() {
            n = workerCnt.getAndIncrement();

            log.info("Worker #" + (n + 1) + " started.");

            String connAddr = CLUSTER_IP_POOL[(n + 1) % CLUSTER_IP_POOL.length];

            doCacheOp(connAddr, nCacheOp(n));
        }

        /**
         *
         */
        public Stat stat() {
            return stat;
        }
    }

    /**
     *
     */
    private static class Stat {
        /** */
        private final LongAdder evtCnt = new LongAdder();
        /** */
        private final LongAdder evt10msCnt = new LongAdder();
        /** */
        private final LongAdder evt100msCnt = new LongAdder();

        /**
         * @param nanos Nanos.
         */
        public void register(long nanos) {
            evtCnt.increment();

            if (nanos >= 10_000_000) {
                evt10msCnt.increment();

                if (nanos >= 100_000_000)
                    evt100msCnt.increment();
            }
        }

        /**
         *
         */
        @SuppressWarnings("IntegerDivisionInFloatingPointContext")
        public double get10msPercentage() {
            long evtCnt = this.evtCnt.sum();

            return (10_000 * (evtCnt - evt10msCnt.sum()) / evtCnt) / 100.0;
        }

        /**
         *
         */
        @SuppressWarnings("IntegerDivisionInFloatingPointContext")
        public double get100msPercentage() {
            long evtCnt = this.evtCnt.sum();

            return (10_000 * (evtCnt - evt100msCnt.sum()) / evtCnt) / 100.0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Stat [" +
                "evtCnt=" + evtCnt.sum() +
                ", evt10msCnt=" + evt10msCnt.sum() +
                ", evt100msCnt=" + evt100msCnt.sum() +
                ", <10ms=" + get10msPercentage() +
                ", <100ms=" + get100msPercentage() +
                ']';
        }
    }
}
