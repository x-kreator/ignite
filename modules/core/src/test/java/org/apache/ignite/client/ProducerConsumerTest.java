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
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
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
    private static final String [] CLUSTER_IP_POOL = {"10.36.8.68", /*"10.36.8.69",*/ "10.36.8.70"};

    /** Client connector address. */
    private static final String CLIENT_CONN_ADDR ="127.0.0.1:" + ClientConnectorConfiguration.DFLT_PORT;

    /** Operations. */
    private ProducerConsumerOperations<String, String> ops = new ProducerConsumerOperations<>();

    /** Data. */
    private RandomStringData data = new RandomStringData();

    /** Keys updated. */
    private final LongAdder keysUpdated = new LongAdder();

    /** Keys read. */
    private final LongAdder keysRead = new LongAdder();

    /** Messages lost. */
    private final LongAdder msgsLost = new LongAdder();

    /** Producer thread count. */
    private int prodThreadNum = 32;

    /** Consumer thread count. */
    private int consThreadNum = 8;

    /** Producer process delay. */
    private long prodProcDelay = 0;

    /** Consumer process delay. */
    private long consProcDelay = 20;

    /** Warm-up time. */
    private long warmUpTime = 100;

    /** Benchmark time. */
    private long benchmarkTime = 30_000;

    /** Is node client. */
    private boolean isNodeClient = false;

    /** Is local cluster. */
    private boolean isLocCluster = false;

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
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        if (isLocCluster) {
            startGrids(NODES_CNT);

            awaitPartitionMapExchange();
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return false; // isLocCluster;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 15 * 60_000;
    }

    /**
     *
     */
    @Test
    public void testThroughput() throws Exception {
        if (isNodeClient) {
            String ip = isLocCluster ? "127.0.0.1" : CLUSTER_IP_POOL[0];

            /*IgniteBiTuple<IgniteConfiguration, GridSpringResourceContext> xmlCfg =
                IgnitionEx.loadConfiguration("D:/dev/_files/ign-267/config-server.xml");*/

            startGrid(/*xmlCfg.get1()*/ getConfiguration("client")
                .setClientMode(true)
                .setDiscoverySpi(new TcpDiscoverySpi()
                    .setIpFinder(new TcpDiscoveryVmIpFinder()
                        .setAddresses(Collections.singletonList(ip + ":47500..47509")))));
        }

        Worker producer = new Worker(this::produce, prodProcDelay);
        Worker consumer = new Worker(this::consume, consProcDelay);

        IgniteInternalFuture<Long> prodFut = GridTestUtils.runMultiThreadedAsync(producer, prodThreadNum, "producer");

        doSleep(warmUpTime);

        IgniteInternalFuture<Long> consFut = GridTestUtils.runMultiThreadedAsync(consumer, consThreadNum, "consumer");

        doSleep(benchmarkTime);

        stopped = true;

        prodFut.get();
        consFut.get();

        log.info("Threads of producer/consumer: " + prodThreadNum + "/" + consThreadNum);
        log.info("warmUp/benchmark time: " + warmUpTime + "/" + benchmarkTime + " ms");
        log.info("Keys updated/read/lost: " + keysUpdated + "/" + keysRead + "/" + msgsLost);
        log.info("Producer stat: " + producer.stat());
        log.info("Consumer stat: " + consumer.stat());

        long totalTps = producer.stat().evtCnt.sum() / (benchmarkTime / 1000);
        log.info("Updates per second, total/thread: " + totalTps + "/" + (totalTps / prodThreadNum));
    }

    /**
     *
     */
    @Test
    public void testCacheContent() throws Exception {
        try (IgniteClient client = startClient(CLUSTER_IP_POOL[0])) {
            ClientCache<String, List<String>> cache = client.getOrCreateCache(CACHE_NAME);

            log.info("Cache [name=" + cache.getName() + ", size=" + cache.size() + "]");
            cache.clear();
            log.info("Cache [name=" + cache.getName() + ", size=" + cache.size() + "]");
        }
    }

    /**
     * @param cache Cache.
     * @param valFn Value fn.
     */
    private <V> void doThinClientReplaceList(ClientCache<String, List<V>> cache, Function<Integer, V> valFn) {
        String key = "key";

        List<V> val0 = new ArrayList<>();
        val0.add(valFn.apply(1));
        val0.add(valFn.apply(2));
        val0.add(valFn.apply(3));

        cache.put(key, val0);
        List<V> val0a = cache.get(key);

        List<V> val1 = new ArrayList<>();
        val1.add(valFn.apply(4));
        val1.add(valFn.apply(5));

        boolean replaced = cache.replace(key, val0a, val1);

        log.info("replaced: " + replaced + ", val0a: " + val0a);
    }

    /**
     *
     */
    private IgniteClient startClient(String host) {
        return Ignition.startClient(new ClientConfiguration().setAddresses(host + ":" + ClientConnectorConfiguration.DFLT_PORT));
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
    private void produce(ProducerConsumerOperations.CacheOps<String, List<String>> cache, String key) {
        if (ops.produce(cache, key, data::randomMessage))
            keysUpdated.increment();
        else
            msgsLost.increment();
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    private void consume(ProducerConsumerOperations.CacheOps<String, List<String>> cache, String key) {
        keysRead.add(ops.consume(cache, key));
    }

    /**
     * @param connAddr Connection address.
     * @param cacheOp Cache operation.
     */
    private <K, V> void doCacheOp(String connAddr, Consumer<ProducerConsumerOperations.CacheOps<K, V>> cacheOp) {
        try (IgniteClient client = startClient(connAddr)) {
            log.info("Client [connAddr=" + connAddr + "] created.");

            ClientCache<K, V> cache = client.getOrCreateCache(CACHE_NAME);

            cacheOp.accept(ProducerConsumerOperations.ofClientCache(cache));
        }
        catch (Throwable e) {
            U.error(log, "Unexpected operation error", e);
        }
    }

    /**
     * @param cacheOp Cache operation.
     */
    private <K, V> void doCacheOp(Consumer<ProducerConsumerOperations.CacheOps<K, V>> cacheOp) {
        IgniteCache<K, V> igniteCache = grid("client").getOrCreateCache(CACHE_NAME);

        cacheOp.accept(ProducerConsumerOperations.ofCache(igniteCache));
    }

    /**
     *
     */
    private class Worker implements Runnable {
        /** */
        private final BiConsumer<ProducerConsumerOperations.CacheOps<String, List<String>>, String> proc;
        /** */
        private final long procDelay;
        /** */
        private final AtomicInteger workerCnt = new AtomicInteger();
        /** */
        private final Stat stat = new Stat();
        /** */
        private final long createTime = System.currentTimeMillis();

        /**
         *
         */
        private Worker(BiConsumer<ProducerConsumerOperations.CacheOps<String, List<String>>, String> proc, long procDelay) {
            this.proc = proc;
            this.procDelay = procDelay;
        }

        /**
         * @param n N.
         */
        private Consumer<ProducerConsumerOperations.CacheOps<String, List<String>>> nCacheOp(int n) {
            return cache -> {
                int keyIdx = n;

                while (!stopped) {
                    if (procDelay > 0)
                        doSleep(procDelay);

                    long startNanos = System.nanoTime();

                    proc.accept(cache, data.key(keyIdx));

                    if (System.currentTimeMillis() - createTime > warmUpTime)
                        stat.register(System.nanoTime() - startNanos);

                    int wCnt = workerCnt.get();

                    if (keyIdx == n)
                        log.info("Worker #" + (n + 1) + " of " + wCnt + " 1st operation done.");

                    keyIdx += wCnt;

                    if (keyIdx >= data.keyCount())
                        keyIdx = n;
                }
            };
        }

        /** {@inheritDoc} */
        @Override public void run() {
            int n = workerCnt.getAndIncrement();

            log.info("Worker #" + (n + 1) + " started.");

            String connAddr = CLUSTER_IP_POOL[(n + 1) % CLUSTER_IP_POOL.length];

            if (isNodeClient)
                doCacheOp(nCacheOp(n));
            else
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
