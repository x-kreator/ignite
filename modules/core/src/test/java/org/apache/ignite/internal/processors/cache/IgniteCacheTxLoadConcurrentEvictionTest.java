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
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedSingleGetFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.PartitionsEvictManager;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequest;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.jsr166.ConcurrentLinkedHashMap;
import org.junit.Test;
import org.mockito.Mockito;

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
    private volatile int keyType = 1 // 0 - sequental, 1 - random, 2 - saved random, 3 - evicting parts, 4 - custom
        ;
    /** */
    private volatile IntSupplier keyFn1;
    /** */
    private volatile IntSupplier keyFn2;
    /** */
    private volatile boolean stopLoad;
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

    private static final int[] PART_IDS1 = {
        105, 107, 110, 113, 115, 117, 118, 124, 126, 129, 135, 138, 139, 140, 141, 142, 143, 146, 147, 149, 153, 154,
        155, 159, 161, 167, 170, 173, 177, 179, 187, 192, 195, 196, 197, 207, 208, 212, 213, 215, 218, 219, 222, 224,
        227, 230, 233, 237, 238, 240, 241, 242, 246, 247, 249, 250, 253, 254, 255, 258, 261, 265, 277, 279, 280, 284,
        290, 291, 297, 300, 302, 314, 315, 317, 320, 323, 325, 326, 328, 329, 330, 332, 333, 342, 344, 350, 351, 354,
        357, 362, 368, 369, 373, 374, 377, 379, 380, 381, 382, 383, 385, 386, 387, 389, 391, 393, 403, 404, 407, 415,
        422, 423, 425, 428, 431, 435, 436, 441, 443, 444, 447, 459, 460, 465, 469, 474, 478, 479, 481, 483, 484, 491,
        494, 506, 509, 515, 517, 520, 525, 526, 527, 528, 532, 537, 539, 541, 543, 547, 552, 553, 554, 558, 560, 564,
        565, 570, 574, 582, 583, 591, 593, 597, 598, 599, 602, 603, 604, 607, 608, 610, 614, 618, 619, 620, 621, 622,
        624, 627, 628, 630, 634, 635, 640, 645, 648, 650, 654, 662, 677, 680, 681, 682, 686, 688, 689, 693, 696, 702,
        704, 707, 714, 716, 718, 723, 724, 725, 728, 731, 733, 734, 736, 738, 740, 741, 743, 746, 747, 750, 754, 756,
        757, 760, 766, 767, 768, 769, 771, 775, 777, 778, 786, 787, 788, 791, 795, 796, 797, 803, 805, 807, 809, 812,
        814, 817, 822, 827, 829, 830, 832, 834, 835, 840, 843, 845, 849, 851, 856, 858, 859, 860, 861, 863, 866, 868,
        872, 875, 876, 877, 879, 881, 883, 885, 892, 895, 899, 906, 908, 909, 911, 912, 914, 915, 916, 920, 921, 925,
        929, 930, 931, 933, 940, 941, 943, 944, 947, 951, 961, 962, 966, 968, 982, 983, 984, 987, 991, 993, 995, 997,
    };

    /** */
    private static final int[] PART_IDS2 = {
        18, 22, 34, 46, 55, 57, 64, 74, 75, 80, 84, 85, 95, 99, 101, 102, 103, 111, 112, 119, 120, 128, 133, 145, 162,
        163, 169, 171, 172, 183, 193, 194, 200, 202, 206, 214, 216, 217, 220, 221, 223, 231, 232, 244, 252, 270, 274,
        278, 293, 294, 295, 309, 310, 311, 327, 331, 339, 346, 347, 359, 361, 365, 378, 392, 397, 400, 402, 408, 414,
        417, 418, 424, 430, 432, 442, 453, 454, 464, 466, 468, 477, 480, 485, 489, 492, 493, 496, 497, 498, 503, 510,
        516, 518, 523, 540, 549, 559, 563, 566, 568, 572, 578, 579, 580, 584, 585, 587, 588, 592, 601, 612, 616, 617,
        626, 629, 632, 637, 639, 655, 656, 657, 663, 668, 671, 674, 675, 679, 687, 691, 695, 699, 703, 706, 708, 712,
        713, 717, 737, 742, 745, 751, 752, 759, 761, 773, 780, 783, 784, 802, 804, 806, 808, 813, 815, 819, 820, 833,
        841, 846, 853, 854, 862, 869, 880, 887, 891, 896, 901, 903, 905, 913, 918, 932, 936, 937, 938, 952, 953, 954,
        957, 959, 969, 972, 975, 980, 994, 1006, 1007, 1008};

    /** */
    private volatile int[] partIds = PART_IDS1; // PART_IDS2

    /**
     *
     */
    @Test
    public void testWorkIdea() throws Exception {
    }

    /**
     *
     */
    private class EvictionRaceDetector {
        /** */
        private final GridCacheContext<?, ?> cacheCtx;
        /** */
        private final IgniteLogger log;
        /** */
        private final Map<Integer, T2<AtomicInteger, CountDownLatch[]>> partOps = new ConcurrentHashMap<>();
        /** */
        private final AtomicInteger awaitingTxFinishes = new AtomicInteger();
        /** */
        private final AtomicInteger awaitingSingleGets = new AtomicInteger();
        /** */
        private final int maxConcurrentRequestsAwaiting;
        /** */
        private volatile Set<Integer> evictingPartIds = Collections.emptySet();
        /** */
        private volatile boolean active;

        /**
         * @param cacheCtx Cache context.
         */
        EvictionRaceDetector(GridCacheContext<?, ?> cacheCtx) {
            this.cacheCtx = cacheCtx;
            log = cacheCtx.logger(getClass());

            maxConcurrentRequestsAwaiting = cacheCtx.kernalContext().config().getStripedPoolSize() / 2 - 1;

            GridCacheSharedContext<?, ?> sharedCtx = cacheCtx.shared();
            GridCacheSharedContext<?, ?> spySharedCtx = Mockito.spy(sharedCtx);

            PartitionsEvictManager spyEvictMgr = Mockito.spy(sharedCtx.evict());
            Mockito.doAnswer(invocation -> spyEvictMgr).when(spySharedCtx).evict();

            ((GridDhtPartitionTopologyImpl) cacheCtx.topology()).partitionFactory(new GridDhtPartitionTopologyImpl.PartitionFactory() {
                @Override public GridDhtLocalPartition create(GridCacheSharedContext ctx, CacheGroupContext grp, int id) {
                    GridDhtLocalPartition part = new GridDhtLocalPartition(spySharedCtx, grp, id, false);
                    onPartitionCreated(part);
                    return part;
                }
            });

            Mockito.doAnswer(invocation -> {
                GridDhtLocalPartition part = invocation.getArgumentAt(1, GridDhtLocalPartition.class);

                System.out.println(U.format(new Date(), "HH:mm:ss,SSS") + "  -- Evicting partition id: " + part.id());

                CountDownLatch latch = active ? beforeEvictPartitionAsync(part) : null;

                try {
                    return invocation.callRealMethod();
                }
                finally {
                    if (latch != null)
                        GridTestUtils.runAsync(() -> awaitPartitionEviction(part, latch));
                }
            }).when(spyEvictMgr).evictPartitionAsync(Mockito.any(), Mockito.any());

            addBeforeCacheHandlers(cacheCtx,
                new T3<>(0, GridDhtTxFinishRequest.class,
                    (nodeId, req) -> processDhtTxFinishRequest(nodeId, (GridDhtTxFinishRequest)req)),
                new T3<>(cacheCtx.cacheId(), GridNearSingleGetRequest.class,
                    (nodeId1, req1) -> processNearSingleGetRequest(nodeId1, (GridNearSingleGetRequest)req1)));
        }

        /**
         * @param evictingPartIds Evicting partition ids.
         */
        private void setEvictingPartIds(Collection<Integer> evictingPartIds) {
            this.evictingPartIds = new GridConcurrentHashSet<>(evictingPartIds);
        }

        /**
         * @param p Partition id.
         */
        private T2<AtomicInteger, CountDownLatch[]> newPartOpState(int p) {
            return new T2<>(new AtomicInteger(0), newLatchArray(3));
        }

        /**
         * @param limit Limit.
         * @param state State.
         * @param stateBits State bits.
         */
        private boolean enterState(AtomicInteger limit, AtomicInteger state, int stateBits) {
            boolean limitUpdated = false;

            while (true) {
                if (limit != null && !limitUpdated) {
                    int l = limit.get();

                    if (l >= maxConcurrentRequestsAwaiting)
                        return false;

                    if (limit.compareAndSet(l, l + 1))
                        limitUpdated = true;
                    else
                        continue;
                }

                int s = state.get();

                if ((s & stateBits) == stateBits) {
                    if (limit != null)
                        limit.decrementAndGet();

                    return false;
                }

                if (state.compareAndSet(s, s | stateBits))
                    return true;
            }
        }

        /**
         * @param state State.
         * @param stateBits State bits.
         */
        private boolean leaveState(AtomicInteger state, int stateBits) {
            int s = state.get();

            return state.compareAndSet(s, s & ~stateBits);
        }

        /**
         * @param nodeId Node id.
         * @param req Request.
         */
        private void processDhtTxFinishRequest(UUID nodeId, GridDhtTxFinishRequest req) {
            if (!active) // FIXME
                return;

            IgniteInternalTx tx = cacheCtx.shared().tm().tx(req.version());

            if (tx != null && tx.writeEntries() != null) {
                // System.out.println(">>> processed request: " + req + "]\n >> tx: " + tx);

                for (IgniteTxEntry txEntry : tx.writeEntries()) {
                    int partId = txEntry.key().partition();

                    if (evictingPartIds.contains(partId)) {
                        T2<AtomicInteger, CountDownLatch[]> partOpState = partOps.computeIfAbsent(
                            partId,
                            this::newPartOpState);

                        if (partOpState.get1() == null) {
                            evictingPartIds.remove(partId);

                            continue;
                        }

                        if (partOpState.get1().get() == 2 && enterState(awaitingTxFinishes, partOpState.get1(), 1)) {
                            logPartOpState("1.1", partId, partOpState);

                            int awaiting = 0;

                            for (Map.Entry<Integer, T2<AtomicInteger, CountDownLatch[]>> entry : partOps.entrySet()) {
                                if ((entry.getValue().get1().get() & 3) == 3 &&
                                    entry.getValue().get2()[0].getCount() == 1 &&
                                    entry.getValue().get2()[1].getCount() == 1) {
                                    awaiting++;

                                    if (awaiting == 3) {
                                        System.out.println(">>> Stopping load by condition!");

                                        stopLoad = true;
                                    }
                                }
                            }

                            try {
                                if (!partOpState.get2()[0].await(1, TimeUnit.SECONDS)) {
                                    if (leaveState(partOpState.get1(), 1)) {
                                        logPartOpState("1.t", partId, partOpState);

                                        break;
                                    }
                                }
                            }
                            catch (InterruptedException e) { // FIXME !!!
                                Thread.currentThread().interrupt();
                                e.printStackTrace();
                            }
                            finally {
                                awaitingTxFinishes.decrementAndGet();
                            }

                            logPartOpState("1.2", partId, partOpState);

                            GridCacheEntryEx cached = txEntry.cached();

                            GridDhtLocalPartition locPart =
                                cacheCtx.group().topology().localPartition(txEntry.cached().partition());

                            GridDhtLocalPartition cachedLocPart = null;

                            if (cached instanceof GridCacheMapEntry)
                                cachedLocPart = ((GridCacheMapEntry)cached).localPartition();

                            if (locPart != cachedLocPart) {
                                LT.warn(log(), "Cached partition isn't same as reserved for operation [locPart=" +
                                    locPart + "\n, cachedLocPart=" + cachedLocPart);
                            }
                            else
                                LT.warn(log(), "cachedTrack=" + txEntry.cachedTrack + "\n  cachedLocPart=" + cachedLocPart);

                            // TODO further callRealMethod should fail!

                            break;
                        }
                        else
                            logPartOpState("1.0", partId, partOpState);
                    }
                }
            }
        }

        /** */
        private final Map<Date, Map<Integer, AtomicInteger>> nsgrTimeMap = new ConcurrentLinkedHashMap<>();

        /** */
        private final Map<Integer, Integer> keyGetCounts = new ConcurrentHashMap<>();

        /**
         * @param nodeId Node id.
         * @param req Request.
         */
        private void processNearSingleGetRequest(UUID nodeId, GridNearSingleGetRequest req) {
            int partId = req.key().partition();

            Date time = new Date(U.currentTimeMillis() / 1000 * 1000);

            nsgrTimeMap
                .computeIfAbsent(time, t -> new ConcurrentLinkedHashMap<>())
                .computeIfAbsent(partId, p -> new AtomicInteger())
                .incrementAndGet();

            Integer key = req.key().value(null, false);

            keyGetCounts.compute(key, (k, cnt) -> cnt != null ? cnt + 1 : 1);

            if (evictingPartIds.contains(partId)) {
                // System.out.println("  >>> partId: " + partId + ", key: " + key);

                if (!active) // FIXME
                    return;

                T2<AtomicInteger, CountDownLatch[]> partOpState = partOps.computeIfAbsent(
                    partId,
                    this::newPartOpState);

                if (partOpState.get1() == null) {
                    evictingPartIds.remove(partId);

                    return;
                }

                if (enterState(awaitingSingleGets, partOpState.get1(), 2)) {
                    //partOpState.get2()[2].countDown();

                    logPartOpState("2.1", partId, partOpState);

                    try {
                        if (!partOpState.get2()[1].await(300, TimeUnit.MILLISECONDS)) {
                            if (leaveState(partOpState.get1(), 2)) {
                                logPartOpState("2.t", partId, partOpState);

                                return;
                            }
                        }
                    }
                    catch (InterruptedException e) { // FIXME !!!
                        Thread.currentThread().interrupt();
                        e.printStackTrace();
                    }
                    finally {
                        awaitingSingleGets.decrementAndGet();
                    }

                    logPartOpState("2.2", partId, partOpState);
                }
                else
                    logPartOpState("2.0", partId, partOpState);
            }
        }

        /**
         * @param part Partition.
         */
        private void onPartitionCreated(GridDhtLocalPartition part) {
            int partId = part.id();

            T2<AtomicInteger, CountDownLatch[]> partOpState = partOps.get(partId);

            if (partOpState == null || partOpState.get2() == null)
                return;

            CountDownLatch[] latches = partOpState.get2();

            if (latches[0].getCount() == 1 && latches[1].getCount() == 0) {
                GridTestUtils.runAsync(() -> {
                    logPartOpState("2.3", partId, partOpState);

                    while (cacheCtx.topology().localPartition(partId) != part)
                        ;

                    latches[0].countDown();

                    logPartOpState("2.4", partId, partOpState);
                });
            }
        }

        /**
         * @param part Partition.
         */
        private CountDownLatch beforeEvictPartitionAsync(GridDhtLocalPartition part) throws InterruptedException {
            CountDownLatch latch = null;

            if (evictingPartIds.contains(part.id())) {
                T2<AtomicInteger, CountDownLatch[]> partOpState = partOps.computeIfAbsent(
                    part.id(),
                    p -> new T2<>(null, null));

                if (partOpState.get1() != null) {
                    if (!enterState(null, partOpState.get1(), 2)) {
                        logPartOpState("3.1", part.id(), partOpState);

                        //partOpState.get2()[1].await();

                        latch = partOpState.get2()[1];
                    }
                    else {
                        logPartOpState("3.0", part.id(), partOpState);

                        evictingPartIds.remove(part.id());

                        partOpState.get2()[0].countDown();
                        partOpState.get2()[1].countDown();
                    }
                }
            }

            return latch;
        }

        /**
         * @param part Partition.
         * @param latch Latch.
         */
        private void awaitPartitionEviction(GridDhtLocalPartition part, CountDownLatch latch) {
            try {
                if (GridTestUtils.waitForCondition(() -> part.state() == GridDhtPartitionState.EVICTED, 10_000)) {
                    latch.countDown();

                    logPartOpState("3.2", part.id(), partOps.get(part.id()));
                }
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteInterruptedException(e.getCause(InterruptedException.class));
            }
        }
    }

    /**
     *
     */
    @Test
    public void testTxLoadConcurrentEviction() throws Exception {
        IgniteEx ignite1 = startGrid(1);

        GridCacheContext<?, ?> cacheCtx1 = ignite1.cachex(DEFAULT_CACHE_NAME).context();

        startGrid(2);
        startGrid(3);
        startGrid(4);
        awaitPartitionMapExchange();
        Set<Integer> top4PartIds = partIds(cacheCtx1.topology().localPartitions());
        AffinityTopologyVersion top4Ver = cacheCtx1.affinity().affinityTopologyVersion();

        EvictionRaceDetector evictionRaceDetector = new EvictionRaceDetector(cacheCtx1);

        stopGrid(4);
        awaitPartitionMapExchange();
        Set<Integer> top3PartIds = partIds(cacheCtx1.topology().localPartitions());
        AffinityTopologyVersion top3Ver = cacheCtx1.affinity().affinityTopologyVersion();

        stopGrid(3);
        awaitPartitionMapExchange();
        Set<Integer> top2PartIds = partIds(cacheCtx1.topology().localPartitions());
        AffinityTopologyVersion top2Ver = cacheCtx1.affinity().affinityTopologyVersion();

        Set<Integer> evictingTop2To3PartIds = new HashSet<>(top2PartIds);
        evictingTop2To3PartIds.removeAll(top3PartIds);
        System.out.println("  ## top2to3: " + Arrays.toString(evictingTop2To3PartIds.stream().mapToInt(Integer::intValue).sorted().toArray()));

        Set<Integer> evictingTop3To4PartIds = new HashSet<>(top3PartIds);
        evictingTop3To4PartIds.removeAll(top4PartIds);
        System.out.println("  ## top3to4: " + Arrays.toString(evictingTop3To4PartIds.stream().mapToInt(Integer::intValue).sorted().toArray()));
/*
        Set<Integer> evictingTop2To4PartIds = new HashSet<>(evictingTop2To3PartIds);
        evictingTop2To4PartIds.addAll(evictingTop3To4PartIds);
*/
        // evictingPartIds.removeIf(v -> v < 105 || v > 997); // FIXME ?

        int[] evictingPartIds = evictingTop3To4PartIds.stream()
            .mapToInt(Integer::intValue)
            .filter(p -> cacheCtx1.affinity().primaryByPartition(cacheCtx1.localNode(), p, top2Ver))
            .filter(p -> p >= 400)
            .sorted().limit(8).toArray();

        System.out.println("  ## evicting part ids: " + evictingPartIds.length);
        for (int p : evictingPartIds) {
            String p2 = nodeIndexes(cacheCtx1.affinity().nodesByPartition(p, top2Ver));
            String p3 = nodeIndexes(cacheCtx1.affinity().nodesByPartition(p, top3Ver));
            String p4 = nodeIndexes(cacheCtx1.affinity().nodesByPartition(p, top4Ver));

            System.out.println("  " + p + ": " + p2 + "/" + p3 + "/" + p4);
        }

        evictionRaceDetector.setEvictingPartIds(Arrays.stream(evictingPartIds).boxed().collect(Collectors.toList()));
/*
        AtomicInteger partIdsIdx = new AtomicInteger();

        keyFn1 = () -> {
            while (true) {
                int i = partIdsIdx.get();

                if (partIdsIdx.compareAndSet(i, i + 1 >= evictingPartIds.length ? 0 : i + 1))
                    return evictingPartIds[i];
            }
        };

        keyFn2 = keyFn1;
*/
        AtomicInteger threadIdx = new AtomicInteger();
        ThreadLocal<Integer> tlIdx = ThreadLocal.withInitial(threadIdx::getAndIncrement);
        ThreadLocal<AtomicInteger> tlItr = ThreadLocal.withInitial(AtomicInteger::new);

        keyFn1 = () -> {
            int idx = tlIdx.get();

            if (idx >= evictingPartIds.length)
                idx = ThreadLocalRandom.current().nextInt(evictingPartIds.length);

            int key = evictingPartIds[idx] + tlItr.get().getAndIncrement() * 1024; // FIXME partition count!

            while (cacheCtx1.affinity().partition(key) != evictingPartIds[idx])
                key--;

            return key;
        };

        keyFn2 = () -> {
            int idx = tlIdx.get();

            idx = idx % 2 == 0 ? idx + 1 : idx - 1;

            if (idx >= evictingPartIds.length)
                idx = ThreadLocalRandom.current().nextInt(evictingPartIds.length);

            int key = evictingPartIds[idx] + tlItr.get().get() * 1024; // FIXME partition count!

            while (cacheCtx1.affinity().partition(key) != evictingPartIds[idx])
                key--;

            return key;
        };


        clientMode = true;

        IgniteEx[] clients = new IgniteEx[4];

        for (int i = 0; i < clients.length; i++)
            clients[i] = startGrid(i + 10);

        clientMode = false;

        LT.info(log, ">>> Initial topology started...");


        addNSGRLogging(evictingTop3To4PartIds, grid(1)/*, grid(2)*/);

        /*addBeforeCacheHandlers(cacheCtx1,
            new T3<>(cacheCtx1.cacheId(), GridNearSingleGetRequest.class, ((uuid, msg) -> {
                GridNearSingleGetRequest req = (GridNearSingleGetRequest)msg;

                System.out.println("  ^^ NSGR part/key: " + req.key().partition() + "/" + req.key().value(null, false) +
                    ", uuid: " + uuid);
            })));*/

        //doSleep(2_000);

        LT.info(log, ">>> Half test time passed...");


/*
        GridTestUtils.runAsync(
            () -> txPutGet(clients[0], asIntSupplier(40), asIntSupplier(60)),
            "tx-put-get-1")
            .get();

        GridTestUtils.runAsync(
            () -> txPutGet(clients[1], asIntSupplier(60), asIntSupplier(70)),
            "tx-put-get-2")
            .get();
*/

        //randomKeys = false;


/*
        evictionRaceDetector.setEvictingPartIds(Stream.concat(
            Arrays.stream(PART_IDS1).boxed(),
            Arrays.stream(PART_IDS2).boxed()).collect(Collectors.toSet()));

        evictionRaceDetector.active = true; // TODO
*/
        IgniteEx ignite3 = startGrid(3);
        LT.info(log, ">>> Node started: " + ignite3.name());

        // keyType = 3;

        partIds = PART_IDS2;

        keyType = 4;

        IgniteInternalFuture<Long> txLoadFinishFut =
            runMassAsync(this::txLoadCycle, 2, clients);
            //runMassAsync(this::txLoadCycle, 64, clients);

        //doSleep(500);

        IgniteEx ignite4 = startGrid(4);
        LT.info(log, ">>> Node started: " + ignite4.name());

        evictionRaceDetector.active = true; // TODO

        // Set<Integer> evictingPartIds = Arrays.stream(partIds).boxed().collect(Collectors.toSet());
        // log.info("Evicting partition ids: " + evictingPartIds);
        // evictionRaceDetector.setEvictingPartIds(evictingPartIds);

        doSleep(3_000);

        stopLoad = true;

        /*LT.info(log, "NSGR time map:");
        evictionRaceDetector.nsgrTimeMap.forEach(((date, map) -> {
            System.out.println(U.format(date, "  HH:mm:ss"));

            map.forEach((part, reqCnt) -> System.out.println("    " + part + ": " + reqCnt));
        }));*/

        /*AtomicInteger evictingPartKeyGets = new AtomicInteger();
        LT.info(log, "Evicting part count: " + evictingPartIds.size() + ", NSGR/key >1:" );
        evictionRaceDetector.keyGetCounts.forEach((key, cnt) -> {
            int part = evictionRaceDetector.cacheCtx.affinity().partition(key);

            boolean evicting = evictingPartIds.contains(part);

            if (evicting)
                evictingPartKeyGets.incrementAndGet();

            if (cnt > 1)
                System.out.println("  key/part/cnt/evicting: " + key + "/" + part + "/" + cnt + "/" + evicting);
        });
        LT.info(log, "Evicting part key gets: " + evictingPartKeyGets);*/

        LT.info(log, "getRequestTrackRoutes: " + GridPartitionedSingleGetFuture.getRequestTrackRoutes);
        LT.info(log, "Total txs: " + totalTxs.sum());
        LT.info(log, "SingleGetRequests created/processed: " +
            GridPartitionedSingleGetFuture.singleGetRequestsCreated.sum() + "/" +
            GridDhtPartitionTopologyImpl.locPart0ReqCnt.sum());

        txLoadFinishFut.get(10_000);

        if (keyType == 1) {
            int[] keys1 = new int[rndSeq1Idx.get()];
            int[] keys2 = new int[rndSeq2Idx.get()];

            System.arraycopy(rndSeq1, 0, keys1, 0, keys1.length);
            System.arraycopy(rndSeq2, 0, keys2, 0, keys2.length);

            LT.info(log, Arrays.toString(keys1));
            LT.info(log, Arrays.toString(keys2));
        }
    }

    /**
     * @param partitions Partitions.
     * @return Set of partition ids.
     */
    private static Set<Integer> partIds(Collection<GridDhtLocalPartition> partitions) {
        return partitions.stream().map(GridDhtLocalPartition::id).collect(Collectors.toSet());
    }

    /**
     * @param size Size.
     */
    private static CountDownLatch[] newLatchArray(int size) {
        CountDownLatch[] a = new CountDownLatch[size];

        for (int i = 0; i < size; i++)
            a[i] = new CountDownLatch(1);

        return a;
    }

    /**
     * @param cacheCtx Cache context.
     * @param cacheHandlers Cache handlers.
     */
    private static void addBeforeCacheHandlers(GridCacheContext<?, ?> cacheCtx, T3<Integer, Class<?>, CI2<UUID, GridCacheMessage>>... cacheHandlers) {
        if (cacheHandlers == null || cacheHandlers.length == 0)
            return;

        Map<T2<Integer, Class<?>>, CI2<UUID, GridCacheMessage>> hndMap = Arrays.stream(cacheHandlers)
            .collect(Collectors.toMap(h -> new T2<>(h.get1(), h.get2()), T3::get3));

        Object ch = U.field(cacheCtx.io(), "cacheHandlers");
        ConcurrentMap<Object, IgniteBiInClosure<UUID, GridCacheMessage>> clsHandlers = U.field(ch, "clsHandlers");

        for (Map.Entry<Object, IgniteBiInClosure<UUID, GridCacheMessage>> entry : clsHandlers.entrySet()) {
            if (hndMap.isEmpty())
                break;

            Object key = entry.getKey();

            CI2<UUID, GridCacheMessage> hnd = hndMap.remove(new T2<Integer, Class<?>>(
                U.field(key, "hndId"),
                U.field(key, "msgCls")));

            if (hnd != null) {
                IgniteBiInClosure<UUID, GridCacheMessage> oldHnd = entry.getValue();

                entry.setValue(((uuid, msg) -> {
                    hnd.apply(uuid, msg);
                    oldHnd.apply(uuid, msg);
                }));

                log.info("Cache handler updated:\n  " + S.toString((Class<Object>) key.getClass(), key) +
                    " -> " + entry.getValue().getClass().getName());
            }
        }
    }

    /**
     * @param nodes Nodes.
     */
    private static String nodeIndexes(List<ClusterNode> nodes) {
        return nodes.stream()
            .map(ClusterNode::id)
            .map(UUID::toString)
            .map(n -> String.valueOf(n.charAt(n.length() - 1)))
            .collect(Collectors.joining("-"));
    }

    private void addNSGRLogging(Set<Integer> top3To4, IgniteEx... nodes) {
        Arrays.asList(nodes).forEach(node -> {
            GridCacheContext<?, ?> cacheCtx = node.cachex(DEFAULT_CACHE_NAME).context();

            addBeforeCacheHandlers(cacheCtx,
                new T3<>(cacheCtx.cacheId(), GridNearSingleGetRequest.class, ((uuid, msg) -> {
                    GridNearSingleGetRequest req = (GridNearSingleGetRequest)msg;

                    AffinityTopologyVersion topVer = req.topologyVersion();

                    if (node.name().endsWith("1")) {
                        String pn = nodeIndexes(cacheCtx.affinity().nodesByPartition(req.key().partition(), topVer));

                        System.out.println(U.format(new Date(), "HH:mm:ss,SSS") + "  ^^ " + node.name() + "@NSGR nodes/part/key: " +
                            pn + "/" + req.key().partition() + "/" + req.key().value(null, false) +
                            (top3To4.contains(req.key().partition()) ? ", 3-4, " : ", 2-3, ") + topVer + ", uuid: " + uuid);
                    }
                })),
                new T3<>(0, GridDhtTxFinishRequest.class, ((uuid, msg) -> {
                    GridDhtTxFinishRequest req = (GridDhtTxFinishRequest)msg;

                    AffinityTopologyVersion topVer = req.topologyVersion();

                    IgniteInternalTx tx = cacheCtx.shared().tm().tx(req.version());

                    if (node.name().endsWith("1")) {
                        if (tx != null && tx.writeEntries() != null) {
                            System.out.println(U.format(new Date(), "HH:mm:ss,SSS") + "  >> tx.xid: " + tx.xid());

                            for (IgniteTxEntry txEntry : tx.writeEntries()) {
                                KeyCacheObject key = txEntry.key();

                                String pn = nodeIndexes(cacheCtx.affinity().nodesByPartition(key.partition(), topVer));

                                System.out.println(U.format(new Date(), "HH:mm:ss,SSS") + "  ** " + node.name() + "@DTFR nodes/part/key: " +
                                    pn + "/" + key.partition() + "/" + key.value(null, false) +
                                    (top3To4.contains(key.partition()) ? ", 3-4, " : ", 2-3, ") + topVer + ", uuid: " + uuid);
                            }
                        }

                    }
                }))
            );
        });
    }

    /**
     * @param phase Phase.
     * @param partId Partition id.
     * @param partOpState Partition operation state.
     */
    private static void logPartOpState(String phase, int partId, T2<AtomicInteger, CountDownLatch[]> partOpState) {
        log.info(">>> #" + phase + " partOpState[" + partId + "]: " + partOpState.get1() + "/" +
            Arrays.stream(partOpState.get2()).map(CountDownLatch::getCount).collect(Collectors.toList()));
    }

    /**
     * @param op Node operation.
     * @param threadNum Thread number.
     * @param nodes Nodes.
     */
    private IgniteInternalFuture<Long> runMassAsync(Consumer<IgniteEx> op, int threadNum, IgniteEx... nodes) {
        GridCompoundIdentityFuture<Long> opFinishFut = new GridCompoundIdentityFuture<>();

        for (int i = 0; i < nodes.length; i++) {
            IgniteEx c = nodes[i];

            opFinishFut.add(GridTestUtils.runMultiThreadedAsync(() -> op.accept(c), threadNum, "mass-op-" + i));
        }

        opFinishFut.markInitialized();

        return opFinishFut;
    }

    /**
     * @param client Client.
     * @param key1 Key1 supplier.
     * @param key2 Key2 supplier.
     */
    private void txPutGet(IgniteEx client, IntSupplier key1, IntSupplier key2) {
        IgniteCache<Integer, Object> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        try {
            doInTransaction(client, OPTIMISTIC, SERIALIZABLE, () -> cacheOp(cache, key1, key2));

            totalTxs.increment();
        }
        catch (TransactionOptimisticException ignored) {
            // No-op
        }
        catch (Exception e) {
            U.error(log, "Operation error", e);
        }
    }

    /**
     * @param client Client.
     */
    private void txLoadCycle(IgniteEx client) {
        IgniteCache<Integer, Object> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        Callable<Object> clo = () -> cacheOp(cache, this::nextKey1, this::nextKey2);

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
    private Object cacheOp(IgniteCache<Integer, Object> cache, IntSupplier key1, IntSupplier key2) {
        int key = key1.getAsInt();

        Object val = cache.get(key);

        if (val != null)
            key = key2.getAsInt();

        cache.put(key, new SampleValue(key));

        return null;
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

            case 3:
                int[] partIds = this.partIds;

                return partIds[nextRandom(0, partIds.length)] + 1024 * nextRandom(0, 64);

            case 4:
                return keyFn1.getAsInt();

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

            case 3:
                int[] partIds = this.partIds;

                return partIds[nextRandom(0, partIds.length)] + 1024 * nextRandom(0, 64);

            case 4:
                return keyFn2.getAsInt();

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
