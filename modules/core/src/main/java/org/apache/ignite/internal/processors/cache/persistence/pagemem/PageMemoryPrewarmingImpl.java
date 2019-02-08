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
package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.PrewarmingConfiguration;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.thread.IgniteThread;
import org.apache.ignite.thread.IgniteThreadFactory;

import static org.apache.ignite.internal.stat.IoStatisticsType.CACHE_GROUP;

/**
 * Default {@link PageMemoryPrewarming} implementation.
 */
public class PageMemoryPrewarmingImpl implements PageMemoryPrewarming, LoadedPagesTracker {
    /** Throttle time in nanoseconds. */
    private static final long THROTTLE_TIME_NANOS = 4_000;

    /** How many last seconds we will check for throttle. */
    private static final int THROTTLE_FIRST_CHECK_PERIOD = 3;

    /** How many last seconds we will check for throttle. */
    private static final int THROTTLE_CHECK_FREQUENCY = 1_000;

    /** Data region name. */
    private final String dataRegName;

    /** Prewarming configuration. */
    private final PrewarmingConfiguration prewarmCfg;

    /** Prewarming page IDs supplier. */
    private final PrewarmingPageIdsSupplier pageIdsSupplier;

    /** Custom prewarming page IDs supplier. */
    private final Supplier<Map<String, Map<Integer, Supplier<int[]>>>> customPageIdsSupplier;

    /** Data region metrics. */
    private final DataRegionMetrics dataRegMetrics;

    /** Cache shared context. */
    private final GridCacheSharedContext<?, ?> ctx;

    /** */
    private final IgniteLogger log;

    /** Need throttling. */
    private final AtomicBoolean needThrottling = new AtomicBoolean();

    /** Throttling count. */
    private final AtomicLong throttlingCnt = new AtomicLong();

    /** Read rates last timestamp. */
    private final AtomicLong readRatesLastTs = new AtomicLong();

    /** Reads values. */
    private final long[] readsVals = new long[THROTTLE_FIRST_CHECK_PERIOD];

    /** Reads rates. */
    private final long[] readsRates = new long[THROTTLE_FIRST_CHECK_PERIOD + 1]; // + ceil for rates

    /** Page memory. */
    private volatile PageMemoryEx pageMem;

    /** Prewarming thread. */
    private volatile Thread prewarmThread;

    /** Stop prewarming flag. */
    private volatile boolean stopPrewarm;

    /** Stopping flag. */
    private volatile boolean stopping;

    /** Thread pool for warm up files processing. */
    private ExecutorService dumpReadSvc;

    /** Thread factory for page loading. */
    IgniteThreadFactory pageLoadThreadFactory;

    /**
     * @param dataRegName Data region name.
     * @param prewarmCfg Prewarming configuration.
     * @param pageIdsSupplier Prewarming page IDs supplier.
     * @param dataRegMetrics Data region metrics.
     * @param ctx Cache shared context.
     */
    public PageMemoryPrewarmingImpl(
        String dataRegName,
        PrewarmingConfiguration prewarmCfg,
        PrewarmingPageIdsSupplier pageIdsSupplier,
        DataRegionMetrics dataRegMetrics,
        GridCacheSharedContext<?, ?> ctx) {

        this.dataRegName = dataRegName;

        assert prewarmCfg != null;

        this.prewarmCfg = prewarmCfg;
        this.dataRegMetrics = dataRegMetrics;

        assert ctx != null;

        this.ctx = ctx;
        this.log = ctx.logger(PageMemoryPrewarmingImpl.class);

        Supplier<Map<String, Map<Integer, Supplier<int[]>>>> customPageIdsSupplier = prewarmCfg.getCustomPageIdsSupplier();

        assert customPageIdsSupplier != null ^ pageIdsSupplier != null;

        this.pageIdsSupplier = pageIdsSupplier;
        this.customPageIdsSupplier = customPageIdsSupplier;

        int dumpReadThread = prewarmCfg.getDumpReadThreads();

        if (dumpReadThread > 1) {
            dumpReadSvc = new ThreadPoolExecutor(
                dumpReadThread,
                dumpReadThread,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());
        }

        pageLoadThreadFactory = new IgniteThreadFactory(ctx.igniteInstanceName(),
            dataRegName + "-prewarm-seg");
    }

    /** {@inheritDoc} */
    @Override public void pageMemory(PageMemoryEx pageMem) {
        this.pageMem = pageMem;

        if (pageIdsSupplier != null)
            pageIdsSupplier.pageMemory(pageMem);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        if (pageIdsSupplier != null)
            pageIdsSupplier.initDir();

        if (prewarmCfg.isWaitPrewarmingOnStart()) {
            prewarmThread = Thread.currentThread();

            prewarm();
        }
        else {
            prewarmThread = new IgniteThread(
                ctx.igniteInstanceName(),
                dataRegName + "-prewarm",
                this::prewarm);

            prewarmThread.setDaemon(true);
            prewarmThread.start();
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        stopping = true;

        try {
            Thread prewarmThread = this.prewarmThread;

            if (prewarmThread != null)
                prewarmThread.join();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteException(e);
        }

        if (pageIdsSupplier != null)
            pageIdsSupplier.stop();
        else if (customPageIdsSupplier instanceof LifecycleAware)
            ((LifecycleAware)customPageIdsSupplier).stop();
    }

    /** {@inheritDoc} */
    @Override public void onPageLoad(int grpId, long pageId) {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void onPageUnload(int grpId, long pageId) {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void onPageEvicted(int grpId, long pageId) {
        stopPrewarm = true;
    }

    /**
     *
     */
    private void prewarm() {
        ExecutorService[] workers = null;

        boolean dumpReadMultithreaded = prewarmCfg.getDumpReadThreads() > 1;

        boolean pageLoadMultithreaded = prewarmCfg.getPageLoadThreads() > 1;

        if (log.isInfoEnabled())
            log.info("Start prewarming of DataRegion [name=" + dataRegName + "]");

        boolean useCustomPageIds = customPageIdsSupplier != null;

        Map<?, Map<Integer, Supplier<int[]>>> pageIdsMap = useCustomPageIds ?
            customPageIdsSupplier.get() :
            pageIdsSupplier.get();

        initReadsRate();

        needThrottling.set(readsRates[readsRates.length - 1] == Long.MAX_VALUE);

        if (needThrottling.get() && log.isInfoEnabled())
            log.info("Detected need to throttle warming up.");

        int segCnt = 0;

        for (Map<?, ?> map : pageIdsMap.values())
            segCnt += map.size();

        if (segCnt == 0) {
            U.warn(log,"Prewarming of DataRegion [name=" + dataRegName + "] skipped because no page IDs supplied.");

            return;
        }

        long startTs = U.currentTimeMillis();

        readRatesLastTs.set(startTs);

        SegmentLoader segmentLdr = new SegmentLoader();

        if (pageLoadMultithreaded) {
            int pageLoadThreads = prewarmCfg.getPageLoadThreads();

            workers = new ExecutorService[pageLoadThreads];

            for (int i = 0; i < pageLoadThreads; i++)
                workers[i] = Executors.newSingleThreadExecutor(pageLoadThreadFactory);

            segmentLdr.setWorkers(workers);
        }

        CountDownFuture completeFut = new CountDownFuture(segCnt);

        AtomicInteger pagesWarmed = new AtomicInteger();

        for (Map.Entry<?, Map<Integer, Supplier<int[]>>> pageIdsEntry : pageIdsMap.entrySet()) {
            int grpId = useCustomPageIds ? CU.cacheId(pageIdsEntry.getKey().toString()) : (Integer)pageIdsEntry.getKey();

            for (Map.Entry<Integer, Supplier<int[]>> partEntry : pageIdsEntry.getValue().entrySet()) {
                Runnable dumpReader = () -> {
                    try {
                        // TODO use localPreloadPartition if useCustomPageIds is true and supplier is WHOLE_PARTITION
                        pagesWarmed.addAndGet(segmentLdr.preload(
                            grpId,
                            partEntry.getKey(),
                            partEntry.getValue().get(),
                            needThrottling));

                        completeFut.onDone();
                    }
                    catch (Throwable e) {
                        completeFut.onDone(e);
                    }
                };

                if (dumpReadMultithreaded)
                    dumpReadSvc.execute(dumpReader);
                else
                    dumpReader.run();
            }
        }

        try {
            completeFut.get();
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Prewarming of DataRegion [name=" + dataRegName + "] failed", e);
        }
        finally {
            long warmingUpTime = U.currentTimeMillis() - startTs;

            if (log.isInfoEnabled()) {
                log.info("Prewarming of DataRegion [name=" + dataRegName +
                    "] finished in " + warmingUpTime + " ms, pages warmed: " + pagesWarmed);
            }

            if (workers != null)
                Arrays.asList(workers).forEach(ExecutorService::shutdown);

            prewarmThread = null;

            if (pageIdsSupplier != null)
                pageIdsSupplier.start();
            else if (customPageIdsSupplier instanceof LifecycleAware)
                ((LifecycleAware)customPageIdsSupplier).start();
        }
    }

    /**
     * Get first IO rates and set ceil for them into last element of rates array.
     */
    private void initReadsRate() {
        readsVals[readsVals.length - 1] = ctx.kernalContext().ioStats().totalPhysicalReads(CACHE_GROUP);

        if (readsVals[readsVals.length - 1] == Long.MAX_VALUE) {
            Arrays.fill(readsVals, Long.MAX_VALUE);
            Arrays.fill(readsRates, Long.MAX_VALUE);

            return;
        }

        for (int i = 0; i < THROTTLE_FIRST_CHECK_PERIOD; i++) {
            LockSupport.parkNanos(1_000_000_000L);

            refreshRates();
        }

        readsRates[readsRates.length - 1] = readsRates[readsRates.length - 2];
    }

    /**
     * Set new IO read rate for last element in rates array.
     *
     * @return New IO read rate.
     */
    private long refreshRates() {
        for (int i = 0; i < readsVals.length - 1; i++) {
            readsVals[i] = readsVals[i + 1];
            readsRates[i] = readsRates[i + 1];
        }

        readsVals[readsVals.length - 1] = ctx.kernalContext().ioStats().totalPhysicalReads(CACHE_GROUP);
        readsRates[readsVals.length - 1] = calculateRate(readsVals);

        return readsRates[readsVals.length - 1];
    }

    /**
     * @param reads IO reads.
     * @return IO rate for given reads.
     */
    private long calculateRate(long[] reads) {
        long curRate = 0;

        for (int i = 1; i < reads.length; i++) {
            long delta = reads[i] - reads[0];

            if (curRate + delta / reads.length < 0)
                return Long.MAX_VALUE;

            curRate += delta / reads.length;
        }

        return curRate;
    }

    /**
     * Check that we need to throttle warmUp.
     *
     * @return {@code True} if warmUp needs throttling.
     */
    private boolean needThrottling() {
        if (readsVals[readsVals.length - 1] == Long.MAX_VALUE)
            return true;

        if (readsVals[0] == 0)
            return false;

        long highBorder = readsRates[readsRates.length - 1] + (long) (readsRates[readsRates.length - 1]
            * prewarmCfg.getThrottleAccuracy());
        long lowBorder = readsRates[readsRates.length - 1] - (long) (readsRates[readsRates.length - 1]
            * prewarmCfg.getThrottleAccuracy());

        if (needThrottling.get()) {
            if (allLower(readsRates, lowBorder))
                return false;

            if (allHigher(readsRates, highBorder)) {
                readsRates[readsRates.length - 1] = readsRates[readsRates.length - 2]; // inc ceil

                return false;
            }

            return true;
        }

        if (allHigher(readsRates, highBorder)) {
            readsRates[readsRates.length - 1] = readsRates[readsRates.length - 2]; // inc ceil

            return false;
        }

        if (allHigher(readsRates, lowBorder)) {
            if (log.isInfoEnabled())
                log.info("Detected need to throttle warming up.");

            return true;
        }

        return false;
    }

    /**
     * @param rates IO read rates.
     * @param border Top border.
     * @return {@code True} if all rates are higher than given value.
     */
    private boolean allHigher(long[] rates, long border) {
        for (int i = 0; i < rates.length - 1; i++) {
            if (rates[i] <= border)
                return false;
        }

        return true;
    }

    /**
     * @param rates IO read rates.
     * @param border Top border.
     * @return {@code True} if all rates are lower than given value.
     */
    private boolean allLower(long[] rates, long border) {
        for (int i = 0; i < rates.length - 1; i++) {
            if (rates[i] >= border)
                return false;
        }

        return true;
    }

    /** */
    private class SegmentLoader {
        /** Workers for multithreaded page loading. */
        private ExecutorService[] workers;

        /**
         * Sets list of workers for multithreaded page loading.
         *
         * @param workers Workers for multithreaded page loading
         */
        public void setWorkers(ExecutorService[] workers) {
            this.workers = workers;
        }

        /**
         * Load pages with indexes {@code pageIdxArr} of partition {@code partId} of cache group {@code grpId} into memory.
         * If workers are setted, they will be used for multithreaded loading.
         *
         * @param grpId Cache group id.
         * @param partId Partition id.
         * @param pageIdxArr Page index array.
         * @param needThrottling Need throttling object.
         * @return Count of preloaded pages.
         */
        public int preload(int grpId, int partId, int[] pageIdxArr, AtomicBoolean needThrottling) {
            AtomicInteger pagesWarmed = new AtomicInteger(0);

            if (stopping || stopPrewarm)
                return pagesWarmed.get();

            if (prewarmCfg.isIndexesOnly() && partId != PageIdAllocator.INDEX_PARTITION)
                return pagesWarmed.get();

            // Arrays.sort(pageIdxArr); // FIXME no need after sorting at dump phase!

            CountDownFuture completeFut = new CountDownFuture(pageIdxArr.length);

            boolean multithreaded = workers != null && workers.length != 0;

            for (int pageIdx : pageIdxArr) {
                if (stopping || stopPrewarm) {
                    completeFut.onDone();

                    break;
                }

                long pageId = PageIdUtils.pageId(partId, pageIdx);

                Runnable pageWarmer = () -> {
                    try {
                        if (loadPage(grpId, pageId)) {
                            pagesWarmed.incrementAndGet();

                            incThrottleCounter();
                        }

                        if (needThrottling.get())
                            LockSupport.parkNanos(THROTTLE_TIME_NANOS);

                        completeFut.onDone();
                    }
                    catch (Throwable e) {
                        completeFut.onDone(e);
                    }
                };

                if (multithreaded) {
                    int segIdx = PageMemoryImpl.segmentIndex(grpId, pageId, pageMem.getSegments());

                    ExecutorService worker = workers[segIdx % workers.length];

                    if (worker == null || worker.isShutdown()) {
                        completeFut.onDone();

                        continue;
                    }

                    worker.execute(pageWarmer);

                } else
                    pageWarmer.run();
            }

            try {
                completeFut.get();
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to prewarm partition [grpId=" + U.hexInt(grpId) + ", partId=" + U.hexInt(partId) + "]", e);

                throw new IgniteException(e);
            }
            /*finally { // FIXME !!!
                if (del.get() && !segFile.delete())
                    U.warn(log, "Failed to delete prewarming dump file: " + segFile.getName());
            }*/

            return pagesWarmed.get();
        }

        /**
         * Loads page with given group ID and page ID into memory.
         *
         * @param grpId Page group ID.
         * @param pageId Page ID.
         * @return Whether page was loaded successfully.
         */
        public boolean loadPage(int grpId, long pageId) {
            boolean warmed = false;

            try {
                long page = pageMem.acquirePage(grpId, pageId);

                pageMem.releasePage(grpId, pageId, page);

                warmed = true;
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to acquire page [grpId=" + grpId + ", pageId=" + pageId + ']', e);
            }

            return warmed;
        }

        /**
         * Increase throttle counter and check throttling need.
         */
        private void incThrottleCounter() {
            long cnt = throttlingCnt.addAndGet(1L);

            if (cnt >= THROTTLE_CHECK_FREQUENCY) {
                synchronized (throttlingCnt) {
                    cnt = throttlingCnt.get();

                    if (cnt >= THROTTLE_CHECK_FREQUENCY) {
                        long newTs = U.currentTimeMillis();

                        if (newTs - readRatesLastTs.get() >= 1) {
                            refreshRates();

                            readRatesLastTs.set(newTs);

                            needThrottling.set(needThrottling());
                        }
                    }

                    throttlingCnt.set(-THROTTLE_CHECK_FREQUENCY);
                }
            }
        }
    }
}
