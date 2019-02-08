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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.PrewarmingConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIO;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;
import org.apache.ignite.thread.IgniteThreadFactory;

import static org.apache.ignite.internal.pagemem.PageIdUtils.PAGE_IDX_MASK;
import static org.apache.ignite.internal.pagemem.PageIdUtils.PAGE_IDX_SIZE;
import static org.apache.ignite.internal.pagemem.PageIdUtils.PART_ID_SIZE;
import static org.apache.ignite.internal.stat.IoStatisticsType.CACHE_GROUP;

/**
 * Default {@link PageMemoryPrewarming} implementation.
 */
public class PageMemoryPrewarmingImpl implements PageMemoryPrewarming, LoadedPagesTracker {
    /** Prewarming page IDs dump directory name. */
    private static final String PREWARM_DUMP_DIR = "prewarm";

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

    /** Prewarming page ids supplier. */
    private final Supplier<Map<Integer, Map<Integer, Supplier<int[]>>>> pageIdsSupplier;

    /** Data region metrics. */
    private final DataRegionMetrics dataRegMetrics;

    /** Cache shared context. */
    private final GridCacheSharedContext<?, ?> ctx;

    /** */
    private final IgniteLogger log;

    /** */
    private final ConcurrentMap<Long, Segment> segments = new ConcurrentHashMap<>();

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

    /** Prewarming page IDs dump directory. */
    private File dumpDir;

    /** Prewarming thread. */
    private volatile Thread prewarmThread;

    /** Stop prewarming flag. */
    private volatile boolean stopPrewarm;

    /** Dump worker. */
    private volatile LoadedPagesIdsDumpWorker dumpWorker;

    /** Loaded pages dump in progress flag. */
    private volatile boolean loadedPagesDumpInProgress;

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

        this.pageIdsSupplier = customPageIdsSupplier == null ? pageIdsSupplier :
            () -> {
                Map<Integer, Map<Integer, Supplier<int[]>>> map = new HashMap<>();

                for (Map.Entry<String, Map<Integer, Supplier<int[]>>> entry : customPageIdsSupplier.get().entrySet())
                    map.put(CU.cacheId(entry.getKey()), entry.getValue());

                return map;
            };

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
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        initDir();

        if (prewarmCfg.isWaitPrewarmingOnStart()) {
            prewarmThread = Thread.currentThread();

            warmUp();
        }
        else {
            prewarmThread = new IgniteThread(
                ctx.igniteInstanceName(),
                dataRegName + "-prewarm",
                this::warmUp);

            prewarmThread.setDaemon(true);
            prewarmThread.start();
        }

        if (prewarmCfg.getRuntimeDumpDelay() > PrewarmingConfiguration.RUNTIME_DUMP_DISABLED) {
            dumpWorker = new LoadedPagesIdsDumpWorker();

            new IgniteThread(dumpWorker).start();
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        stopping = true;

        try {
            Thread prewarmThread = this.prewarmThread;

            if (prewarmThread != null)
                prewarmThread.join();

            if (dumpWorker != null) {
                if (!loadedPagesDumpInProgress)
                    dumpWorker.cancel();

                dumpWorker.join();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteException(e);
        }

        dumpLoadedPagesIds(true);
    }

    /** {@inheritDoc} */
    @Override public void onPageLoad(int grpId, long pageId) {
        if (prewarmCfg.isIndexesOnly() &&
            PageIdUtils.partId(pageId) != PageIdAllocator.INDEX_PARTITION)
            return;

        getSegment(Segment.key(grpId, pageId)).incCount();
    }

    /** {@inheritDoc} */
    @Override public void onPageUnload(int grpId, long pageId) {
        if (prewarmCfg.isIndexesOnly() &&
            PageIdUtils.partId(pageId) != PageIdAllocator.INDEX_PARTITION)
            return;

        getSegment(Segment.key(grpId, pageId)).decCount();
    }

    /** {@inheritDoc} */
    @Override public void onPageEvicted(int grpId, long pageId) {
        stopPrewarm = true;

        onPageUnload(grpId, pageId);
    }

    /**
     *
     */
    private void initDir() throws IgniteException {
        IgnitePageStoreManager store = ctx.pageStore();

        assert store instanceof FilePageStoreManager : "Invalid page store manager was created: " + store;

        dumpDir = Paths.get(((FilePageStoreManager)store).workDir().getAbsolutePath(), PREWARM_DUMP_DIR).toFile();

        if (!U.mkdirs(dumpDir))
            throw new IgniteException("Could not create directory for prewarming data: " + dumpDir);
    }

    /**
     *
     */
    private void warmUp() {
        ExecutorService[] workers = null;

        boolean dumpReadMultithreaded = prewarmCfg.getDumpReadThreads() > 1;

        boolean pageLoadMultithreaded = prewarmCfg.getPageLoadThreads() > 1;

        try {
            if (log.isInfoEnabled())
                log.info("Start prewarming of DataRegion [name=" + dataRegName + "]");

            File[] segFiles = dumpDir.listFiles(Segment.FILE_FILTER);

            if (segFiles == null) {
                if (log.isInfoEnabled())
                    log.info("Saved prewarming dump files not found!");

                return;
            }

            if (log.isInfoEnabled())
                log.info("Saved prewarming dump files found: " + segFiles.length);

            initReadsRate();

            needThrottling.set(readsRates[readsRates.length - 1] == Long.MAX_VALUE);

            if (needThrottling.get() && log.isInfoEnabled())
                log.info("Detected need to throttle warming up.");

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

            CountDownFuture completeFut = new CountDownFuture(segFiles.length);

            AtomicInteger pagesWarmed = new AtomicInteger();

            for (File segFile : segFiles) {
                Runnable dumpReader = () -> {
                    try {
                        pagesWarmed.addAndGet(segmentLdr.load(segFile, needThrottling));

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

            if (segFiles.length > 0)
                completeFut.get();

            long warmingUpTime = U.currentTimeMillis() - startTs;

            if (log.isInfoEnabled()) {
                log.info("Prewarming of DataRegion [name=" + dataRegName + "] finished in " +
                    warmingUpTime + " ms, pages warmed: " + pagesWarmed);
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
            if (workers != null)
                Arrays.asList(workers).forEach(ExecutorService::shutdown);

            prewarmThread = null;
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

    /**
     * @param onStopping On stopping.
     */
    private void dumpLoadedPagesIds(boolean onStopping) {
        if (prewarmThread != null) {
            if (onStopping)
                U.warn(log, "Attempt dump of loaded pages IDs on stopping while prewarming process is running!");

            return;
        }

        loadedPagesDumpInProgress = true;

        try {
            if (!onStopping && stopping)
                return;

            if (log.isInfoEnabled())
                log.info("Starting dump of loaded pages IDs of DataRegion [name=" + dataRegName + "]");

            final ConcurrentMap<Long, Segment> updated = new ConcurrentHashMap<>();

            long startTs = U.currentTimeMillis();

            pageMem.forEachAsync((fullId, touchTs) -> {
                if (prewarmCfg.isIndexesOnly() &&
                    PageIdUtils.partId(fullId.pageId()) != PageIdAllocator.INDEX_PARTITION)
                    return;

                Segment seg = !onStopping && stopping ?
                    updated.get(
                        Segment.key(fullId.groupId(), fullId.pageId())) :
                    updated.computeIfAbsent(
                        Segment.key(fullId.groupId(), fullId.pageId()),
                        key -> getSegment(key).resetModifiedAndGet());

                if (seg != null)
                    seg.addPageIdx(fullId.pageId(), touchTs);
            }).get();

            int segUpdated = 0;

            for (Segment seg : updated.values()) {
                if (!onStopping && stopping && seg.modified)
                    continue;

                try {
                    updateSegment(seg);

                    segUpdated++;
                }
                catch (IOException e) {
                    throw new IgniteCheckedException(e);
                }
            }

            long dumpTime = U.currentTimeMillis() - startTs;

            if (log.isInfoEnabled()) {
                log.info("Dump of loaded pages IDs of DataRegion [name=" + dataRegName + "] finished in " +
                    dumpTime + " ms, segments updated: " + segUpdated);
            }
        }
        catch (IgniteCheckedException e) {
            U.warn(log, "Dump of loaded pages IDs for DataRegion [name=" + dataRegName + "] failed", e);
        }
        finally {
            loadedPagesDumpInProgress = false;
        }
    }

    /**
     * @param key Segment key.
     */
    private Segment getSegment(long key) {
        return segments.computeIfAbsent(key, Segment::new);
    }

    /**
     * @param segFile Segment file.
     */
    private int[] loadPageIndexes(File segFile) throws IOException {
        try (FileIO io = new RandomAccessFileIO(segFile, StandardOpenOption.READ)) {
            int[] pageIdxArr = new int[(int)(io.size() / Integer.BYTES)];

            byte[] intBytes = new byte[Integer.BYTES];

            for (int i = 0; i < pageIdxArr.length; i++) {
                io.read(intBytes, 0, intBytes.length);

                pageIdxArr[i] = U.bytesToInt(intBytes, 0);
            }

            return pageIdxArr;
        }
    }

    /**
     * @param seg Segment.
     */
    private void updateSegment(Segment seg) throws IOException {
        long[] pageIdxTsArr = seg.pageIdxTsArr;

        seg.pageIdxTsArr = null;

        File segFile = new File(dumpDir, seg.fileName());

        if (pageIdxTsArr.length == 0) {
            if (!segFile.delete())
                U.warn(log, "Failed to delete prewarming dump file: " + segFile.getName());

            return;
        }

        int heatTimeQuantum = prewarmCfg.getHeatTimeQuantum();

        int curTime = (int)(U.currentTimeMillis() / 1000 / heatTimeQuantum);

        for (int i = 0; i < pageIdxTsArr.length; i++) {
            long pageIdxTs = pageIdxTsArr[i];

            pageIdxTsArr[i] = (pageIdxTs & PAGE_IDX_MASK) |
                ((curTime - (pageIdxTs >> PAGE_IDX_SIZE) / heatTimeQuantum) << PAGE_IDX_SIZE);
        }

        Arrays.sort(pageIdxTsArr);

        try (FileIO io = new RandomAccessFileIO(segFile,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING)) {

            byte[] chunk = new byte[Integer.BYTES * 1024];

            int chunkPtr = 0;

            for (long pageIdxTs : pageIdxTsArr) {
                int pageIdx = (int)(pageIdxTs & PAGE_IDX_MASK);

                chunkPtr = U.intToBytes(pageIdx, chunk, chunkPtr);

                if (chunkPtr == chunk.length) {
                    io.write(chunk, 0, chunkPtr);
                    io.force();

                    chunkPtr = 0;
                }
            }

            if (chunkPtr > 0) {
                io.write(chunk, 0, chunkPtr);
                io.force();
            }
        }
    }

    /**
     *
     */
    private static class Segment {
        /** File extension. */
        private static final String FILE_EXT = ".seg";

        /** File name length. */
        private static final int FILE_NAME_LENGTH = 12;

        /** Group ID prefix length. */
        private static final int GRP_ID_PREFIX_LENGTH = 8;

        /** Group ID key mask. */
        private static final long GRP_ID_MASK = ~(-1L << 32);

        /** File name pattern. */
        private static final Pattern FILE_NAME_PATTERN = Pattern.compile("[0-9A-Fa-f]{" + FILE_NAME_LENGTH + "}\\" + FILE_EXT);

        /** File filter. */
        private static final FileFilter FILE_FILTER = file -> !file.isDirectory() && FILE_NAME_PATTERN.matcher(file.getName()).matches();

        /** Id count field updater. */
        private static final AtomicIntegerFieldUpdater<Segment> idCntUpd = AtomicIntegerFieldUpdater.newUpdater(
            Segment.class, "idCnt");

        /** Page index array pointer field updater. */
        private static final AtomicIntegerFieldUpdater<Segment> pageIdxIUpd = AtomicIntegerFieldUpdater.newUpdater(
            Segment.class, "pageIdxI");

        /** Key. */
        final long key;

        /** Id count. */
        volatile int idCnt;

        /** Modified flag. */
        volatile boolean modified;

        /** Page index and timestamp array. */
        volatile long[] pageIdxTsArr;

        /** Page index array pointer. */
        volatile int pageIdxI;

        // TODO Collection of sub-segments if idCnt was dramatically grown.

        /**
         * @param key Key.
         */
        Segment(long key) {
            this.key = key;
        }

        /**
         *
         */
        String fileName() {
            SB b = new SB();

            String keyHex = Long.toHexString(key);

            for (int i = keyHex.length(); i < FILE_NAME_LENGTH; i++)
                b.a('0');

            return b.a(keyHex).a(FILE_EXT).toString();
        }

        /**
         *
         */
        void incCount() {
            modified = true;

            idCntUpd.incrementAndGet(this);
        }

        /**
         *
         */
        void decCount() {
            modified = true;

            idCntUpd.decrementAndGet(this);
        }

        /**
         * @param pageId Page id.
         * @param touchTs Touch timestamp.
         */
        void addPageIdx(long pageId, long touchTs) {
            int ptr = pageIdxIUpd.getAndIncrement(this);

            if (ptr < pageIdxTsArr.length)
                pageIdxTsArr[ptr] = ((touchTs / 1000) << PAGE_IDX_SIZE) | PageIdUtils.pageIndex(pageId);
        }

        /**
         * Returns {@code null} if {@link #modified} is {@code false}, otherwise resets {@link #pageIdxI},
         * creates new {@link #pageIdxTsArr} and returns {@code this}.
         */
        Segment resetModifiedAndGet() {
            if (!modified)
                return null;

            modified = false;

            pageIdxI = 0;

            pageIdxTsArr = new long[idCnt];

            return this;
        }

        /**
         * @param grpId Group id.
         * @param pageId Page id.
         */
        static long key(long grpId, long pageId) {
            return (((grpId & GRP_ID_MASK) << PART_ID_SIZE)) + PageIdUtils.partId(pageId);
        }
    }

    /**
     *
     */
    private class LoadedPagesIdsDumpWorker extends GridWorker {
        /** */
        private static final String NAME_SUFFIX = "-loaded-pages-ids-dump-worker";

        /**
         * Default constructor.
         */
        LoadedPagesIdsDumpWorker() {
            super(ctx.igniteInstanceName(), dataRegName + NAME_SUFFIX, PageMemoryPrewarmingImpl.this.log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!stopping && !isCancelled()) {
                Thread.sleep(prewarmCfg.getRuntimeDumpDelay());

                if (stopping)
                    break;

                dumpLoadedPagesIds(false);
            }
        }
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
         * Loads pages which pageIDs saved in dump file into memory.
         * If workers are setted, they will be used for multithreaded loading.
         *
         * @param segFile Dump file to be load.
         * @param needThrottling Need throttling object.
         * @return Count of warmed pages.
         */
        public int load(File segFile, AtomicBoolean needThrottling) {
            AtomicBoolean del = new AtomicBoolean(false);

            AtomicInteger pagesWarmed = new AtomicInteger(0);

            try {
                if (stopping || stopPrewarm)
                    return pagesWarmed.get();

                int partId = Integer.parseInt(segFile.getName().substring(
                    Segment.GRP_ID_PREFIX_LENGTH,
                    Segment.FILE_NAME_LENGTH), 16);

                if (prewarmCfg.isIndexesOnly() && partId != PageIdAllocator.INDEX_PARTITION)
                    return pagesWarmed.get();

                int grpId = (int)Long.parseLong(segFile.getName().substring(0, Segment.GRP_ID_PREFIX_LENGTH), 16);

                int[] pageIdxArr = loadPageIndexes(segFile);

                // Arrays.sort(pageIdxArr); // TODO no need after sorting at dump phase!

                CountDownFuture completeFut = new CountDownFuture(pageIdxArr.length);

                boolean multithreaded = workers != null && workers.length != 0;

                for (int pageIdx : pageIdxArr) {
                    if (stopping || stopPrewarm) {
                        del.set(true);

                        completeFut.onDone();

                        break;
                    }

                    long pageId = PageIdUtils.pageId(partId, pageIdx);

                    Runnable pageWarmer = () -> {
                        try {
                            if (loadPage(grpId, pageId)) {
                                pagesWarmed.incrementAndGet();

                                incThrottleCounter();
                            } else
                                del.set(true);

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

                if (pageIdxArr.length > 0)
                    completeFut.get();
            }
            catch (IOException | NumberFormatException e) {
                U.error(log, "Failed to read prewarming dump file: " + segFile.getName(), e);

                del.set(true);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to load prewarming dump file: " + segFile.getName(), e);

                throw new IgniteException(e);
            }
            finally {
                if (del.get() && !segFile.delete())
                    U.warn(log, "Failed to delete prewarming dump file: " + segFile.getName());
            }

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
