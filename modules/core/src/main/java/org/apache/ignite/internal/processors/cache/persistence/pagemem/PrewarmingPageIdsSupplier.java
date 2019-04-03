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
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.LongPredicate;
import java.util.function.Supplier;
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
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.thread.IgniteThread;

import static org.apache.ignite.internal.pagemem.PageIdUtils.PAGE_IDX_MASK;
import static org.apache.ignite.internal.pagemem.PageIdUtils.PAGE_IDX_SIZE;
import static org.apache.ignite.internal.pagemem.PageIdUtils.PART_ID_SIZE;

/**
 *
 */
public class PrewarmingPageIdsSupplier implements LifecycleAware, LoadedPagesTracker,
    Supplier<List<List<T3<Integer, Integer, Supplier<int[]>>>>> {
    /** Prewarming page IDs dump directory name. */
    private static final String PREWARM_DUMP_DIR = "prewarm";

    /** Data region name. */
    private final String dataRegName;

    /** Prewarming configuration. */
    private final PrewarmingConfiguration prewarmCfg;

    /** Cache shared context. */
    private final GridCacheSharedContext<?, ?> ctx;

    /** */
    private final IgniteLogger log;

    /** */
    private final ConcurrentMap<Long, Partition> segments = new ConcurrentHashMap<>();

    /** Dump worker. */
    private volatile LoadedPagesIdsDumpWorker dumpWorker;

    /** Page memory. */
    private volatile PageMemoryEx pageMem;

    /** Page IDs store. */
    private volatile PageIdsDumpStore pageIdsStore;

    /** Stopping flag. */
    private volatile boolean stopping;

    /** Loaded pages dump in progress flag. */
    private volatile boolean loadedPagesDumpInProgress;


    /**
     * @param dataRegName Data region name.
     * @param prewarmCfg Prewarming configuration.
     */
    public PrewarmingPageIdsSupplier(
        String dataRegName,
        PrewarmingConfiguration prewarmCfg,
        GridCacheSharedContext<?, ?> ctx) {
        this.dataRegName = dataRegName;
        this.prewarmCfg = prewarmCfg;
        this.ctx = ctx;
        this.log = ctx.logger(PrewarmingPageIdsSupplier.class);
    }

    /**
     *
     */
    void initStore() throws IgniteException {
        IgnitePageStoreManager store = ctx.pageStore();

        assert store instanceof FilePageStoreManager : "Invalid page store manager was created: " + store;

        File dumpDir = Paths.get(((FilePageStoreManager)store).workDir().getAbsolutePath(), PREWARM_DUMP_DIR).toFile();

        if (!U.mkdirs(dumpDir))
            throw new IgniteException("Could not create directory for prewarming data: " + dumpDir);

        pageIdsStore = new FilePageIdsDumpStore(dumpDir, log);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        if (prewarmCfg.getRuntimeDumpDelay() > PrewarmingConfiguration.RUNTIME_DUMP_DISABLED) {
            dumpWorker = new LoadedPagesIdsDumpWorker();

            new IgniteThread(dumpWorker).start();
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        stopping = true;

        LoadedPagesIdsDumpWorker dumpWorker = this.dumpWorker;

        if (dumpWorker != null) {
            dumpWorker.wakeUp();

            try {
                dumpWorker.join();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new IgniteException(e);
            }
        }
        else
            dumpLoadedPagesIds(true);
    }

    /** {@inheritDoc} */
    @Override public void onPageLoad(int grpId, long pageId) {
        if (prewarmCfg.isIndexesOnly() &&
            PageIdUtils.partId(pageId) != PageIdAllocator.INDEX_PARTITION)
            return;

        getSegment(Partition.key(grpId, pageId)).incCount();
    }

    /** {@inheritDoc} */
    @Override public void onPageUnload(int grpId, long pageId) {
        if (prewarmCfg.isIndexesOnly() &&
            PageIdUtils.partId(pageId) != PageIdAllocator.INDEX_PARTITION)
            return;

        getSegment(Partition.key(grpId, pageId)).decCount();
    }

    /** {@inheritDoc} */
    @Override public void onPageEvicted(int grpId, long pageId) {
        onPageUnload(grpId, pageId);
    }

    /** {@inheritDoc} */
    @Override public List<List<T3<Integer, Integer, Supplier<int[]>>>> get() {
        List<List<T3<Integer, Integer, Supplier<int[]>>>> zones = new ArrayList<>();

        for (PageIdsDumpStore.Zone zone : pageIdsStore.zones()) {
            
            for (PageIdsDumpStore.Partition partition : zone.partitions()) {

            }
        }

        return zones;
    }

    /**
     * @param pageMem Page memory.
     */
    void pageMemory(PageMemoryEx pageMem) {
        this.pageMem = pageMem;
    }

    /**
     *
     */
    private void dumpLoadedPagesIds() {
        try {
            if (log.isInfoEnabled())
                log.info("Starting dump of loaded pages IDs of DataRegion [name=" + dataRegName + "]");

            final long startTs = U.currentTimeMillis();

            final ConcurrentMap<Long, Partition> partMap = new ConcurrentHashMap<>();

            final LongPredicate skipPageIdPred = (pageId) ->
                prewarmCfg.isIndexesOnly() &&
                PageIdUtils.partId(pageId) != PageIdAllocator.INDEX_PARTITION;

            pageMem.forEachAsync((grpId, pageId, touchTs) -> {
                if (skipPageIdPred.test(pageId))
                    return;

                long segmentKey = Partition.key(grpId, pageId);

                Partition part = partMap.computeIfAbsent(segmentKey, Partition::new);

                part.incCount();
            }).get();

            pageMem.forEachAsync((grpId, pageId, touchTs) -> {
                if (skipPageIdPred.test(pageId))
                    return;

                long segmentKey = Partition.key(grpId, pageId);

                Partition seg = partMap.computeIfPresent(segmentKey, (key, seg0) -> {
                    seg0.resetModifiedAndGet(); // FIXME

                    return seg0;
                });

                if (seg != null)
                    seg.addPageIdx(pageId, startTs - touchTs);
            }).get();

            Collection<Partition> parts = partMap.values();

            long pageIdsCnt = 0;

            for (Partition seg : parts) {
                pageIdsCnt += seg.pageIdxI;

                seg.cnt = 0;
            }

            double dumpPercentage = prewarmCfg.getDumpPercentage();

            assert dumpPercentage > 0 : "Dump percentage must be greater than 0";

            if (dumpPercentage < 1.0) {
                parts.forEach(part -> Arrays.sort(part.pageIdxTsArr, 0, part.pageIdxI));

                long pageIdsLeft = (long)Math.floor(pageIdsCnt * dumpPercentage);

                Partition head = Collections.min(parts, Partition.byCoolingAtCnt);

                while (pageIdsLeft > 0) {
                    Partition head0 = head;

                    Optional<Partition> nextOpt = parts.stream().filter(seg -> seg != head0).min(Partition.byCoolingAtCnt);

                    if (!nextOpt.isPresent()) {
                        head.addCount(Math.min((int)pageIdsLeft, head.pageIdxI - head.cnt));

                        pageIdsLeft = 0;
                    }
                    else {
                        Partition next = nextOpt.get();

                        while (Partition.byCoolingAtCnt.compare(head, next) <= 0) {
                            head.incCount();

                            if (--pageIdsLeft == 0)
                                break;
                        }

                        head = next;
                    }
                }

                
            }

            // long
            // TODO sort part idx arrays and parts!
        }
        catch (IgniteCheckedException e) {
            U.warn(log, "Dump of loaded pages IDs for DataRegion [name=" + dataRegName + "] failed", e);
        }
        finally {
            loadedPagesDumpInProgress = false; // FIXME ?
        }
    }

    /**
     * @param onStopping On stopping.
     */
    private void dumpLoadedPagesIds(boolean onStopping) {
        /*if (prewarmThread != null) { // FIXME
            if (onStopping)
                U.warn(log, "Attempt dump of loaded pages IDs on stopping while prewarming process is running!");

            return;
        }*/

        loadedPagesDumpInProgress = true;

        try {
            if (!onStopping && stopping)
                return;

            if (log.isInfoEnabled())
                log.info("Starting dump of loaded pages IDs of DataRegion [name=" + dataRegName + "]");

            final ConcurrentMap<Long, Partition> updated = new ConcurrentHashMap<>();

            final long startTs = U.currentTimeMillis();

            pageMem.forEachAsync((grpId, pageId, touchTs) -> {
                if (prewarmCfg.isIndexesOnly() &&
                    PageIdUtils.partId(pageId) != PageIdAllocator.INDEX_PARTITION)
                    return;

                long segmentKey = Partition.key(grpId, pageId);

                Partition seg = !onStopping && stopping ?
                    updated.get(segmentKey) :
                    updated.computeIfAbsent(segmentKey,
                        key -> getSegment(key).resetModifiedAndGet());

                if (seg != null)
                    seg.addPageIdx(pageId, touchTs); // FIXME use startTs - touchTs instead touchTs!
            }).get();

            // TODO use prewarmCfg.getDumpPercentage() for dump limit

            int segUpdated = 0;

            for (Partition seg : updated.values()) {
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
     * @param key Partition key.
     */
    private Partition getSegment(long key) {
        return segments.computeIfAbsent(key, Partition::new);
    }

    /**
     * @param seg Partition.
     */
    private void updateSegment(Partition seg) throws IOException {
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
    private static class Partition {

        /** Group ID key mask. */
        private static final long GRP_ID_MASK = ~(-1L << 32);

        /** Id count field updater. */
        private static final AtomicIntegerFieldUpdater<Partition> cntUpd = AtomicIntegerFieldUpdater.newUpdater(
            Partition.class, "cnt");

        /** Page index array pointer field updater. */
        private static final AtomicIntegerFieldUpdater<Partition> pageIdxIUpd = AtomicIntegerFieldUpdater.newUpdater(
            Partition.class, "pageIdxI");

        /** Comparator by page index at count. */
        private static final Comparator<Partition> byCoolingAtCnt = Comparator.comparingInt(o -> o.cooling(o.cnt));

        /** Key. */
        final long key;

        /** Id count. */
        volatile int cnt;

        /** Modified flag. */
        volatile boolean modified;

        /** Page index and timestamp array. */
        volatile long[] pageIdxTsArr;

        /** Page index array pointer. */
        volatile int pageIdxI;

        // TODO Collection of sub-segments if cnt was dramatically grown.

        /**
         * @param key Key.
         */
        Partition(long key) {
            this.key = key;
        }

        /**
         *
         */
        int cacheId() {
            return (int)(key >> PART_ID_SIZE);
        }

        /**
         *
         */
        void incCount() {
            modified = true;

            cntUpd.incrementAndGet(this);
        }

        /**
         *
         */
        void decCount() {
            modified = true;

            cntUpd.decrementAndGet(this);
        }

        /**
         * @param delta Delta.
         */
        void addCount(int delta) {
            modified = true;

            cntUpd.addAndGet(this, delta);
        }

        /**
         * @param pageId Page ID.
         * @param cooling Cooling.
         */
        void addPageIdx(long pageId, long cooling) {
            int ptr = pageIdxIUpd.getAndIncrement(this);

            if (ptr < pageIdxTsArr.length)
                pageIdxTsArr[ptr] = ((cooling / 1000) << PAGE_IDX_SIZE) | PageIdUtils.pageIndex(pageId);
        }

        /**
         * @param ptr {@link #pageIdxTsArr} pointer.
         */
        int cooling(int ptr) {
            return (int)(pageIdxTsArr[ptr] >>> PAGE_IDX_SIZE);
        }

        /**
         * Returns {@code null} if {@link #modified} is {@code false}, otherwise resets {@link #pageIdxI},
         * creates new {@link #pageIdxTsArr} and returns {@code this}.
         */
        Partition resetModifiedAndGet() {
            if (!modified)
                return null;

            modified = false;

            pageIdxI = 0;

            pageIdxTsArr = new long[cnt];

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

        /** */
        private final Object monitor = new Object();

        /**
         * Default constructor.
         */
        LoadedPagesIdsDumpWorker() {
            super(ctx.igniteInstanceName(), dataRegName + NAME_SUFFIX, PrewarmingPageIdsSupplier.this.log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!stopping && !isCancelled()) {

                long timeout;
                long wakeTs = prewarmCfg.getRuntimeDumpDelay() + U.currentTimeMillis();

                synchronized (monitor) {
                    while (!stopping && (timeout = wakeTs - U.currentTimeMillis()) > 0)
                        monitor.wait(timeout);
                }

                dumpLoadedPagesIds(false);
            }
        }

        /**
         *
         */
        void wakeUp() {
            synchronized (monitor) {
                monitor.notify();
            }
        }
    }
}
