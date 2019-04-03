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
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIO;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class FilePageIdsDumpStore implements PageIdsDumpStore {
    /** Segment file extension. */
    private static final String SEG_FILE_EXT = ".seg";

    /** File name length. */
    private static final int FILE_NAME_LENGTH = 12;

    /** Group ID prefix length. */
    private static final int GRP_ID_PREFIX_LENGTH = 8;

    /** File name pattern. */
    private static final Pattern FILE_NAME_PATTERN = Pattern.compile("[0-9A-Fa-f]{" + FILE_NAME_LENGTH + "}\\" + SEG_FILE_EXT);

    /** File filter. */
    private static final FileFilter FILE_FILTER = file -> !file.isDirectory() && FILE_NAME_PATTERN.matcher(file.getName()).matches();

    /** Small page count threshold. */
    private static final int SMALL_PAGE_COUNT_THRESHOLD = 4;

    /** Partition block overhead: cacheId(4) + partId(2) + count(4). */
    private static final int PARTITION_BLOCK_OVERHEAD = 10;

    /** Partition item size: pageIdx(4). */
    private static final int PARTITION_ITEM_SIZE = 4;

    /** Cache block overhead: zeroCacheId(4) + cacheId(4) + count(4). */
    private static final int CACHE_BLOCK_OVERHEAD = 12;

    /** Cache item size: partId(2) + pageIdx(4). */
    private static final int CACHE_ITEM_SIZE = 6;

    /** Page IDs dump directory. */
    private final File dumpDir;

    /** */
    private final IgniteLogger log;

    /** Active dump file. */
    private File dumpFile;

    /** Active dump file IO. */
    private volatile FileIO dumpIO;

    /**
     * @param dumpDir Page IDs dump directory.
     */
    public FilePageIdsDumpStore(File dumpDir, IgniteLogger log) {
        this.dumpDir = dumpDir;
        this.log = log.getLogger(FilePageIdsDumpStore.class);
    }

    /** {@inheritDoc} */
    @Override public String createDump() {
        if (dumpIO != null)
            throw new IllegalStateException("Attempt to create new dump while other one still active");

        long ts = U.currentTimeMillis();

        while (true) {
            String dumpId = String.valueOf(ts);

            if (new File(dumpDir, dumpId + ".dump").exists()) {
                ts += 10;

                continue;
            }

            try {
                dumpIO = new RandomAccessFileIO(dumpFile = new File(dumpDir, dumpId + ".dump.new"),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);

                return dumpId;
            }
            catch (IOException e) {
                throw new IgniteException("Dump creation failed", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishDump() {
        FileIO dumpIO = this.dumpIO;

        if (dumpIO == null)
            throw new IllegalStateException("No active dump found");

        try {
            dumpIO.force();
            dumpIO.close();
        }
        catch (IOException e) {
            throw new IgniteException("Dump finish failed", e);
        }

        assert dumpFile != null
            && dumpFile.exists()
            && dumpFile.getName().endsWith(".new");

        try {
            String dumpFileName = dumpFile.getName();

            if (!dumpFile.renameTo(new File(dumpDir, dumpFileName.substring(0, dumpFileName.length() - 4))))
                log.warning("Failed rename of dump file [name=" + dumpFileName + "]");
        }
        finally {
            dumpFile = null;

            this.dumpIO = null;
        }
    }

    /** {@inheritDoc} */
    @Override public void save(Iterable<Partition> partitions) {
        FileIO dumpIO = this.dumpIO;

        if (dumpIO == null)
            throw new IllegalStateException("No active dump found");

        Map<Integer, int[]> cacheSmallPartsCounts = cacheSmallPartsCounts(partitions);

        int smallPartsCnt = cacheSmallPartsCounts.values().stream()
            .mapToInt(FilePageIdsDumpStore::totalPartsCount)
            .sum();

        Set<Partition> smallParts = new HashSet<>(smallPartsCnt);

        for (Partition part : partitions) {
            int pageCnt = part.pageIndexes().length;

            if (pageCnt == 0)
                continue;

            if (pageCnt <= SMALL_PAGE_COUNT_THRESHOLD && cacheSmallPartsCounts.containsKey(part.cacheId())) {
                smallParts.add(part);

                continue;
            }


        }
    }

    private void savePartition(Partition part, FileIO io) throws IOException {
        byte[] chunk = new byte[Integer.BYTES * 1024];

        int chunkPtr = U.intToBytes(part.cacheId(), chunk, 0);
        chunkPtr = U.shortToBytes((short)part.id(), chunk, chunkPtr);
        chunkPtr = U.intToBytes(part.pageIndexes().length, chunk, chunkPtr);

        for (int pageIdx : part.pageIndexes()) {
            chunkPtr = U.intToBytes(pageIdx, chunk, chunkPtr);

            if (chunkPtr == chunk.length) {
                io.write(chunk, 0, chunkPtr);

                chunkPtr = 0;
            }
        }
    }

    /**
     * @param partitions Partitions.
     * @return Map which keys are cache IDs and values are arrays of counts of partitions
     * with page counts from 1 (first array value) to {@link #SMALL_PAGE_COUNT_THRESHOLD} (last array value) accordingly.
     */
    private Map<Integer, int[]> cacheSmallPartsCounts(Iterable<Partition> partitions) {
        ConcurrentMap<Integer, int[]> cacheSmallPartsCounts = new ConcurrentHashMap<>();

        for (Partition part : partitions) {
            int pageCnt = part.pageIndexes().length;

            if (pageCnt == 0 || pageCnt > SMALL_PAGE_COUNT_THRESHOLD)
                continue;

            int[] counts = cacheSmallPartsCounts.computeIfAbsent(part.cacheId(), key -> new int[SMALL_PAGE_COUNT_THRESHOLD]);

            counts[pageCnt - 1]++;
        }

        cacheSmallPartsCounts.values().removeIf(FilePageIdsDumpStore::usePartitionBlocks);

        return cacheSmallPartsCounts;
    }

    /**
     * @param pageCnt Page count.
     */
    private static int partitionBlockSize(int pageCnt) {
        return PARTITION_BLOCK_OVERHEAD + pageCnt * PARTITION_ITEM_SIZE;
    }

    /**
     * @param pageCnt Page count.
     */
    private static int cacheBlockSize(int pageCnt) {
        return CACHE_BLOCK_OVERHEAD + pageCnt * CACHE_ITEM_SIZE;
    }

    /**
     * @param stats Stats.
     */
    private static int totalPartsCount(int[] stats) {
        int res = 0;

        for (int i = 0; i < stats.length; i++)
            res += (i + 1) * stats[i];

        return res;
    }

    /**
     * @param stats Stats.
     */
    private static boolean usePartitionBlocks(int[] stats) {
        int pageCntTotal = 0;
        int partBlocksTotalSize = 0;

        for (int i = 0; i < stats.length; i++) {
            pageCntTotal += stats[i] * (i + 1);

            partBlocksTotalSize += partitionBlockSize(i + 1) * stats[i];
        }

        return partBlocksTotalSize <= cacheBlockSize(pageCntTotal);
    }

    /** {@inheritDoc} */
    @Override public void forEach(FullPageIdConsumer consumer) {

    }

    /** {@inheritDoc} */
    @Override public void forEach(FullPageIdConsumer consumer, String dumpId) {

    }

    /** {@inheritDoc} */
    @Override public void clear() {

    }

    /** {@inheritDoc} */
    public Iterable<Zone> zones() { // FIXME remove!
        File[] segFiles = dumpDir.listFiles(FILE_FILTER);

        if (segFiles == null) {
            if (log.isInfoEnabled())
                log.info("Saved prewarming dump files not found!");

            return Collections.emptyList();
        }

        if (log.isInfoEnabled())
            log.info("Saved prewarming dump files found: " + segFiles.length);

        List<PageIdsDumpStore.Partition> partitions = new ArrayList<>(segFiles.length);

        for (File segFile : segFiles) {
            try {
                int partId = Integer.parseInt(segFile.getName().substring(
                    GRP_ID_PREFIX_LENGTH,
                    FILE_NAME_LENGTH), 16);

                int grpId = (int)Long.parseLong(segFile.getName().substring(0, GRP_ID_PREFIX_LENGTH), 16);

                partitions.add(new PageIdsDumpStore.Partition(grpId, partId, () -> {
                    try {
                        return loadPageIndexes(segFile);
                    }
                    catch (IOException e) {
                        U.error(log, "Failed to read prewarming dump file: " + segFile.getName(), e);

                        throw new IgniteException(e);
                    }
                }));
            }
            catch (NumberFormatException e) {
                U.error(log, "Invalid prewarming dump file name: " + segFile.getName(), e);
            }
        }

        return Collections.singleton(new PageIdsDumpStore.Zone(partitions, null)); // FIXME
    }

    /**
     * @param partKey Partition key.
     */
    private String partFileName(long partKey) {
        SB b = new SB();

        String keyHex = Long.toHexString(partKey);

        for (int i = keyHex.length(); i < FILE_NAME_LENGTH; i++)
            b.a('0');

        return b.a(keyHex).a(SEG_FILE_EXT).toString();
    }

    /**
     * @param segFile Partition file.
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
     *
     */
    private class DumpContext {
        /** */
        private final String dumpId;

        /** */
        private final File dumpFile;

        /** */
        private final FileIO dumpIO;

        /** */
        private final byte[] chunk;

        /** */
        private int chunkPtr;

        /**
         * @param dumpId Dump id.
         * @param isNew Is new.
         */
        DumpContext(String dumpId, boolean isNew) throws IOException {
            this.dumpId = dumpId;

            dumpFile = new File(dumpDir, dumpId + (isNew ? ".dump.new" : ".dump"));

            dumpIO = isNew ?
                new RandomAccessFileIO(dumpFile,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING) :
                new RandomAccessFileIO(dumpFile, StandardOpenOption.READ);

            chunk = new byte[isNew ? Integer.BYTES * 1024 : Integer.BYTES];
        }
    }
}
