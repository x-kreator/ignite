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

package org.apache.ignite.internal.processors.cache.index;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CheckpointEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgnitePredicate;

import static org.apache.ignite.events.EventType.EVT_CHECKPOINT_LOADED;
import static org.apache.ignite.events.EventType.EVT_CHECKPOINT_REMOVED;
import static org.apache.ignite.events.EventType.EVT_CHECKPOINT_SAVED;

/**
 *
 */
public class DynamicIndexPartialBuildSelfTest extends DynamicIndexAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration serverConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg = super.serverConfiguration(idx);

        cfg.getDataStorageConfiguration()
            .setPageSize(2048)
            .setWalBufferSize(128 * 1024)
            .setWalSegmentSize(512 * 1024)
            .setCheckpointFrequency(1000)
            .getDefaultDataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(2048L * 1024 * 1024)
                .setCheckpointPageBufferSize(512 * 1024)
        ;

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testIndexPartialBuild() throws Exception {
        final IgniteEx srv1 = startGrid(serverConfiguration(1));

        final AtomicInteger savedCnt = new AtomicInteger();
        final AtomicInteger loadedCnt = new AtomicInteger();
        final AtomicInteger rmvCnt = new AtomicInteger();

        srv1.events().remoteListen(null, new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                assert evt instanceof CheckpointEvent;

                CheckpointEvent e = (CheckpointEvent) evt;

                info("Checkpoint event: " + e);

                switch (evt.type()) {
                    case EVT_CHECKPOINT_SAVED: {
                        savedCnt.incrementAndGet();

                        break;
                    }

                    case EVT_CHECKPOINT_LOADED: {
                        loadedCnt.incrementAndGet();

                        break;
                    }

                    case EVT_CHECKPOINT_REMOVED: {
                        rmvCnt.incrementAndGet();

                        break;
                    }
                }

                return true;
            }
        }, EVT_CHECKPOINT_SAVED, EVT_CHECKPOINT_LOADED, EVT_CHECKPOINT_REMOVED);

        srv1.cluster().active(true);

        srv1.context().cache().dynamicStartSqlCache(cacheConfiguration()).get();

        Thread cpWaitThread = new Thread() {
            @Override public void run() {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        srv1.context().cache().context().database().waitForCheckpoint("too many dirty pages");

                        System.out.println(">>> Checkpoint happens!");
                    }
                    catch (IgniteCheckedException e) {
                        error("Failed to wait for checkpoint", e);
                    }
                }
            }
        };
        cpWaitThread.setDaemon(true);
        cpWaitThread.start();

        forceCheckpoint(srv1);
        /*IgniteCache<BinaryObject, BinaryObject> cache = srv1.cache(CACHE_NAME).withKeepBinary();

        for (int i = 1; i <= 50_000; i++) {
            BinaryObject key = key(srv1, i);
            BinaryObject val = value(srv1, i);

            cache.put(key, val);

            if (i % 1000 == 0)
                System.out.println(">>> " + i + " entries put");
        }*/
        put(srv1, 0, 100_000);

        QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1));

        final IgniteInternalFuture<?> idxFut =
            queryProcessor(srv1).dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TBL_NAME, idx, false, 0);

        idxFut.get(); //

        cpWaitThread.interrupt();

        doSleep(2000);
    }
}
