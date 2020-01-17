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

package hang;

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class StressTest3 extends GridCommonAbstractTest {
    /** */
    private String savedIgniteQuietProp;
    /** */
    private String savedIgniteUpdateNtfProp;
    /** */
    private final AtomicInteger gridIdxSeq = new AtomicInteger();
    /** */
    private final ThreadLocal<Boolean> client = ThreadLocal.withInitial(() -> false);


    /** {@inheritDoc} */
    @Override public void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        updateProperties();

        client.set(false);

        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.ENTRY_LOCK);
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 2_000_000_000;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        log.info("gridIdxSeq value: " + gridIdxSeq.get());

        stopAllGrids();

        restoreProperties();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(client.get());
        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).setBackups(1));
/*
        cfg.setIgniteInstanceName(IGNITE_NAME_PREFIX + id0 + (client ? "_client" : "_server"));
 */
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testConcurrentClientAndServerNodesRestart() throws Exception {
        IgniteEx[] ignites = new IgniteEx[4];

        for (int i = 0; i < ignites.length; i++)
            ignites[i] = startGrid(i);

        //CyclicBarrier barrier = new CyclicBarrier(2);
        Phaser phaser = new Phaser(2);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new ServerNodesRestartTask(ignites, phaser), "restart-servers");

        client.set(true);

        while (!fut.isDone()) {
            System.out.println(">>> Starting new client...");

            try (IgniteEx client = startGrid(ignites.length)) {
                log.info("r/a #1.1: " + phaser.getRegisteredParties() + "/" + phaser.getArrivedParties());
                phaser.arriveAndAwaitAdvance();
                log.info("r/a #1.2: " + phaser.getRegisteredParties() + "/" + phaser.getArrivedParties());
            }
            catch (BrokenBarrierException ignored) {
                break;
            }
        }

        U.error(log, "Error:", fut.error());
        //fut.get();
    }

    /**
     *
     */
    private void updateProperties() {
        savedIgniteQuietProp = System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "false");
        savedIgniteUpdateNtfProp = System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, "false");
    }

    /**
     *
     */
    private void restoreProperties() {
        if (savedIgniteQuietProp != null)
            System.setProperty(IgniteSystemProperties.IGNITE_QUIET, savedIgniteQuietProp);

        if (savedIgniteUpdateNtfProp != null)
            System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, savedIgniteUpdateNtfProp);
    }

    /**
     *
     */
    class ServerNodesRestartTask implements Runnable {
        /** */
        private final IgniteEx[] ignites;
        /** */
        private final Phaser phaser;
        /** */
        private final Random rnd = new Random();

        /**
         * @param ignites Ignites.
         */
        ServerNodesRestartTask(IgniteEx[] ignites, Phaser phaser) {
            this.ignites = ignites;
            this.phaser = phaser;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            int gridIdx;

            while (true) {
                gridIdx = rnd.nextInt(4);

                try {
                    log.info("r/a #2.1: " + phaser.getRegisteredParties() + "/" + phaser.getArrivedParties());
                    phaser.arriveAndAwaitAdvance();
                    log.info("r/a #2.2: " + phaser.getRegisteredParties() + "/" + phaser.getArrivedParties());

                    ignites[gridIdx].close();

                    ignites[gridIdx] = startGrid(gridIdx);
                }
                catch (Exception e) {
                    U.error(log, "" + gridIdx + " start failed", e);

                    phaser.arriveAndDeregister();

                    throw new RuntimeException(e);
                }

                /*if (gridIdx == 0)
                    break;*/
            }

            /*phaser.arriveAndDeregister();

            System.out.println(">>> Servers restarting routine is stopped.");

            throw new RuntimeException("Servers restarting routine is stopped");*/
        }
    }
}
