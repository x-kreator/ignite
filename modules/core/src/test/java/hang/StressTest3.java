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

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
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
        return cfg;
    }

    /**
     *
     */
    @Test
    public void testConcurrentClientAndServerNodesRestart() throws Exception {
        LinkedList<IgniteEx> ignites = new LinkedList<>();

        for (int i = 0; i < 4; i++)
            ignites.add(startGrid(gridIdxSeq.getAndIncrement()));

        new Thread(new ServerNodesRestartTask(ignites)).start();

        client.set(true);

        //doSleep(5000);
        while (true) {
            IgniteEx client = startGrid(gridIdxSeq.getAndIncrement());

            client.close();
        }
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
        private LinkedList<IgniteEx> ignites;

        /**
         * @param ignites Ignites.
         */
        ServerNodesRestartTask(LinkedList<IgniteEx> ignites) {
            this.ignites = ignites;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            int gridIdx;

            while (true) {
                Ignite ignite = ignites.remove();
                //sleep(2_000);
                ignite.close();

                gridIdx = gridIdxSeq.getAndIncrement();

                try {
                    ignites.add(startGrid(gridIdx));
                }
                catch (Exception e) {
                    U.error(log, "Ignite #" + gridIdx + " start failed", e);
                    break;
                }
            }
        }
    }
}
