package org.apache.ignite.util;

import com.sun.management.GarbageCollectionNotificationInfo;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ExcessiveGCPausesTest extends GridCommonAbstractTest {
    /**
     *
     */
    public void testGarbageCollectorMXBean() throws Exception {
        logMemInfo();

        final Thread beat = new Thread() {
            private long millis = System.currentTimeMillis();

            @Override public void run() {
                while (true) {
                    try {
                        Thread.sleep(50);

                        final long now = System.currentTimeMillis();

                        final long pause = now - millis - 10;

                        millis = now;

                        if (pause >= 500)
                            log.warning("Possible too long STW pause: " + pause + " milliseconds.");
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        };
        beat.setPriority(Thread.MAX_PRIORITY);
        beat.start();

        installGCMonitoring();

        //byte[] bytes1 = new byte[2000000000];

        //log.info("bytes length: " + bytes1.length);

        //logMemInfo();

        //bytes1 = null;

        logMemInfo();

        final long[][] arr = new long[50000000][];

        for (int i = 0; i < arr.length; i++) {
            arr[i] = new long[5];

            if (i % 10000 == 0)
                Thread.yield();
        }

        logMemInfo();

        for (int i = 0; i < arr.length; i += 2)
            arr[i] = null;

        logMemInfo();

        //System.gc();
        //Thread.sleep(5000);

        //logMemInfo();

        byte[] bytes2 = new byte[1000000000];

        log.info("bytes length: " + bytes2.length);

        logMemInfo();

        byte[] bytes3 = new byte[200000000];

        logMemInfo();

        Thread.sleep(1000);

        beat.interrupt();
        beat.join();
    }

    private int memInfoCnt = 0;

    /**
     *
     */
    private void logMemInfo() {
        memInfoCnt++;
        /*log.info("--- Memory info #" + memInfoCnt);
        log.info("Total memory: " + Runtime.getRuntime().totalMemory());
        log.info("Free memory: " + Runtime.getRuntime().freeMemory());
        log.info("Max memory: " + Runtime.getRuntime().maxMemory());
        log.info("---");*/
    }


    public static void installGCMonitoring() {
        //get all the GarbageCollectorMXBeans - there's one for each heap generation
        //so probably two - the old generation and young generation
        List<GarbageCollectorMXBean> gcbeans = java.lang.management.ManagementFactory.getGarbageCollectorMXBeans();
        //Install a notifcation handler for each bean
        for (GarbageCollectorMXBean gcbean : gcbeans) {
            System.out.println(gcbean);
            NotificationEmitter emitter = (NotificationEmitter) gcbean;
            //use an anonymously generated listener for this example
            // - proper code should really use a named class
            NotificationListener listener = new NotificationListener() {
                //keep a count of the total time spent in GCs
                long totalGcDuration = 0;

                //implement the notifier callback handler
                @Override
                public void handleNotification(Notification notification, Object handback) {
                    //we only handle GARBAGE_COLLECTION_NOTIFICATION notifications here
                    if (notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
                        //get the information associated with this notification
                        GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
                        //get all the info and pretty print it
                        long duration = info.getGcInfo().getDuration();
                        String gctype = info.getGcAction();

                        if ("end of minor GC".equals(gctype))
                            gctype = "Young Gen GC";
                        else if ("end of major GC".equals(gctype))
                            gctype = "Old Gen GC";

                        System.out.println();
                        System.out.println(gctype + ": - " + info.getGcInfo().getId() + " " + info.getGcName() + " (from " + info.getGcCause() + ") " + duration + " milliseconds; start-end times " + info.getGcInfo().getStartTime() + "-" + info.getGcInfo().getEndTime());
                        //System.out.println("GcInfo CompositeType: " + info.getGcInfo().getCompositeType());
                        //System.out.println("GcInfo MemoryUsageAfterGc: " + info.getGcInfo().getMemoryUsageAfterGc());
                        //System.out.println("GcInfo MemoryUsageBeforeGc: " + info.getGcInfo().getMemoryUsageBeforeGc());

                        //Get the information about each memory space, and pretty print it
                        Map<String, MemoryUsage> membefore = info.getGcInfo().getMemoryUsageBeforeGc();
                        Map<String, MemoryUsage> mem = info.getGcInfo().getMemoryUsageAfterGc();
                        for (Map.Entry<String, MemoryUsage> entry : mem.entrySet()) {
                            String name = entry.getKey();
                            MemoryUsage memdetail = entry.getValue();
                            long memInit = memdetail.getInit();
                            long memCommitted = memdetail.getCommitted();
                            long memMax = memdetail.getMax();
                            long memUsed = memdetail.getUsed();
                            MemoryUsage before = membefore.get(name);
                            long beforepercent = ((before.getUsed() * 1000L) / before.getCommitted());
                            long percent = ((memUsed * 1000L) / before.getCommitted()); //>100% when it gets expanded

                            System.out.println(name
                                    + (memCommitted == memMax ? "(fully expanded)" : "(still expandable)")
                                    + "used: " + (beforepercent / 10) + "." + (beforepercent % 10) + "%->" + (percent / 10) + "." + (percent % 10) + "%(" + ((memUsed / 1048576) + 1) + "MB)");
                        }
                        System.out.println();
                        totalGcDuration += info.getGcInfo().getDuration();
                        long percent = totalGcDuration * 1000L / info.getGcInfo().getEndTime();
                        System.out.println("GC cumulated overhead " + (percent / 10) + "." + (percent % 10) + "%");
                    }
                }
            };

            //Add the listener
            emitter.addNotificationListener(listener, null, null);
        }
    }

    /** */
    public void testThrowInCatchWithFinally() throws Exception {
        try {
            try {
                log.info("internal body");
                throw new Exception("1st");
            }
            catch (Exception e) {
                //log.error("internal catch", e);
                throw e;
            }
            finally {
                log.warning("internal finally");
            }
        }
        catch (Exception e) {
            log.error("external catch", e);
            throw e;
        }
        finally {
            log.warning("external finally");
        }
    }

    /** */
    public void testStopping() throws Exception {
        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            awaitPartitionMapExchange();
        }
        finally {
            stopAllGrids();
        }
    }

}
