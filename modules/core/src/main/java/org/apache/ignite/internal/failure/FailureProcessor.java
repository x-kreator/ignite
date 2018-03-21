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

package org.apache.ignite.internal.failure;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.failure.FailureAction;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.internal.GridKernalContextImpl;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * General failure processing API
 */
public class FailureProcessor {
    /** Default failure handler. */
    private static final FailureHandler DFLT_FAILURE_HND = new DefaultFailureHandler();

    /** Context. */
    private GridKernalContextImpl ctx;

    /**
     * @param ctx Context.
     */
    public FailureProcessor(GridKernalContextImpl ctx) {
        this.ctx = ctx;
    }

    /**
     * Processes some failure.
     * May cause node termination.
     *
     * @param failureCtx Failure context.
     */
    public synchronized void process(FailureContext failureCtx) {
        if (ctx.invalidationCause() != null) // Node already terminating, no reason to process more errors.
            return;

        FailureHandler hnd = ctx.config().getIgniteFailureHandler();

        if (hnd == null)
            hnd = DFLT_FAILURE_HND;

        FailureAction act = hnd.onFailure(failureCtx, null);

        if (act == FailureAction.NOOP)
            return;

        ctx.invalidate(failureCtx);

        switch (act) {
            case RESTART_JVM:
                restartJvm(failureCtx);

                break;

            case STOP:
                stopNode(failureCtx);

                break;

            default:
                throw new RuntimeException("Unsupported Ignite failure action: " + act);
        }
    }

    /**
     * Restarts JVM.
     */
    public void restartJvm(final FailureContext failureCtx) {
        new Thread(
            new Runnable() {
                @Override public void run() {
                    final IgniteLogger log = ctx.log(getClass());

                    U.warn(log, "Restarting JVM on Ignite failure: " + failureCtx);

                    G.restart(true);
                }
            },
            "node-restarter"
        ).start();
    }

    /**
     * Stops local node.
     */
    public void stopNode(final FailureContext failureCtx) {
        new Thread(
            new Runnable() {
                @Override public void run() {
                    final IgniteLogger log = ctx.log(getClass());

                    U.warn(log, "Stopping local node on Ignite failure: " + failureCtx);

                    IgnitionEx.stop(ctx.igniteInstanceName(), true, true, 60 * 1000);
                }
            },
            "node-stopper"
        ).start();
    }
}
