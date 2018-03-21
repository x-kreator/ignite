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

import org.apache.ignite.failure.FailureAction;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.internal.GridKernalContext;

/**
 * This implementation will try to stop node if tryStop value is true.
 * If node can't be stopped during provided timeout or tryStop value is false
 * then JVM process will be terminated forcibly ( Runtime.halt() ).
 */
public class StopNodeOrHaltFailureHandler implements FailureHandler {
    /** Try stop. */
    private final boolean tryStop;

    /** Timeout. */
    private final long timeout;

    /**
     * @param tryStop Try stop.
     * @param timeout Timeout.
     */
    public StopNodeOrHaltFailureHandler(boolean tryStop, long timeout) {
        this.tryStop = tryStop;
        this.timeout = timeout;
    }

    /** {@inheritDoc} */
    @Override public FailureAction onFailure(FailureContext failureCtx, GridKernalContext ctx) {
        // TODO Runtime.getRuntime().halt(Ignition.KILL_EXIT_CODE);

        return null; // FIXME
    }
}
