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

package org.gridgain.grid.util.future;

import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;

/**
 * Future listener to fill chained future with converted result of the source future.
 */
public class GridFutureChainListener<T, R> implements IgniteInClosure<IgniteFuture<T>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Context. */
    private final GridKernalContext ctx;

    /** Target future. */
    private final GridFutureAdapter<R> fut;

    /** Done callback. */
    private final IgniteClosure<? super IgniteFuture<T>, R> doneCb;

    /**
     * Constructs chain listener.
     *
     * @param ctx Kernal context.
     * @param fut Target future.
     * @param doneCb Done callback.
     */
    public GridFutureChainListener(GridKernalContext ctx, GridFutureAdapter<R> fut,
        IgniteClosure<? super IgniteFuture<T>, R> doneCb) {
        this.ctx = ctx;
        this.fut = fut;
        this.doneCb = doneCb;
    }

    /** {@inheritDoc} */
    @Override public void apply(IgniteFuture<T> t) {
        try {
            fut.onDone(doneCb.apply(t));
        }
        catch (GridClosureException e) {
            fut.onDone(e.unwrap());
        }
        catch (RuntimeException | Error e) {
            U.warn(null, "Failed to notify chained future (is grid stopped?) [grid=" + ctx.gridName() +
                ", doneCb=" + doneCb + ", err=" + e.getMessage() + ']');

            fut.onDone(e);

            throw e;
        }
    }
}
