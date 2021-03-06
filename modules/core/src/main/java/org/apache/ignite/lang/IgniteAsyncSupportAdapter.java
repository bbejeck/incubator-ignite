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

package org.apache.ignite.lang;

import org.apache.ignite.*;

/**
 * Adapter for {@link IgniteAsyncSupport}.
 */
public class IgniteAsyncSupportAdapter implements IgniteAsyncSupport {
    /** Future for previous asynchronous operation. */
    protected ThreadLocal<IgniteFuture<?>> curFut;

    /**
     * Default constructor.
     */
    public IgniteAsyncSupportAdapter() {
        // No-op.
    }

    /**
     * @param async Async enabled flag.
     */
    public IgniteAsyncSupportAdapter(boolean async) {
        if (async)
            curFut = new ThreadLocal<>();
    }

    /** {@inheritDoc} */
    @Override public IgniteAsyncSupport enableAsync() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        return curFut != null;
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> future() {
        if (curFut == null)
            throw new IllegalStateException("Asynchronous mode is disabled.");

        IgniteFuture<?> fut = curFut.get();

        if (fut == null)
            throw new IllegalStateException("Asynchronous operation not started.");

        curFut.set(null);

        return (IgniteFuture<R>)fut;
    }

    /**
     * @param fut Future.
     * @return If async mode is enabled saves future and returns {@code null},
     *         otherwise waits for future and returns result.
     * @throws IgniteCheckedException If asynchronous mode is disabled and future failed.
     */
    public <R> R saveOrGet(IgniteFuture<R> fut) throws IgniteCheckedException {
        if (curFut != null) {
            curFut.set(fut);

            return null;
        }
        else
            return fut.get();
    }
}
