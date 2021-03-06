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

package org.gridgain.grid.kernal.processors.cache.transactions;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Cache transaction proxy.
 */
public class IgniteTxProxyImpl<K, V> implements IgniteTxProxy, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Wrapped transaction. */
    @GridToStringInclude
    private IgniteTxEx<K, V> tx;

    /** Gateway. */
    @GridToStringExclude
    private GridCacheSharedContext<K, V> cctx;

    /** Async flag. */
    private boolean async;

    /** Async call result. */
    private IgniteFuture asyncRes;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public IgniteTxProxyImpl() {
        // No-op.
    }

    /**
     * @param tx Transaction.
     * @param cctx Shared context.
     * @param async Async flag.
     */
    public IgniteTxProxyImpl(IgniteTxEx<K, V> tx, GridCacheSharedContext<K, V> cctx, boolean async) {
        assert tx != null;
        assert cctx != null;

        this.tx = tx;
        this.cctx = cctx;
        this.async = async;
    }

    /**
     * Enters a call.
     */
    private void enter() {
        if (cctx.deploymentEnabled())
            cctx.deploy().onEnter();

        try {
            cctx.kernalContext().gateway().readLock();
        }
        catch (IllegalStateException e) {
            throw e;
        }
        catch (RuntimeException | Error e) {
            cctx.kernalContext().gateway().readUnlock();

            throw e;
        }
    }

    /**
     * Leaves a call.
     */
    private void leave() {
        try {
            CU.unwindEvicts(cctx);
        }
        finally {
            cctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid xid() {
        return tx.xid();
    }

    /** {@inheritDoc} */
    @Override public UUID nodeId() {
        if (async)
            save(tx.nodeId());

        return tx.nodeId();
    }

    /** {@inheritDoc} */
    @Override public long threadId() {
        if (async)
            save(tx.threadId());

        return tx.threadId();
    }

    /** {@inheritDoc} */
    @Override public long startTime() {
        if (async)
            save(tx.startTime());

        return tx.startTime();
    }

    /** {@inheritDoc} */
    @Override public IgniteTxIsolation isolation() {
        if (async)
            save(tx.isolation());

        return tx.isolation();
    }

    /** {@inheritDoc} */
    @Override public IgniteTxConcurrency concurrency() {
        if (async)
            save(tx.concurrency());

        return tx.concurrency();
    }

    /** {@inheritDoc} */
    @Override public boolean isInvalidate() {
        if (async)
            save(tx.isInvalidate());

        return tx.isInvalidate();
    }

    /** {@inheritDoc} */
    @Override public boolean implicit() {
        if (async)
            save(tx.implicit());

        return tx.implicit();
    }

    /** {@inheritDoc} */
    @Override public long timeout() {
        if (async)
            save(tx.timeout());

        return tx.timeout();
    }

    /** {@inheritDoc} */
    @Override public IgniteTxState state() {
        if (async)
            save(tx.state());

        return tx.state();
    }

    /** {@inheritDoc} */
    @Override public long timeout(long timeout) {
        return tx.timeout(timeout);
    }

    /** {@inheritDoc} */
    @Override public IgniteAsyncSupport enableAsync() {
        return new IgniteTxProxyImpl<>(tx, cctx, true);
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        return async;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <R> IgniteFuture<R> future() {
        return asyncRes;
    }

    /** {@inheritDoc} */
    @Override public boolean setRollbackOnly() {
        enter();

        try {
            return tx.setRollbackOnly();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isRollbackOnly() {
        enter();

        try {
            if (async)
                save(tx.isRollbackOnly());

            return tx.isRollbackOnly();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void commit() throws IgniteCheckedException {
        enter();

        try {
            IgniteFuture<IgniteTx> commitFut = cctx.commitTxAsync(tx);

            if (async)
                asyncRes = commitFut;
            else
                commitFut.get();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteCheckedException {
        enter();

        try {
            cctx.endTx(tx);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void rollback() throws IgniteCheckedException {
        enter();

        try {
            IgniteFuture rollbackFut = cctx.rollbackTxAsync(tx);

            if (async)
                asyncRes = rollbackFut;
            else
                rollbackFut.get();
        }
        finally {
            leave();
        }
    }

    /**
     * @param res Result to convert to finished future.
     */
    private void save(Object res) {
        asyncRes = new GridFinishedFutureEx<>(res);
    }

    /** {@inheritDoc} */
    @Override public void copyMeta(GridMetadataAware from) {
        tx.copyMeta(from);
    }

    /** {@inheritDoc} */
    @Override public void copyMeta(Map<String, ?> data) {
        tx.copyMeta(data);
    }

    /** {@inheritDoc} */
    @Override public <V1> V1 addMeta(String name, V1 val) {
        return tx.addMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V1> V1 putMetaIfAbsent(String name, V1 val) {
        return tx.putMetaIfAbsent(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V1> V1 putMetaIfAbsent(String name, Callable<V1> c) {
        return tx.putMetaIfAbsent(name, c);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <V1> V1 addMetaIfAbsent(String name, V1 val) {
        return tx.addMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V1> V1 addMetaIfAbsent(String name, @Nullable Callable<V1> c) {
        return tx.addMetaIfAbsent(name, c);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Override public <V1> V1 meta(String name) {
        return tx.<V1>meta(name);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Override public <V1> V1 removeMeta(String name) {
        return tx.<V1>removeMeta(name);
    }

    /** {@inheritDoc} */
    @Override public <V1> boolean removeMeta(String name, V1 val) {
        return tx.removeMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V1> Map<String, V1> allMeta() {
        return tx.allMeta();
    }

    /** {@inheritDoc} */
    @Override public boolean hasMeta(String name) {
        return tx.hasMeta(name);
    }

    /** {@inheritDoc} */
    @Override public <V1> boolean hasMeta(String name, V1 val) {
        return tx.hasMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V1> boolean replaceMeta(String name, V1 curVal, V1 newVal) {
        return tx.replaceMeta(name, curVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(tx);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        tx = (IgniteTxEx<K, V>)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteTxProxyImpl.class, this);
    }
}
