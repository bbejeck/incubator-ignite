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

import org.apache.ignite.transactions.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Tx metrics adapter.
 */
public class IgniteTxMetricsAdapter implements IgniteTxMetrics, Externalizable {
    /** Number of transaction commits. */
    private volatile int txCommits;

    /** Number of transaction rollbacks. */
    private volatile int txRollbacks;

    /** Last commit time. */
    private volatile long commitTime;

    /** Last rollback time. */
    private volatile long rollbackTime;

    /**
     *
     */
    public IgniteTxMetricsAdapter() {

    }

    /**
     * @param m Transaction metrics to copy.
     */
    public IgniteTxMetricsAdapter(IgniteTxMetrics m) {
        commitTime = m.commitTime();
        rollbackTime = m.rollbackTime();
        txCommits = m.txCommits();
        txRollbacks = m.txRollbacks();
    }

    /** {@inheritDoc} */
    @Override public long commitTime() {
        return commitTime;
    }

    /** {@inheritDoc} */
    @Override public long rollbackTime() {
        return rollbackTime;
    }

    /** {@inheritDoc} */
    @Override public int txCommits() {
        return txCommits;
    }

    /** {@inheritDoc} */
    @Override public int txRollbacks() {
        return txRollbacks;
    }

    /**
     * Transaction commit callback.
     */
    public void onTxCommit() {
        commitTime = U.currentTimeMillis();

        txCommits++;
    }

    /**
     * Transaction rollback callback.
     */
    public void onTxRollback() {
        rollbackTime = U.currentTimeMillis();

        txRollbacks++;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(commitTime);
        out.writeLong(rollbackTime);
        out.writeInt(txCommits);
        out.writeInt(txRollbacks);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        commitTime = in.readLong();
        rollbackTime = in.readLong();
        txCommits = in.readInt();
        txRollbacks = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteTxMetricsAdapter.class, this);
    }
}
