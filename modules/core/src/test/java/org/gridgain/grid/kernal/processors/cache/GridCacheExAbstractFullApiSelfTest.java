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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;

import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;
import static org.apache.ignite.events.IgniteEventType.*;

/**
 * Abstract test for private cache interface.
 */
public abstract class GridCacheExAbstractFullApiSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return PARTITIONED_ONLY;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetOutTx() throws Exception {
        final AtomicInteger lockEvtCnt = new AtomicInteger();

        IgnitePredicate<IgniteEvent> lsnr = new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                lockEvtCnt.incrementAndGet();

                return true;
            }
        };

        try {
            grid(0).events().localListen(lsnr, EVT_CACHE_OBJECT_LOCKED, EVT_CACHE_OBJECT_UNLOCKED);

            GridCache<String, Integer> cache = cache();

            try (IgniteTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                int key = 0;

                for (int i = 0; i < 1000; i++) {
                    if (cache.affinity().mapKeyToNode("key" + i).id().equals(grid(0).localNode().id())) {
                        key = i;

                        break;
                    }
                }

                cache.get("key" + key);

                for (int i = key + 1; i < 1000; i++) {
                    if (cache.affinity().mapKeyToNode("key" + i).id().equals(grid(0).localNode().id())) {
                        key = i;

                        break;
                    }
                }

                ((GridCacheProjectionEx<String, Integer>)cache).getAllOutTx(F.asList("key" + key));
            }

            assertTrue(GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    info("Lock event count: " + lockEvtCnt.get());

                    return lockEvtCnt.get() == (nearEnabled() ? 4 : 2);
                }
            }, 15000));
        }
        finally {
            grid(0).events().stopLocalListen(lsnr, EVT_CACHE_OBJECT_LOCKED, EVT_CACHE_OBJECT_UNLOCKED);
        }
    }
}
