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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 * Test case checks partition exchange when non-cache node joins topology (partition
 * exchange should be skipped in this case).
 */
public class GridCacheMixedPartitionExchangeSelfTest extends GridCommonAbstractTest {
    /** Flag indicating whether to include cache to the node configuration. */
    private boolean cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (cache)
            cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setDistributionMode(PARTITIONED_ONLY);
        ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeJoinLeave() throws Exception {
        try {
            cache = true;

            startGrids(4);

            awaitPartitionMapExchange();

            final AtomicBoolean finished = new AtomicBoolean();

            IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new IgniteCallable<Object>() {
                @Override public Object call() throws Exception {
                    Random rnd = new Random();

                    int keys = 100;

                    while (!finished.get()) {
                        int g = rnd.nextInt(4);

                        int key = rnd.nextInt(keys);

                        IgniteCache<Integer, Integer> prj = grid(g).jcache(null);

                        try (Transaction tx = grid(g).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            Integer val = prj.get(key);

                            val = val == null ? 1 : val + 1;

                            prj.put(key, val);

                            tx.commit();
                        }
                    }

                    return null;
                }
            }, 4, "async-runner");

            cache = false;

            for (int r = 0; r < 3; r++) {
                for (int i = 4; i < 8; i++)
                    startGrid(i);

                for (int i = 4; i < 8; i++)
                    stopGrid(i);
            }

            // Check we can start more cache nodes after non-cache ones.
            cache = true;

            startGrid(4);

            U.sleep(500);

            finished.set(true);

            fut.get();

            long topVer = grid(0).cluster().topologyVersion();

            assertEquals(29, topVer);

            // Check all grids have all exchange futures completed.
            for (int i = 0; i < 4; i++) {
                IgniteKernal grid = (IgniteKernal)grid(i);

                GridCacheContext<Object, Object> cctx = grid.internalCache(null).context();

                IgniteInternalFuture<Long> verFut = cctx.affinity().affinityReadyFuture(topVer);

                assertEquals((Long)topVer, verFut.get());
                assertEquals((Long)topVer, cctx.topologyVersionFuture().get());
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
