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

import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;

/**
 * Test for cache swap preloading.
 */
public class GridCacheSwapPreloadSelfTest extends GridCommonAbstractTest {
    /** Entry count. */
    private static final int ENTRY_CNT = 15000;

    /** */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private GridCacheMode cacheMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setNetworkTimeout(2000);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setSwapEnabled(true);
        cacheCfg.setCacheMode(cacheMode);
        cacheCfg.setPreloadMode(SYNC);
        cacheCfg.setEvictSynchronized(false);
        cacheCfg.setEvictNearSynchronized(false);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        if (cacheMode == PARTITIONED)
            cacheCfg.setBackups(1);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** @throws Exception If failed. */
    public void testSwapReplicated() throws Exception {
        cacheMode = REPLICATED;

        checkSwap();
    }

    /** @throws Exception If failed. */
    public void testSwapPartitioned() throws Exception {
        cacheMode = PARTITIONED;

        checkSwap();
    }

    /** @throws Exception If failed. */
    private void checkSwap() throws Exception {
        try {
            startGrid(0);

            GridCache<Integer, Integer> cache = grid(0).cache(null);

            // Populate.
            for (int i = 0; i < ENTRY_CNT; i++)
                cache.put(i, i);

            info("Put finished.");

            // Evict all.
            cache.evictAll();

            info("Evict finished.");

            for (int i = 0; i < ENTRY_CNT; i++)
                assertNull(cache.peek(i));

            assert cache.isEmpty();

            startGrid(1);

            int size = grid(1).cache(null).size();

            info("New node cache size: " + size);

            assertEquals(ENTRY_CNT, size);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * TODO: GG-4804 Swap preloading test failed with NotNull assertion, TODO: GG-4804 while key should have been found
     * either in swap or in cache
     *
     * @throws Exception If failed.
     */
    public void _testSwapReplicatedMultithreaded() throws Exception {
        cacheMode = REPLICATED;

        checkSwapMultithreaded();
    }

    /** @throws Exception If failed. */
    public void testSwapPartitionedMultithreaded() throws Exception {
        cacheMode = PARTITIONED;

        checkSwapMultithreaded();
    }

    /** @throws Exception If failed. */
    private void checkSwapMultithreaded() throws Exception {
        final AtomicBoolean done = new AtomicBoolean();
        IgniteFuture<?> fut = null;

        try {
            startGrid(0);

            final GridCache<Integer, Integer> cache = grid(0).cache(null);

            assertNotNull(cache);

            // Populate.
            for (int i = 0; i < ENTRY_CNT; i++)
                cache.put(i, i);

            cache.evictAll();

            fut = multithreadedAsync(new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    Random rnd = new Random();

                    while (!done.get()) {
                        int key = rnd.nextInt(ENTRY_CNT);

                        Integer i = cache.get(key);

                        assertNotNull(i);
                        assertEquals(Integer.valueOf(key), i);

                        boolean b = cache.evict(rnd.nextInt(ENTRY_CNT));

                        assert b;
                    }

                    return null;
                }
            }, 10);

            startGrid(1);

            done.set(true);

            int size = grid(1).cache(null).size();

            info("New node cache size: " + size);

            if (size != ENTRY_CNT) {
                Iterable<Integer> keySet = new TreeSet<>(grid(1).<Integer, Integer>cache(null).keySet());

                int next = 0;

                for (Integer i : keySet) {
                    while (next < i)
                        info("Missing key: " + next++);

                    next++;
                }
            }

            assertEquals(ENTRY_CNT, size);
        }
        finally {
            done.set(true);

            try {
                if (fut != null)
                    fut.get();
            }
            finally {
                stopAllGrids();
            }
        }
    }
}
