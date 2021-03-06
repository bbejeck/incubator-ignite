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
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Grid cache concurrent hash map self test.
 */
public class GridCacheConcurrentMapTest extends GridCommonAbstractTest {
    /** Random. */
    private static final Random RAND = new Random();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(LOCAL);
        cc.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(cc);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).cache(null).removeAll();
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomEntry() throws Exception {
        GridCache<String, String> cache = grid(0).cache(null);

        for (int i = 0; i < 500; i++)
            cache.put("key" + i, "val" + i);

        for (int i = 0; i < 20; i++) {
            GridCacheEntry<String, String> entry = cache.randomEntry();

            assert entry != null;

            info("Random entry key: " + entry.getKey());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomEntryMultiThreaded() throws Exception {
        final GridCache<String, String> cache = grid(0).cache(null);

        final AtomicBoolean done = new AtomicBoolean();

        IgniteFuture<?> fut1 = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    while (!done.get()) {
                        int i = RAND.nextInt(500);

                        boolean rmv = RAND.nextBoolean();

                        if (rmv)
                            cache.remove("key" + i);
                        else
                            cache.put("key" + i, "val" + i);
                    }

                    return null;
                }
            },
            3
        );

        IgniteFuture<?> fut2 = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    while (!done.get()) {
                        GridCacheEntry<String, String> entry = cache.randomEntry();

                        info("Random entry key: " + (entry != null ? entry.getKey() : "N/A"));
                    }

                    return null;
                }
            },
            1
        );

        Thread.sleep( 60 * 1000);

        done.set(true);

        fut1.get();
        fut2.get();
    }
}
