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

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;

/**
 * MultiThreaded load test for DHT preloader.
 */
public class GridCacheDhtPreloadMultiThreadedSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /**
     * Creates new test.
     */
    public GridCacheDhtPreloadMultiThreadedSelfTest() {
        super(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeLeaveBeforePreloadingComplete() throws Exception {
        try {
            final CountDownLatch startLatch = new CountDownLatch(1);

            final CountDownLatch stopLatch = new CountDownLatch(1);

            GridTestUtils.runMultiThreadedAsync(
                new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        Ignite g = startGrid("first");

                        g.events().localListen(
                            new IgnitePredicate<IgniteEvent>() {
                                @Override public boolean apply(IgniteEvent evt) {
                                    stopLatch.countDown();

                                    return true;
                                }
                            },
                            IgniteEventType.EVT_NODE_JOINED);

                        startLatch.countDown();

                        stopLatch.await();

                        G.stop(g.name(), false);

                        return null;
                    }
                },
                1,
                "first"
            );

            GridTestUtils.runMultiThreaded(
                new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        startLatch.await();

                        startGrid("second");

                        return null;
                    }
                },
                1,
                "second"
            );
        }
        finally {
            // Intentionally used this method. See startGrid(String, String).
            G.stopAll(false);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentNodesStart() throws Exception {
        try {
            multithreadedAsync(
                new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        IgniteConfiguration cfg = loadConfiguration("modules/core/src/test/config/spring-multicache.xml");

                        startGrid(Thread.currentThread().getName(), cfg);

                        return null;
                    }
                },
                4,
                "starter"
            ).get();
        }
        finally {
            G.stopAll(true);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentNodesStartStop() throws Exception {
        try {
            multithreadedAsync(
                new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        String gridName = "grid-" + Thread.currentThread().getName();

                        startGrid(gridName, "modules/core/src/test/config/example-cache.xml");

                        // Immediately stop the grid.
                        stopGrid(gridName);

                        return null;
                    }
                },
                6,
                "tester"
            ).get();
        }
        finally {
            G.stopAll(true);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = loadConfiguration("modules/core/src/test/config/spring-multicache.xml");

        cfg.setGridName(gridName);

        for (GridCacheConfiguration cCfg : cfg.getCacheConfiguration()) {
            if (cCfg.getCacheMode() == GridCacheMode.PARTITIONED) {
                cCfg.setAffinity(new GridCacheConsistentHashAffinityFunction(2048, null));
                cCfg.setBackups(1);
            }
        }

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        return cfg;
    }
}
