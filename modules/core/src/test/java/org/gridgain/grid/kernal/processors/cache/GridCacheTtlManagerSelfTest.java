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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * TTL manager self test.
 */
public class GridCacheTtlManagerSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Test cache mode. */
    protected GridCacheMode cacheMode;

    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        GridCacheConfiguration ccfg = new GridCacheConfiguration();

        ccfg.setCacheMode(cacheMode);
        ccfg.setEagerTtl(true);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalTtl() throws Exception {
        checkTtl(LOCAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedTtl() throws Exception {
        checkTtl(PARTITIONED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedTtl() throws Exception {
        checkTtl(REPLICATED);
    }

    /**
     * @param mode Cache mode.
     * @throws Exception If failed.
     */
    private void checkTtl(GridCacheMode mode) throws Exception {
        cacheMode = mode;

        final GridKernal g = (GridKernal)startGrid(0);

        try {
            final String key = "key";

            final GridCache<Object, Object> cache = g.cache(null);

            GridCacheEntry<Object, Object> entry = cache.entry(key);

            entry.timeToLive(1000);
            entry.setValue(1);

            U.sleep(1100);

            GridTestUtils.retryAssert(log, 10, 100, new CAX() {
                @Override public void applyx() {
                    // Check that no more entries left in the map.
                    try {
                        assertNull(g.cache(null).get(key));
                    }
                    catch (IgniteCheckedException ignore) {
                        // No-op.
                    }

                    if (!g.internalCache().context().deferredDelete())
                        assertNull(g.internalCache().map().getEntry(key));
                }
            });
        }
        finally {
            stopAllGrids();
        }
    }
}
