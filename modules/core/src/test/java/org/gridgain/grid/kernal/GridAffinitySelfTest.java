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

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Tests affinity mapping.
 */
public class GridAffinitySelfTest extends GridCommonAbstractTest {
    /** VM ip finder for TCP discovery. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setMaxMissedHeartbeats(Integer.MAX_VALUE);
        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        if (gridName.endsWith("1"))
            cfg.setCacheConfiguration(); // Empty cache configuration.
        else {
            assert gridName.endsWith("2");

            GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

            cacheCfg.setCacheMode(PARTITIONED);
            cacheCfg.setBackups(1);

            cfg.setCacheConfiguration(cacheCfg);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(1, 2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testAffinity() throws IgniteCheckedException {
        Ignite g1 = grid(1);
        Ignite g2 = grid(2);

        assert caches(g1).size() == 0;
        assert F.first(caches(g2)).getCacheMode() == PARTITIONED;

        Map<ClusterNode, Collection<String>> map = g1.cluster().mapKeysToNodes(null, F.asList("1"));

        assertNotNull(map);
        assertEquals("Invalid map size: " + map.size(), 1, map.size());
        assertEquals(F.first(map.keySet()), g2.cluster().localNode());

        UUID id1 = g1.cluster().mapKeyToNode(null, "2").id();

        assertNotNull(id1);
        assertEquals(g2.cluster().localNode().id(), id1);

        UUID id2 = g1.cluster().mapKeyToNode(null, "3").id();

        assertNotNull(id2);
        assertEquals(g2.cluster().localNode().id(), id2);
    }

    /**
     * @param g Grid.
     * @return Non-system caches.
     */
    private Collection<GridCacheConfiguration> caches(Ignite g) {
        return F.view(Arrays.asList(g.configuration().getCacheConfiguration()), new IgnitePredicate<GridCacheConfiguration>() {
            @Override public boolean apply(GridCacheConfiguration c) {
                return c.getName() == null || !c.getName().equals(CU.UTILITY_CACHE_NAME);
            }
        });
    }
}
