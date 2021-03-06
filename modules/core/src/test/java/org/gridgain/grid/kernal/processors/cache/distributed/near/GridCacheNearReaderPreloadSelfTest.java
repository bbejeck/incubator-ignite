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

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * We have three nodes - A, B and C - and start them in that order. Each node contains NEAR_PARTITIONED transactional
 * cache. Then we immediately put a key which is primary for A, near for B and backup for C. Once key is put, we
 * read it on B. Finally the key is updated again and we ensure that it was updated on the near node B as well. I.e.
 * with this test we ensures that node B is considered as near reader for that key in case put occurred during preload.
 */
public class GridCacheNearReaderPreloadSelfTest extends GridCommonAbstractTest {
    /** Test iterations count. */
    private static final int REPEAT_CNT = 10;

    /** Amopunt of updates on each test iteration. */
    private static final int PUT_CNT = 100;

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Cache on primary node. */
    private GridCache<Integer, Integer> cache1;

    /** Cache on near node. */
    private GridCache<Integer, Integer> cache2;

    /** Cache on backup node. */
    private GridCache<Integer, Integer> cache3;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cache1 = null;
        cache2 = null;
        cache3 = null;

        stopAllGrids(true);
    }

    /**
     * Test.
     *
     * @throws Exception If failed.
     */
    public void testNearReaderPreload() throws Exception {
        for (int i = 0; i < REPEAT_CNT; i++) {
            startUp();

            int key = key();

            for (int j = 0; j < PUT_CNT; j++) {
                cache1.put(key, j);

                checkCaches(key, j);
            }

            stopAllGrids(true);
        }
    }

    /**
     * Startup routine.
     *
     * @throws Exception If failed.
     */
    private void startUp() throws Exception {
        TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

        Ignite node1 = G.start(dataNode(ipFinder, "node1"));
        Ignite node2 = G.start(dataNode(ipFinder, "node2"));
        Ignite node3 = G.start(dataNode(ipFinder, "node3"));

        info("Node 1: " + node1.cluster().localNode().id());
        info("Node 2: " + node2.cluster().localNode().id());
        info("Node 3: " + node3.cluster().localNode().id());

        cache1 = node1.cache(CACHE_NAME);
        cache2 = node2.cache(CACHE_NAME);
        cache3 = node3.cache(CACHE_NAME);
    }

    /**
     * Create configuration for data node.
     *
     * @param ipFinder IP finder.
     * @param gridName Grid name.
     * @return Configuration for data node.
     * @throws IgniteCheckedException If failed.
     */
    private IgniteConfiguration dataNode(TcpDiscoveryIpFinder ipFinder, String gridName)
        throws Exception {
        GridCacheConfiguration ccfg = new GridCacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setDistributionMode(NEAR_PARTITIONED);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(1);

        IgniteConfiguration cfg = getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setLocalHost("127.0.0.1");
        cfg.setDiscoverySpi(spi);
        cfg.setCacheConfiguration(ccfg);
        cfg.setIncludeProperties();
        cfg.setRestEnabled(false);

        return cfg;
    }

    /**
     * Get key which will be primary for the first node and backup for the third node.
     *
     * @return Key.
     */
    private Integer key() {
        int key = 0;

        while (true) {
            Collection<ClusterNode> affNodes = cache1.affinity().mapKeyToPrimaryAndBackups(key);

            assert !F.isEmpty(affNodes);

            ClusterNode primaryNode = F.first(affNodes);

            if (F.eq(primaryNode, cache1.gridProjection().ignite().cluster().localNode()) &&
                affNodes.contains(cache3.gridProjection().ignite().cluster().localNode()))
                break;

            key++;
        }

        return key;
    }

    /**
     * Check whether all caches contains expected value for the given key.
     *
     * @param key Key.
     * @param expVal Expected value.
     * @throws Exception If failed.
     */
    private void checkCaches(int key, int expVal) throws Exception {
        checkCache(cache1, key, expVal);
        checkCache(cache2, key, expVal);
        checkCache(cache3, key, expVal);
    }

    /**
     * Check whether provided cache contains expected value for the given key.
     *
     * @param cache Cache.
     * @param key Key.
     * @param expVal Expected value.
     * @throws Exception If failed.
     */
    private void checkCache(GridCacheProjection<Integer, Integer> cache, int key, int expVal) throws Exception {
        GridCacheEntry<Integer, Integer> entry = cache.entry(key);

        assert F.eq(expVal, entry.getValue()) : "Unexpected cache value [key=" + key + ", expected=" + expVal +
            ", actual=" + entry.getValue() + ", primary=" + entry.primary() + ", backup=" + entry.backup() + ']';
    }
}
