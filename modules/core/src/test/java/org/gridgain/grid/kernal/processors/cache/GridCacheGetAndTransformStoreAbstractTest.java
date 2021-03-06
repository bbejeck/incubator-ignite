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

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Basic get and transform store test.
 */
public abstract class GridCacheGetAndTransformStoreAbstractTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache store. */
    private static final GridCacheTestStore store = new GridCacheTestStore();

    /**
     *
     */
    protected GridCacheGetAndTransformStoreAbstractTest() {
        super(true /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        store.resetTimestamp();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cache().clearAll();

        store.reset();
    }

    /** @return Caching mode. */
    protected abstract GridCacheMode cacheMode();

    /** {@inheritDoc} */
    @Override protected final IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(cacheMode());
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setSwapEnabled(false);
        cc.setAtomicityMode(atomicityMode());
        cc.setDistributionMode(distributionMode());
        cc.setPreloadMode(SYNC);

        cc.setStore(store);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @return Distribution mode.
     */
    protected GridCacheDistributionMode distributionMode() {
        return NEAR_PARTITIONED;
    }

    /**
     * @return Cache atomicity mode.
     */
    protected GridCacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndTransform() throws Exception {
        final AtomicBoolean finish = new AtomicBoolean();

        try {
            startGrid(0);
            startGrid(1);
            startGrid(2);

            final IgniteClosure<String, String> trans = new TransformClosure();

            IgniteFuture<?> fut = multithreadedAsync(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        GridCache<Integer, String> c = cache(ThreadLocalRandom.current().nextInt(3));

                        while (!finish.get() && !Thread.currentThread().isInterrupted()) {
                            c.get(ThreadLocalRandom.current().nextInt(100));
                            c.put(ThreadLocalRandom.current().nextInt(100), "s");
                            c.transform(
                                ThreadLocalRandom.current().nextInt(100),
                                trans);
                        }

                        return null;
                    }
                },
                20);

            Thread.sleep(15_000);

            finish.set(true);

            fut.get();
        }
        finally {
            stopGrid(0);
            stopGrid(1);
            stopGrid(2);

            while (!cache().isEmpty())
                cache().globalClearAll(Long.MAX_VALUE);
        }
    }

    /**
     *
     */
    private static class TransformClosure implements IgniteClosure<String, String> {
        /** {@inheritDoc} */
        @Override public String apply(String s) {
            return "str";
        }
    }
}
