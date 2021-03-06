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

package org.gridgain.grid.kernal.processors.cache.distributed.replicated.preloader;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.lifecycle.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Tests for replicated cache preloader.
 */
@SuppressWarnings({"PublicInnerClass"})
public class GridCacheReplicatedPreloadLifecycleSelfTest extends GridCachePreloadLifecycleAbstractTest {
    /** */
    private static boolean quiet = true;

    /** Grid count. */
    private int gridCnt = 5;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionsConfiguration().setDefaultTxConcurrency(OPTIMISTIC);
        c.getTransactionsConfiguration().setDefaultTxIsolation(READ_COMMITTED);

        GridCacheConfiguration cc1 = defaultCacheConfiguration();

        cc1.setName("one");
        cc1.setCacheMode(REPLICATED);
        cc1.setWriteSynchronizationMode(FULL_SYNC);
        cc1.setPreloadMode(preloadMode);
        cc1.setEvictionPolicy(null);
        cc1.setSwapEnabled(false);
        cc1.setStore(null);

        // Identical configuration.
        GridCacheConfiguration cc2 = new GridCacheConfiguration(cc1);

        cc2.setName("two");

        c.setCacheConfiguration(cc1, cc2);

        return c;
    }

    /**
     * @param keys Keys.
     * @return Lifecycle bean.
     */
    private LifecycleBean lifecycleBean(final Object[] keys) {
        return new LifecycleBean() {
            @IgniteInstanceResource
            private Ignite ignite;

            @Override public void onLifecycleEvent(LifecycleEventType evt) throws IgniteCheckedException {
                switch (evt) {
                    case AFTER_GRID_START: {
                        GridCache<Object, MyValue> c1 = ignite.cache("one");
                        GridCache<Object, MyValue> c2 = ignite.cache("two");

                        if (!ignite.name().contains("Test0")) {
                            if (!quiet) {
                                info("Keys already in cache:");

                                for (Object k : c1.keySet())
                                    info("Cache1: " + k.toString());

                                for (Object k : c2.keySet())
                                    info("Cache2: " + k.toString());
                            }

                            return;
                        }

                        info("Populating cache data...");

                        int i = 0;

                        for (Object key : keys) {
                            c1.put(key, new MyValue(value(key)));

                            if (i++ % 2 == 0)
                                c2.put(key, new MyValue(value(key)));
                        }

                        assert c1.size() == keys.length : "Invalid cache1 size [size=" + c1.size() +
                            ", entries=" + c1.entrySet() + ']';
                        assert c2.size() == keys.length / 2 : "Invalid cache2 size [size=" + c2.size() +
                            ", entries=" + c2.entrySet() + ']';

                        break;
                    }

                    case BEFORE_GRID_START:
                    case BEFORE_GRID_STOP:
                    case AFTER_GRID_STOP: {
                        info("Lifecycle event: " + evt);

                        break;
                    }
                }
            }
        };
    }

    /**
     * @param keys Keys.
     * @throws Exception If failed.
     */
    public void checkCache(Object[] keys) throws Exception {
        preloadMode = SYNC;

        lifecycleBean = lifecycleBean(keys);

        for (int i = 0; i < gridCnt; i++) {
            startGrid(i);

            info("Checking '" + (i + 1) + "' nodes...");

            for (int j = 0; j < G.allGrids().size(); j++) {
                GridCache<String, MyValue> c1 = grid(j).cache("one");
                GridCache<String, MyValue> c2 = grid(j).cache("two");

                int size1 = c1.size();
                int size2 = c2.size();

                assert size1 == keys.length : " Invalid cache1 size [i=" + i + ", j=" + j + ", size=" + size1 +
                    ", cache=" + c1.entrySet() + ']';
                assert size2 == keys.length / 2 : " Invalid cache2 size [i=" + i + ", j=" + j + ", size=" + size2 +
                    ", cache=" + c2.entrySet() + ']';
            }
        }
    }


    /**
     * @param keys Keys.
     * @throws Exception If failed.
     */
    public void checkScanQuery(Object[] keys) throws Exception {
        preloadMode = SYNC;

        lifecycleBean = lifecycleBean(keys);

        for (int i = 0; i < gridCnt; i++) {
            startGrid(i);

            info("Checking '" + (i + 1) + "' nodes...");

            for (int j = 0; j < G.allGrids().size(); j++) {
                GridCache<Object, MyValue> c2 = grid(j).cache("two");

                GridCacheQuery<Map.Entry<Object, MyValue>> qry = c2.queries().createScanQuery(null);

                final int i0 = j;
                final int j0 = i;

                qry = qry.projection(grid(j));

                int totalCnt = F.sumInt(qry.execute(new IgniteReducer<Map.Entry<Object, MyValue>, Integer>() {
                    @IgniteInstanceResource
                    private Ignite grid;

                    @IgniteLoggerResource
                    private IgniteLogger log0;

                    private int cnt;

                    @Override public boolean collect(Map.Entry<Object, MyValue> e) {
                        if (!quiet && log0.isInfoEnabled())
                            log0.info("Collecting entry: " + e);

                        Object key = e.getKey();

                        assertNotNull(e.getValue());

                        try {
                            Object v1 = e.getValue();
                            Object v2 = grid.cache("one").get(key);

                            assertNotNull("Cache c1 misses value for key [i=" + j0 + ", j=" + i0 +
                                ", missedKey=" + key + ", cache=" + grid.cache("one").values() + ']', v2);
                            assertEquals(v1, v2);
                        }
                        catch (IgniteCheckedException e1) {
                            e1.printStackTrace();

                            assert false;
                        }

                        cnt++;

                        return true;
                    }

                    @Override public Integer reduce() {
                        return cnt;
                    }
                }).get());

                info("Total entry count [grid=" + j + ", totalCnt=" + totalCnt + ']');

                assertEquals(keys.length * (i + 1) / 2, totalCnt);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLifecycleBean1() throws Exception {
        checkCache(keys(true, DFLT_KEYS.length, DFLT_KEYS));
    }

    /**
     * @throws Exception If failed.
     */
    public void testLifecycleBean2() throws Exception {
        checkCache(keys(false, DFLT_KEYS.length, DFLT_KEYS));
    }

    /**
     * @throws Exception If failed.
     */
    public void testLifecycleBean3() throws Exception {
        checkCache(keys(true, 500));
    }

    /**
     * @throws Exception If failed.
     */
    public void testLifecycleBean4() throws Exception {
        checkCache(keys(false, 500));
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQuery1() throws Exception {
        checkScanQuery(keys(true, DFLT_KEYS.length, DFLT_KEYS));
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQuery2() throws Exception {
        checkScanQuery(keys(false, DFLT_KEYS.length, DFLT_KEYS));
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQuery3() throws Exception {
        checkScanQuery(keys(true, 500));
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQuery4() throws Exception {
        checkScanQuery(keys(false, 500));
    }
}
