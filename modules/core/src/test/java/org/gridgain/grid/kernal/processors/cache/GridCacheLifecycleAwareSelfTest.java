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
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.cloner.*;
import org.gridgain.grid.cache.eviction.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Test for {@link LifecycleAware} support in {@link GridCacheConfiguration}.
 */
public class GridCacheLifecycleAwareSelfTest extends GridAbstractLifecycleAwareSelfTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private GridCacheDistributionMode distroMode;

    /** */
    private boolean writeBehind;

    /**
     */
    private static class TestStore extends TestLifecycleAware implements GridCacheStore {
        /**
         */
        TestStore() {
            super(CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object load(@Nullable IgniteTx tx, Object key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure clo, @Nullable Object... args) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void loadAll(@Nullable IgniteTx tx, @Nullable Collection keys,
            IgniteBiInClosure c) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void put(@Nullable IgniteTx tx, Object key,
            @Nullable Object val) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void putAll(@Nullable IgniteTx tx, @Nullable Map map) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void remove(@Nullable IgniteTx tx, Object key) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void removeAll(@Nullable IgniteTx tx,
            @Nullable Collection keys) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void txEnd(IgniteTx tx, boolean commit) {
            // No-op.
        }
    }

    /**
     */
    private static class TestAffinityFunction extends TestLifecycleAware implements GridCacheAffinityFunction {
        /**
         */
        TestAffinityFunction() {
            super(CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public int partitions() {
            return 1;
        }

        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(GridCacheAffinityFunctionContext affCtx) {
            List<List<ClusterNode>> res = new ArrayList<>();

            res.add(nodes(0, affCtx.currentTopologySnapshot()));

            return res;
        }

        /** {@inheritDoc} */
        public List<ClusterNode> nodes(int part, Collection<ClusterNode> nodes) {
            return new ArrayList<>(nodes);
        }

        /** {@inheritDoc} */
        @Override public void removeNode(UUID nodeId) {
            // No-op.
        }
    }

    /**
     */
    private static class TestEvictionPolicy extends TestLifecycleAware implements GridCacheEvictionPolicy {
        /**
         */
        TestEvictionPolicy() {
            super(CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Override public void onEntryAccessed(boolean rmv, GridCacheEntry entry) {
            // No-op.
        }
    }

    /**
     */
    private static class TestEvictionFilter extends TestLifecycleAware implements GridCacheEvictionFilter {
        /**
         */
        TestEvictionFilter() {
            super(CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Override public boolean evictAllowed(GridCacheEntry entry) {
            return false;
        }
    }

    /**
     */
    private static class TestCloner extends TestLifecycleAware implements GridCacheCloner {
        /**
         */
        TestCloner() {
            super(CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> T cloneValue(T val) throws IgniteCheckedException {
            return val;
        }
    }

    /**
     */
    private static class TestAffinityKeyMapper extends TestLifecycleAware implements GridCacheAffinityKeyMapper {
        /**
         */
        TestAffinityKeyMapper() {
            super(CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Override public Object affinityKey(Object key) {
            return key;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            // No-op.
        }
    }

    /**
     */
    private static class TestInterceptor extends TestLifecycleAware implements GridCacheInterceptor {
        /**
         */
        private TestInterceptor() {
            super(CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object onGet(Object key, @Nullable Object val) {
            return val;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object onBeforePut(Object key, @Nullable Object oldVal, Object newVal) {
            return newVal;
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Object key, Object val) {
            // No-op.
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked") @Nullable @Override public IgniteBiTuple onBeforeRemove(Object key, @Nullable Object val) {
            return new IgniteBiTuple(false, val);
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Object key, Object val) {
            // No-op.
        }
    }

    /** {@inheritDoc} */
    @Override protected final IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi());

        GridCacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);

        ccfg.setDistributionMode(distroMode);

        ccfg.setWriteBehindEnabled(writeBehind);

        ccfg.setCacheMode(GridCacheMode.PARTITIONED);

        ccfg.setName(CACHE_NAME);

        TestStore store = new TestStore();

        ccfg.setStore(store);

        lifecycleAwares.add(store);

        TestAffinityFunction affinity = new TestAffinityFunction();

        ccfg.setAffinity(affinity);

        lifecycleAwares.add(affinity);

        TestEvictionPolicy evictionPlc = new TestEvictionPolicy();

        ccfg.setEvictionPolicy(evictionPlc);

        lifecycleAwares.add(evictionPlc);

        TestEvictionPolicy nearEvictionPlc = new TestEvictionPolicy();

        ccfg.setNearEvictionPolicy(nearEvictionPlc);

        lifecycleAwares.add(nearEvictionPlc);

        TestEvictionFilter evictionFilter = new TestEvictionFilter();

        ccfg.setEvictionFilter(evictionFilter);

        lifecycleAwares.add(evictionFilter);

        TestCloner cloner = new TestCloner();

        ccfg.setCloner(cloner);

        lifecycleAwares.add(cloner);

        TestAffinityKeyMapper mapper = new TestAffinityKeyMapper();

        ccfg.setAffinityMapper(mapper);

        lifecycleAwares.add(mapper);

        TestInterceptor interceptor = new TestInterceptor();

        lifecycleAwares.add(interceptor);

        ccfg.setInterceptor(interceptor);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ErrorNotRethrown")
    @Override public void testLifecycleAware() throws Exception {
        for (GridCacheDistributionMode mode : new GridCacheDistributionMode[] {PARTITIONED_ONLY, NEAR_PARTITIONED}) {
            distroMode = mode;

            writeBehind = false;

            try {
                super.testLifecycleAware();
            }
            catch (AssertionError e) {
                throw new AssertionError("Failed for [distroMode=" + distroMode + ", writeBehind=" + writeBehind + ']',
                    e);
            }

            writeBehind = true;

            try {
                super.testLifecycleAware();
            }
            catch (AssertionError e) {
                throw new AssertionError("Failed for [distroMode=" + distroMode + ", writeBehind=" + writeBehind + ']',
                    e);
            }
        }
    }
}
