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
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Affinity routing tests.
 */
public class GridCacheAffinityRoutingSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 4;

    /** */
    private static final String NON_DFLT_CACHE_NAME = "myCache";

    /** */
    private static final int KEY_CNT = 50;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     * Constructs test.
     */
    public GridCacheAffinityRoutingSelfTest() {
        super(/* don't start grid */ false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        if (!gridName.equals(getTestGridName(GRID_CNT))) {
            // Default cache configuration.
            GridCacheConfiguration dfltCacheCfg = defaultCacheConfiguration();

            dfltCacheCfg.setCacheMode(PARTITIONED);
            dfltCacheCfg.setBackups(1);
            dfltCacheCfg.setWriteSynchronizationMode(FULL_SYNC);

            // Non-default cache configuration.
            GridCacheConfiguration namedCacheCfg = defaultCacheConfiguration();

            namedCacheCfg.setCacheMode(PARTITIONED);
            namedCacheCfg.setBackups(1);
            namedCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
            namedCacheCfg.setName(NON_DFLT_CACHE_NAME);

            cfg.setCacheConfiguration(dfltCacheCfg, namedCacheCfg);
        }
        else {
            // No cache should be configured for extra node.
            cfg.setCacheConfiguration();
        }

        cfg.setMarshaller(new IgniteOptimizedMarshaller(false));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (int i = 0; i < GRID_CNT; i++)
            startGrid(i);

        assert G.allGrids().size() == GRID_CNT;

        for (int i = 0; i < KEY_CNT; i++) {
            grid(0).cache(null).put(i, i);

            grid(0).cache(NON_DFLT_CACHE_NAME).put(i, i);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        for (int i = 0; i < GRID_CNT; i++)
            stopGrid(i);

        assert G.allGrids().isEmpty();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testAffinityRun() throws Exception {
        for (int i = 0; i < KEY_CNT; i++)
            grid(0).compute().affinityRun(NON_DFLT_CACHE_NAME, i, new CheckRunnable(i, i));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testAffinityRunComplexKey() throws Exception {
        for (int i = 0; i < KEY_CNT; i++) {
            AffinityTestKey key = new AffinityTestKey(i);

            grid(0).compute().affinityRun(NON_DFLT_CACHE_NAME, i, new CheckRunnable(i, key));
            grid(0).compute().affinityRun(NON_DFLT_CACHE_NAME, key, new CheckRunnable(i, key));
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testAffinityCall() throws Exception {
        for (int i = 0; i < KEY_CNT; i++)
            grid(0).compute().affinityCall(NON_DFLT_CACHE_NAME, i, new CheckCallable(i, i));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testAffinityCallComplexKey() throws Exception {
        for (int i = 0; i < KEY_CNT; i++) {
            final AffinityTestKey key = new AffinityTestKey(i);

            grid(0).compute().affinityCall(NON_DFLT_CACHE_NAME, i, new CheckCallable(i, key));
            grid(0).compute().affinityCall(NON_DFLT_CACHE_NAME, key, new CheckCallable(i, key));
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testField() throws Exception {
        // Jobs should be routed correctly in case of using load balancer.
        for (int i = 0; i < KEY_CNT; i++)
            assert grid(0).compute().call(new FieldAffinityJob(i)) :
                "Job was routed to a wrong node [i=" + i + "]";
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testMethod() throws Exception {
        // Jobs should be routed correctly in case of using load balancer.
        for (int i = 0; i < KEY_CNT; i++)
            assert grid(0).compute().call(new MethodAffinityJob(i)) :
                "Job was routed to a wrong node [i=" + i + "]";
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testFiledCacheName() throws Exception {
        // Jobs should be routed correctly in case of using load balancer.
        for (int i = 0; i < KEY_CNT; i++)
            assert grid(0).compute().call(new FieldCacheNameAffinityJob(i)) :
                "Job was routed to a wrong node [i=" + i + "]";
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testMethodCacheName() throws Exception {
        // Jobs should be routed correctly in case of using load balancer.
        for (int i = 0; i < KEY_CNT; i++)
            assert grid(0).compute().call(new MethodCacheNameAffinityJob(i)) :
                "Job was routed to a wrong node [i=" + i + "]";
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testMultipleAnnotationsJob() throws Exception {
        try {
            grid(0).compute().call(new MultipleAnnotationsJob(0));

            fail();
        }
        catch (IgniteCheckedException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testTask() throws Exception {
        // Jobs should be routed correctly.
        for (int i = 0; i < KEY_CNT; i++)
            assert grid(0).compute().execute(new OneJobTask(i), i) :
                "Job was routed to a wrong node [i=" + i + "]";

        info("Starting extra node without configured caches...");

        assertEquals(GRID_CNT, G.allGrids().size());

        Ignite g = startGrid(GRID_CNT);

        try {
            assertEquals(GRID_CNT + 1, g.cluster().nodes().size());

            for (int i = 0; i < KEY_CNT; i++)
                assert grid(GRID_CNT).compute().execute(new OneJobTask(i), i) :
                    "Job was routed to a wrong node [i=" + i + "]";
        }
        finally {
            stopGrid(GRID_CNT);
        }
    }

    /**
     * Test job with field annotation.
     */
    private static class FieldAffinityJob implements IgniteCallable<Boolean> {
        /** Affinity key. */
        @GridCacheAffinityKeyMapped
        @GridToStringInclude
        private Object affKey;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** */
        @IgniteJobContextResource
        private ComputeJobContext jobCtx;

        /**
         * @param affKey Affinity key.
         */
        FieldAffinityJob(Object affKey) {
            this.affKey = affKey;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() {
            assert ignite != null;

            assert jobCtx.affinityKey().equals(affKey);
            assert jobCtx.cacheName() == null;

            if (log.isDebugEnabled())
                log.debug("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            GridCacheAffinity<Object> aff = ignite.cache(null).affinity();

            return F.eqNodes(ignite.cluster().localNode(), aff.mapKeyToNode(affKey));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(FieldAffinityJob.class, this);
        }
    }

    /**
     * Test job with method annotation.
     */
    private static class MethodAffinityJob implements IgniteCallable<Boolean> {
        /** Affinity key. */
        @GridToStringInclude
        private Object affKey;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** */
        @IgniteJobContextResource
        private ComputeJobContext jobCtx;

        /**
         * @param affKey Affinity key.
         */
        MethodAffinityJob(Object affKey) {
            this.affKey = affKey;
        }

        /**
         * @return Affinity key.
         */
        @GridCacheAffinityKeyMapped
        public Object affinityKey() {
            return affKey;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() {
            assert ignite != null;

            assert jobCtx.affinityKey().equals(affinityKey());
            assert jobCtx.cacheName() == null;

            if (log.isDebugEnabled())
                log.debug("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            GridCacheAffinity<Object> aff = ignite.cache(null).affinity();

            return F.eqNodes(ignite.cluster().localNode(), aff.mapKeyToNode(affKey));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MethodAffinityJob.class, this);
        }
    }

    /**
     * Test job with field cache name annotation.
     */
    private static class FieldCacheNameAffinityJob implements IgniteCallable<Boolean> {
        /** Affinity key. */
        @GridToStringInclude
        private Object affKey;

        /** Cache name to use affinity from. */
        @GridCacheName
        private String cacheName = NON_DFLT_CACHE_NAME;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** */
        @IgniteJobContextResource
        private ComputeJobContext jobCtx;

        /**
         * @param affKey Affinity key.
         */
        FieldCacheNameAffinityJob(Object affKey) {
            this.affKey = affKey;
        }

        /**
         * @return Affinity key.
         */
        @GridCacheAffinityKeyMapped
        public Object affinityKey() {
            return affKey;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() {
            assert ignite != null;

            assert jobCtx.affinityKey().equals(affKey);
            assert jobCtx.cacheName().equals(cacheName);

            if (log.isDebugEnabled())
                log.debug("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            GridCacheAffinity<Object> aff = ignite.cache(cacheName).affinity();

            return F.eqNodes(ignite.cluster().localNode(), aff.mapKeyToNode(affKey));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(FieldCacheNameAffinityJob.class, this);
        }
    }

    /**
     * Test job with method cache name annotation.
     */
    private static class MethodCacheNameAffinityJob implements IgniteCallable<Boolean> {
        /** Affinity key. */
        @GridToStringInclude
        private Object affKey;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** */
        @IgniteJobContextResource
        private ComputeJobContext jobCtx;

        /**
         * @param affKey Affinity key.
         */
        MethodCacheNameAffinityJob(Object affKey) {
            this.affKey = affKey;
        }

        /**
         * @return Affinity key.
         */
        @GridCacheAffinityKeyMapped
        public Object affinityKey() {
            return affKey;
        }

        /**
         * @return Cache name for affinity routing.
         */
        @GridCacheName
        public String cacheName() {
            return NON_DFLT_CACHE_NAME;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() {
            assert ignite != null;

            assert jobCtx.affinityKey().equals(affKey);
            assert jobCtx.cacheName().equals(cacheName());

            if (log.isDebugEnabled())
                log.debug("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            GridCacheAffinity<Object> aff = ignite.cache(cacheName()).affinity();

            return F.eqNodes(ignite.cluster().localNode(), aff.mapKeyToNode(affKey));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MethodCacheNameAffinityJob.class, this);
        }
    }

    /**
     * Test job with method cache name annotation.
     */
    private static class MultipleAnnotationsJob implements IgniteCallable<Boolean> {
        /** Affinity key. */
        @GridToStringInclude
        @GridCacheAffinityKeyMapped
        private Object affKey;

        /** Duplicated affinity key. */
        @SuppressWarnings({"UnusedDeclaration"})
        @GridCacheAffinityKeyMapped
        private Object affKeyDup;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /**
         * @param affKey Affinity key.
         */
        MultipleAnnotationsJob(Object affKey) {
            this.affKey = affKey;
            affKeyDup = affKey;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() {
            assert ignite != null;

            if (log.isDebugEnabled())
                log.debug("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            GridCacheAffinity<Object> aff = ignite.cache(null).affinity();

            return F.eqNodes(ignite.cluster().localNode(), aff.mapKeyToNode(affKey));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MultipleAnnotationsJob.class, this);
        }
    }

    /**
     * Test task that produces a single job.
     */
    private static class OneJobTask extends ComputeTaskSplitAdapter<Integer, Boolean> {
        /** Affinity key. */
        @GridToStringInclude
        @GridCacheAffinityKeyMapped
        private Object affKey;

        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * @param affKey Affinity key.
         */
        private OneJobTask(Integer affKey) {
            this.affKey = affKey;
        }

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Integer arg) throws IgniteCheckedException {
            return F.asList(new ComputeJobAdapter() {
                @Override public Object execute() {
                    GridCacheAffinity<Object> aff = ignite.cache(null).affinity();

                    ClusterNode primary = aff.mapKeyToNode(affKey);

                    if (log.isInfoEnabled())
                        log.info("Primary node for the job key [affKey=" + affKey + ", primary=" + primary.id() + "]");

                    return F.eqNodes(ignite.cluster().localNode(), primary);
                }
            });
        }

        /** {@inheritDoc} */
        @Override public Boolean reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            return results.get(0).getData();
        }
    }

    /**
     * Test key.
     */
    private static class AffinityTestKey {
        /** Affinity key. */
        @GridCacheAffinityKeyMapped
        private final int affKey;

        /**
         * @param affKey Affinity key.
         */
        private AffinityTestKey(int affKey) {
            this.affKey = affKey;
        }

        /**
         * @return Affinity key.
         */
        public int affinityKey() {
            return affKey;
        }
    }

    /**
     * Test runnable.
     */
    private static class CheckRunnable extends CAX {
        /** Affinity key. */
        private final Object affKey;

        /** Key. */
        private final Object key;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @IgniteJobContextResource
        private ComputeJobContext jobCtx;

        /**
         * @param affKey Affinity key.
         * @param key Key.
         */
        private CheckRunnable(Object affKey, Object key) {
            this.affKey = affKey;
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public void applyx() throws IgniteCheckedException {
            assert ignite.cluster().localNode().id().equals(ignite.cluster().mapKeyToNode(null, affKey).id());
            assert ignite.cluster().localNode().id().equals(ignite.cluster().mapKeyToNode(null, key).id());
            assert jobCtx.affinityKey().equals(affKey);
            assert jobCtx.cacheName().equals(NON_DFLT_CACHE_NAME);
        }
    }

    /**
     * Test callable.
     */
    private static class CheckCallable implements IgniteCallable<Object> {
        /** Affinity key. */
        private final Object affKey;

        /** Key. */
        private final Object key;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @IgniteJobContextResource
        private ComputeJobContext jobCtx;

        /**
         * @param affKey Affinity key.
         * @param key Key.
         */
        private CheckCallable(Object affKey, Object key) {
            this.affKey = affKey;
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws IgniteCheckedException {
            assert ignite.cluster().localNode().id().equals(ignite.cluster().mapKeyToNode(null, affKey).id());
            assert ignite.cluster().localNode().id().equals(ignite.cluster().mapKeyToNode(null, key).id());
            assert jobCtx.affinityKey().equals(affKey);
            assert jobCtx.cacheName().equals(NON_DFLT_CACHE_NAME);

            return null;
        }
    }
}
