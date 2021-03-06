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

package org.gridgain.grid.tests.p2p;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Test P2P task.
 */
public class GridP2PTestTaskExternalPath1 extends ComputeTaskAdapter<Object, int[]> {
    /** */
    @IgniteLoggerResource
    private IgniteLogger log;

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings({"unchecked"})
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) throws IgniteCheckedException {
        if (log.isInfoEnabled()) {
            log.info("Mapping [task=" + this + ", subgrid=" + F.viewReadOnly(subgrid, F.node2id()) +
                ", arg=" + arg + ']');
        }

        Set<UUID> nodeIds;

        boolean sleep;

        if (arg instanceof Object[]) {
            nodeIds = Collections.singleton((UUID)(((Object[])arg)[0]));

            sleep = (Boolean)((Object[])arg)[1];
        }
        else if (arg instanceof List) {
            nodeIds = new HashSet<>((Collection<UUID>)arg);

            sleep = false;
        }
        else {
            nodeIds = Collections.singleton((UUID)arg);

            sleep = false;
        }

        Map<TestJob, ClusterNode> jobs = U.newHashMap(subgrid.size());

        for (ClusterNode node : subgrid) {
            if (nodeIds.contains(node.id()))
                jobs.put(new TestJob(node.id(), sleep), node);
        }

        if (!jobs.isEmpty())
            return jobs;

        throw new IgniteCheckedException("Failed to find target node: " + arg);
    }

    /**
     * {@inheritDoc}
     */
    @Override public int[] reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
        return results.get(0).getData();
    }

    /**
     * Simple job class
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestJob extends ComputeJobAdapter {
        /** User resource. */
        @IgniteUserResource
        private transient GridTestUserResource rsrc;

        /** Local node ID. */
        @IgniteLocalNodeIdResource
        private UUID locNodeId;

        /** Task session. */
        @IgniteTaskSessionResource
        private ComputeTaskSession ses;

        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** */
        @IgniteInstanceResource
        private Ignite g;

        /** */
        private boolean sleep;

        /**
         *
         */
        public TestJob() {
            // No-op.
        }

        /**
         * @param nodeId Node ID for node this job is supposed to execute on.
         * @param sleep Sleep flag.
         */
        public TestJob(UUID nodeId, boolean sleep) {
            super(nodeId);

            this.sleep = sleep;
        }

        /** {@inheritDoc} */
        @Override public int[] execute() throws IgniteCheckedException {
            assert locNodeId.equals(argument(0));

            log.info("Running job on node: " + g.cluster().localNode().id());

            if (sleep) {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                }
                catch (InterruptedException e) {
                    log.info("Job has been cancelled. Caught exception: " + e);

                    Thread.currentThread().interrupt();
                }
            }

            return new int[] {
                System.identityHashCode(rsrc),
                System.identityHashCode(ses.getClassLoader())
            };
        }
    }
}
