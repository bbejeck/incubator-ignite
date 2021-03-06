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

package org.gridgain.grid.p2p;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.common.*;
import java.net.*;
import java.util.*;

/**
 * Test P2P class loading in SHARED_CLASSLOADER_UNDEPLOY mode.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "P2P")
public class GridP2PNodeLeftSelfTest extends GridCommonAbstractTest {
    /** */
    private static final ClassLoader urlClsLdr1;

    /** */
    static {
        String path = GridTestProperties.getProperty("p2p.uri.cls");

        try {
            urlClsLdr1 = new URLClassLoader(
                new URL[] { new URL(path) },
                GridP2PNodeLeftSelfTest.class.getClassLoader());
        }
        catch (MalformedURLException e) {
            throw new RuntimeException("Failed to create URL: " + path, e);
        }
    }

    /**
     * Current deployment mode. Used in {@link #getConfiguration(String)}.
     */
    private IgniteDeploymentMode depMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * Test undeploy task.
     * @param isExpectUndeploy Whether undeploy is expected.
     *
     * @throws Exception if error occur.
     */
    @SuppressWarnings("unchecked")
    private void processTest(boolean isExpectUndeploy) throws Exception {
        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);
            Ignite ignite3 = startGrid(3);

            Class task1 = urlClsLdr1.loadClass("org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath1");

            int[] res1 = (int[]) ignite1.compute().execute(task1, ignite2.cluster().localNode().id());

            stopGrid(1);

            Thread.sleep(1000);

            // Task will be deployed after stop node1
            int[] res2 = (int[]) ignite3.compute().execute(task1, ignite2.cluster().localNode().id());

            if (isExpectUndeploy)
                assert isNotSame(res1, res2);
            else
                assert Arrays.equals(res1, res2);
        }
        finally {
            stopGrid(1);
            stopGrid(2);
            stopGrid(3);
        }
    }

    /**
     * Test GridDeploymentMode.CONTINOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testContinuousMode() throws Exception {
        depMode = IgniteDeploymentMode.CONTINUOUS;

        processTest(false);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSharedMode() throws Exception {
        depMode = IgniteDeploymentMode.SHARED;

        processTest(true);
    }

    /**
     * Return true if and only if all elements of array are different.
     *
     * @param m1 array 1.
     * @param m2 array 2.
     * @return true if all elements of array are different.
     */
    private boolean isNotSame(int[] m1, int[] m2) {
        assert m1.length == m2.length;
        assert m1.length == 2;
        return m1[0] != m2[0] && m1[1] != m2[1];
    }
}
