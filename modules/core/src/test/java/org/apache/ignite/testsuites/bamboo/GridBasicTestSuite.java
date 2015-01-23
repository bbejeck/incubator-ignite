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

package org.apache.ignite.testsuites.bamboo;

import junit.framework.*;
import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.product.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.testsuites.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.closure.*;
import org.apache.ignite.internal.processors.continuous.*;
import org.apache.ignite.internal.util.typedef.internal.*;

/**
 * Basic test suite.
 */
public class GridBasicTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Basic Test Suite");

        suite.addTest(GridLangSelfTestSuite.suite());
        suite.addTest(GridUtilSelfTestSuite.suite());
        suite.addTest(GridMarshallerSelfTestSuite.suite());
        suite.addTest(GridKernalSelfTestSuite.suite());
        suite.addTest(GridLoadersSelfTestSuite.suite());
        suite.addTest(GridRichSelfTestSuite.suite());
        suite.addTest(GridExternalizableSelfTestSuite.suite());
        suite.addTest(GridP2PSelfTestSuite.suite());

        if (U.isLinux() || U.isMacOs())
            suite.addTest(GridIpcSharedMemorySelfTestSuite.suite());

        suite.addTestSuite(GridTopologyBuildVersionSelfTest.class);
        suite.addTestSuite(GridReleaseTypeSelfTest.class);
        suite.addTestSuite(GridProductVersionSelfTest.class);
        suite.addTestSuite(GridAffinityProcessorConsistentHashSelfTest.class);
        suite.addTestSuite(GridAffinityProcessorRendezvousSelfTest.class);
        suite.addTestSuite(GridClosureProcessorSelfTest.class);
        suite.addTestSuite(GridStartStopSelfTest.class);
        suite.addTestSuite(GridProjectionForCachesSelfTest.class);
        suite.addTestSuite(GridSpiLocalHostInjectionTest.class);
        suite.addTestSuite(GridLifecycleBeanSelfTest.class);
        suite.addTestSuite(GridStopWithCancelSelfTest.class);
        suite.addTestSuite(GridReduceSelfTest.class);
        suite.addTestSuite(GridEventConsumeSelfTest.class);
        suite.addTestSuite(GridExceptionHelpLinksSelfTest.class);
        suite.addTestSuite(GridSuppressedExceptionSelfTest.class);
        suite.addTestSuite(GridLifecycleAwareSelfTest.class);
        suite.addTestSuite(GridMessageListenSelfTest.class);

        // Streamer.
        suite.addTest(GridStreamerSelfTestSuite.suite());

        return suite;
    }
}