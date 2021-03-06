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
import org.apache.ignite.configuration.*;
import org.apache.ignite.lifecycle.*;
import org.apache.ignite.logger.java.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.plugin.segmentation.*;
import org.gridgain.client.ssl.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import javax.net.ssl.*;

/**
 * Test for {@link org.apache.ignite.lifecycle.LifecycleAware} support in {@link org.apache.ignite.configuration.IgniteConfiguration}.
 */
public class GridLifecycleAwareSelfTest extends GridAbstractLifecycleAwareSelfTest {
    /**
     */
    private static class TestClientMessageInterceptor extends TestLifecycleAware
        implements ClientMessageInterceptor {
        /**
         */
        TestClientMessageInterceptor() {
            super(null);
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object onReceive(@Nullable Object obj) {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object onSend(Object obj) {
            return null;
        }
    }

    /**
     */
    private static class TestSegmentationResolver extends TestLifecycleAware implements GridSegmentationResolver {
        /**
         */
        TestSegmentationResolver() {
            super(null);
        }

        /** {@inheritDoc} */
        @Override public boolean isValidSegment() throws IgniteCheckedException {
            return true;
        }
    }

    /**
     */
    private static class TestContextFactory extends TestLifecycleAware implements GridSslContextFactory {
        /**
         */
        TestContextFactory() {
            super(null);
        }

        /** {@inheritDoc} */
        @Override public SSLContext createSslContext() throws SSLException {
            return null;
        }
    }

    /**
     */
    private static class TestLifecycleBean extends TestLifecycleAware implements LifecycleBean {
        /**
         */
        TestLifecycleBean() {
            super(null);
        }

        /** {@inheritDoc} */
        @Override public void onLifecycleEvent(LifecycleEventType evt) throws IgniteCheckedException {
            // No-op.
        }
    }

    /**
     */
    private static class TestMarshaller extends IgniteOptimizedMarshaller implements LifecycleAware {
        /** */
        private final TestLifecycleAware lifecycleAware = new TestLifecycleAware(null);

        /** {@inheritDoc} */
        @Override public void start() throws IgniteCheckedException {
            lifecycleAware.start();
        }

        /** {@inheritDoc} */
        @Override public void stop() throws IgniteCheckedException {
            lifecycleAware.stop();
        }

        /**
         * @return Lifecycle aware.
         */
        TestLifecycleAware lifecycleAware() {
            return lifecycleAware;
        }
    }

    /**
     */
    private static class TestLogger extends IgniteJavaLogger implements LifecycleAware {
        /** */
        private final TestLifecycleAware lifecycleAware = new TestLifecycleAware(null);

        /** {@inheritDoc} */
        @Override public void start() throws IgniteCheckedException {
            lifecycleAware.start();
        }

        /** {@inheritDoc} */
        @Override public void stop() throws IgniteCheckedException {
            lifecycleAware.stop();
        }

        /**
         * @return Lifecycle aware.
         */
        TestLifecycleAware lifecycleAware() {
            return lifecycleAware;
        }
    }

    /** {@inheritDoc} */
    @Override protected final IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TestClientMessageInterceptor interceptor = new TestClientMessageInterceptor();

        ClientConnectionConfiguration clientCfg = new ClientConnectionConfiguration();

        clientCfg.setClientMessageInterceptor(interceptor);

        cfg.setClientConnectionConfiguration(clientCfg);

        lifecycleAwares.add(interceptor);

        TestSegmentationResolver segmentationRslvr = new TestSegmentationResolver();

        cfg.setSegmentationResolvers(segmentationRslvr);

        lifecycleAwares.add(segmentationRslvr);

        TestContextFactory ctxFactory = new TestContextFactory();

        clientCfg.setRestTcpSslContextFactory(ctxFactory);

        lifecycleAwares.add(ctxFactory);

        TestLifecycleBean lifecycleBean = new TestLifecycleBean();

        cfg.setLifecycleBeans(lifecycleBean);

        lifecycleAwares.add(lifecycleBean);

        TestMarshaller marshaller = new TestMarshaller();

        cfg.setMarshaller(marshaller);

        lifecycleAwares.add(marshaller.lifecycleAware());

        TestLogger testLog = new TestLogger();

        cfg.setGridLogger(testLog);

        lifecycleAwares.add(testLog.lifecycleAware());

        return cfg;
    }
}
