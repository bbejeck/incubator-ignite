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
import org.apache.ignite.plugin.*;
import org.apache.ignite.spi.*;
import org.gridgain.grid.util.direct.*;

import java.util.*;

/**
 *
 */
public class GridPluginContext implements PluginContext {
    /** */
    private final PluginConfiguration cfg;

    /** */
    private final GridKernalContext ctx;

    /** */
    private IgniteConfiguration igniteCfg;

    /**
     * @param ctx Kernal context.
     * @param cfg Plugin configuration.
     */
    public GridPluginContext(GridKernalContext ctx, PluginConfiguration cfg, IgniteConfiguration igniteCfg) {
        this.cfg = cfg;
        this.ctx = ctx;
        this.igniteCfg = igniteCfg;
    }

    /** {@inheritDoc} */
    @Override public <C extends PluginConfiguration> C configuration() {
        return (C)cfg;
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration igniteConfiguration() {
        return igniteCfg;
    }

    /** {@inheritDoc} */
    @Override public Ignite grid() {
        return ctx.grid();
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes() {
        return ctx.discovery().allNodes();
    }

    /** {@inheritDoc} */
    @Override public ClusterNode localNode() {
        return ctx.discovery().localNode();
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log(Class<?> cls) {
        return ctx.log(cls);
    }

    /** {@inheritDoc} */
    @Override public void registerPort(int port, IgnitePortProtocol proto, Class<?> cls) {
        ctx.ports().registerPort(port, proto, cls);
    }

    /** {@inheritDoc} */
    @Override public void deregisterPort(int port, IgnitePortProtocol proto, Class<?> cls) {
        ctx.ports().deregisterPort(port, proto, cls);
    }

    /** {@inheritDoc} */
    @Override public void deregisterPorts(Class<?> cls) {
        ctx.ports().deregisterPorts(cls);
    }

    /** {@inheritDoc} */
    @Override public byte registerMessageProducer(GridTcpCommunicationMessageProducer producer) {
        return ctx.registerMessageProducer(producer);
    }
}
