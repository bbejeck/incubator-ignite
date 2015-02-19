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

package org.apache.ignite.internal.visor.node;

import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.visor.cache.*;
import org.apache.ignite.internal.visor.streamer.*;

import java.io.*;
import java.util.*;

import static java.lang.System.*;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.*;

/**
 * Data transfer object for node configuration data.
 */
public class VisorGridConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Basic. */
    private VisorBasicConfiguration basic;

    /** Metrics. */
    private VisorMetricsConfiguration metrics;

    /** SPIs. */
    private VisorSpisConfiguration spis;

    /** P2P. */
    private VisorPeerToPeerConfiguration p2p;

    /** Lifecycle. */
    private VisorLifecycleConfiguration lifecycle;

    /** Executors service configuration. */
    private VisorExecutorServiceConfiguration execSvc;

    /** Segmentation. */
    private VisorSegmentationConfiguration seg;

    /** Include properties. */
    private String inclProps;

    /** Include events types. */
    private int[] inclEvtTypes;

    /** REST configuration. */
    private VisorRestConfiguration rest;

    /** User attributes. */
    private Map<String, ?> userAttrs;

    /** Caches. */
    private Iterable<VisorCacheConfiguration> caches;

    /** Igfss. */
    private Iterable<VisorIgfsConfiguration> igfss;

    /** Streamers. */
    private Iterable<VisorStreamerConfiguration> streamers;

    /** Environment. */
    private Map<String, String> env;

    /** System properties. */
    private Properties sysProps;

    /** Configuration of atomic data structures. */
    private VisorAtomicConfiguration atomic;

    /** Transactions configuration. */
    private VisorTransactionConfiguration txCfg;

    /** Query configuration */
    private VisorQueryConfiguration qryCfg;

    /**
     * @param ignite Grid.
     * @return Fill data transfer object with node configuration data.
     */
    public VisorGridConfiguration from(IgniteEx ignite) {
        assert ignite != null;

        IgniteConfiguration c = ignite.configuration();

        basic(VisorBasicConfiguration.from(ignite, c));
        metrics(VisorMetricsConfiguration.from(c));
        spis(VisorSpisConfiguration.from(c));
        p2p(VisorPeerToPeerConfiguration.from(c));
        lifecycle(VisorLifecycleConfiguration.from(c));
        executeService(VisorExecutorServiceConfiguration.from(c));
        segmentation(VisorSegmentationConfiguration.from(c));
        includeProperties(compactArray(c.getIncludeProperties()));
        includeEventTypes(c.getIncludeEventTypes());
        rest(VisorRestConfiguration.from(c));
        userAttributes(c.getUserAttributes());
        caches(VisorCacheConfiguration.list(ignite, c.getCacheConfiguration()));
        igfss(VisorIgfsConfiguration.list(c.getIgfsConfiguration()));
        streamers(VisorStreamerConfiguration.list(c.getStreamerConfiguration()));
        env(new HashMap<>(getenv()));
        systemProperties(getProperties());
        atomic(VisorAtomicConfiguration.from(c.getAtomicConfiguration()));
        transaction(VisorTransactionConfiguration.from(c.getTransactionConfiguration()));
        queryConfiguration(VisorQueryConfiguration.from(c.getQueryConfiguration()));

        return this;
    }

    /**
     * @return Basic.
     */
    public VisorBasicConfiguration basic() {
        return basic;
    }

    /**
     * @param basic New basic.
     */
    public void basic(VisorBasicConfiguration basic) {
        this.basic = basic;
    }

    /**
     * @return Metrics.
     */
    public VisorMetricsConfiguration metrics() {
        return metrics;
    }

    /**
     * @param metrics New metrics.
     */
    public void metrics(VisorMetricsConfiguration metrics) {
        this.metrics = metrics;
    }

    /**
     * @return SPIs.
     */
    public VisorSpisConfiguration spis() {
        return spis;
    }

    /**
     * @param spis New SPIs.
     */
    public void spis(VisorSpisConfiguration spis) {
        this.spis = spis;
    }

    /**
     * @return P2P.
     */
    public VisorPeerToPeerConfiguration p2p() {
        return p2p;
    }

    /**
     * @param p2P New p2p.
     */
    public void p2p(VisorPeerToPeerConfiguration p2P) {
        p2p = p2P;
    }

    /**
     * @return Lifecycle.
     */
    public VisorLifecycleConfiguration lifecycle() {
        return lifecycle;
    }

    /**
     * @param lifecycle New lifecycle.
     */
    public void lifecycle(VisorLifecycleConfiguration lifecycle) {
        this.lifecycle = lifecycle;
    }

    /**
     * @return Executors service configuration.
     */
    public VisorExecutorServiceConfiguration executeService() {
        return execSvc;
    }

    /**
     * @param execSvc New executors service configuration.
     */
    public void executeService(VisorExecutorServiceConfiguration execSvc) {
        this.execSvc = execSvc;
    }

    /**
     * @return Segmentation.
     */
    public VisorSegmentationConfiguration segmentation() {
        return seg;
    }

    /**
     * @param seg New segmentation.
     */
    public void segmentation(VisorSegmentationConfiguration seg) {
        this.seg = seg;
    }

    /**
     * @return Include properties.
     */
    public String includeProperties() {
        return inclProps;
    }

    /**
     * @param inclProps New include properties.
     */
    public void includeProperties(String inclProps) {
        this.inclProps = inclProps;
    }

    /**
     * @return Include events types.
     */
    public int[] includeEventTypes() {
        return inclEvtTypes;
    }

    /**
     * @param inclEvtTypes New include events types.
     */
    public void includeEventTypes(int[] inclEvtTypes) {
        this.inclEvtTypes = inclEvtTypes;
    }

    /**
     * @return Rest.
     */
    public VisorRestConfiguration rest() {
        return rest;
    }

    /**
     * @param rest New rest.
     */
    public void rest(VisorRestConfiguration rest) {
        this.rest = rest;
    }

    /**
     * @return User attributes.
     */
    public Map<String, ?> userAttributes() {
        return userAttrs;
    }

    /**
     * @param userAttrs New user attributes.
     */
    public void userAttributes(Map<String, ?> userAttrs) {
        this.userAttrs = userAttrs;
    }

    /**
     * @return Caches.
     */
    public Iterable<VisorCacheConfiguration> caches() {
        return caches;
    }

    /**
     * @param caches New caches.
     */
    public void caches(Iterable<VisorCacheConfiguration> caches) {
        this.caches = caches;
    }

    /**
     * @return Igfss.
     */
    public Iterable<VisorIgfsConfiguration> igfss() {
        return igfss;
    }

    /**
     * @param igfss New igfss.
     */
    public void igfss(Iterable<VisorIgfsConfiguration> igfss) {
        this.igfss = igfss;
    }

    /**
     * @return Streamers.
     */
    public Iterable<VisorStreamerConfiguration> streamers() {
        return streamers;
    }

    /**
     * @param streamers New streamers.
     */
    public void streamers(Iterable<VisorStreamerConfiguration> streamers) {
        this.streamers = streamers;
    }

    /**
     * @return Environment.
     */
    public Map<String, String> env() {
        return env;
    }

    /**
     * @param env New environment.
     */
    public void env(Map<String, String> env) {
        this.env = env;
    }

    /**
     * @return System properties.
     */
    public Properties systemProperties() {
        return sysProps;
    }

    /**
     * @param sysProps New system properties.
     */
    public void systemProperties(Properties sysProps) {
        this.sysProps = sysProps;
    }

    /**
     * @return Configuration of atomic data structures.
     */
    public VisorAtomicConfiguration atomic() {
        return atomic;
    }

    /**
     * @param atomic New configuration of atomic data structures.
     */
    public void atomic(VisorAtomicConfiguration atomic) {
        this.atomic = atomic;
    }

    /**
     * @return Transactions configuration.
     */
    public VisorTransactionConfiguration transaction() {
        return txCfg;
    }

    /**
     * @param txCfg New transactions configuration.
     */
    public void transaction(VisorTransactionConfiguration txCfg) {
        this.txCfg = txCfg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGridConfiguration.class, this);
    }

    /**
     * @return Query configuration.
     */
    public VisorQueryConfiguration queryConfiguration() {
        return qryCfg;
    }

    /**
     * @param qryCfg New query configuration.
     */
    public void queryConfiguration(VisorQueryConfiguration qryCfg) {
        this.qryCfg = qryCfg;
    }
}
