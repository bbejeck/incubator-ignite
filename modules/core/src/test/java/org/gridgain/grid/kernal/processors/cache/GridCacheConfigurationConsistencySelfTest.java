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
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.cache.affinity.fair.*;
import org.gridgain.grid.cache.cloner.*;
import org.gridgain.grid.cache.eviction.*;
import org.gridgain.grid.cache.eviction.fifo.*;
import org.gridgain.grid.cache.eviction.lru.*;
import org.gridgain.grid.cache.eviction.random.*;
import org.gridgain.grid.cache.store.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.configuration.IgniteDeploymentMode.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 *
 */
public class GridCacheConfigurationConsistencySelfTest extends GridCommonAbstractTest {
    /** */
    private boolean cacheEnabled;

    /** */
    private String cacheName;

    /** */
    private GridCacheMode cacheMode = REPLICATED;

    /** */
    private IgniteDeploymentMode depMode = SHARED;

    /** */
    private C1<GridCacheConfiguration, Void> initCache;

    /** */
    private boolean useStrLog;

    /** */
    private GridStringLogger strLog;

    /** */
    private GridCacheAffinityFunction aff;

    /** */
    private int backups;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (useStrLog) {
            strLog = new GridStringLogger(false, cfg.getGridLogger());
            cfg.setGridLogger(strLog);
        }

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setDeploymentMode(depMode);

        if (cacheEnabled) {
            GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

            cacheCfg.setName(cacheName);
            cacheCfg.setCacheMode(cacheMode);
            cacheCfg.setAffinity(aff);
            cacheCfg.setBackups(backups);
            cacheCfg.setAtomicityMode(TRANSACTIONAL);

            if (initCache != null)
                initCache.apply(cacheCfg);

            cfg.setCacheConfiguration(cacheCfg);
        }
        else
            cfg.setCacheConfiguration();

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheUtilsCheckAttributeMismatch() throws Exception {
        Ignite ignite = startGrid(1);

        final ClusterNode node = ignite.cluster().localNode();

        final GridStringLogger strLog = new GridStringLogger(false, log);

        CU.checkAttributeMismatch(strLog, "cache", node, "cacheMode", "Cache mode", LOCAL, PARTITIONED, false);

        assertTrue("No expected message in log: " + strLog.toString(),
            strLog.toString().contains("Cache mode mismatch"));

        strLog.reset();

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            /** {@inheritDoc} */
            @Override public Void call() throws Exception {
                CU.checkAttributeMismatch(strLog, "cache", node, "cacheMode", "Cache mode", LOCAL, PARTITIONED, true);
                return null;
            }
        }, IgniteCheckedException.class, "Cache mode mismatch");

        final GridCacheConfiguration cfg1 = defaultCacheConfiguration();

        cfg1.setCacheMode(LOCAL);

        final GridCacheConfiguration cfg2 = defaultCacheConfiguration();

        cfg2.setCacheMode(PARTITIONED);

        CU.checkAttributeMismatch(strLog, cfg1, cfg2, node, new T2<>("cacheMode", "Cache mode"), false);

        assertTrue("No expected message in log: " + strLog.toString(),
            strLog.toString().contains("Cache mode mismatch"));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            /** {@inheritDoc} */
            @Override public Void call() throws Exception {
                CU.checkAttributeMismatch(strLog, cfg1, cfg2, node, new T2<>("cacheMode", "Cache mode"), true);
                return null;
            }
        }, IgniteCheckedException.class, "Cache mode mismatch");
    }

    /**
     * @throws Exception If failed.
     */
    public void testNullCacheMode() throws Exception {
        // Grid with null cache mode.
        // This is a legal case. The default cache mode should be used.
        cacheEnabled = true;
        cacheName = "myCache";
        cacheMode = null;
        depMode = SHARED;

        assert startGrid(1).cache("myCache").configuration().getCacheMode() == GridCacheConfiguration.DFLT_CACHE_MODE;
    }

    /**
     * @throws Exception If failed.
     */
    public void testWithCacheAndWithoutCache() throws Exception {
        // 1st grid without cache.
        cacheEnabled = false;
        depMode = SHARED;

        startGrid(2);

        // 2nd grid with replicated cache on board.
        cacheEnabled = true;
        cacheName = "myCache";
        cacheMode = REPLICATED;
        depMode = SHARED;

        startGrid(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSameCacheDifferentModes() throws Exception {
        // 1st grid with replicated cache.
        cacheEnabled = true;
        cacheName = "myCache";
        cacheMode = REPLICATED;
        depMode = SHARED;

        startGrid(1);

        // 2nd grid with partitioned cache.
        cacheEnabled = true;
        cacheName = "myCache";
        cacheMode = PARTITIONED;
        depMode = SHARED;

        try {
            startGrid(2);

            fail();
        }
        catch (IgniteCheckedException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentCacheDifferentModes() throws Exception {
        // 1st grid with local cache.
        cacheEnabled = true;
        cacheName = "local";
        cacheMode = LOCAL;
        depMode = SHARED;

        startGrid(1);

        // 2nd grid with replicated cache.
        cacheEnabled = true;
        cacheName = "replicated";
        cacheMode = REPLICATED;
        depMode = SHARED;

        startGrid(2);

        // 3d grid with partitioned cache.
        cacheEnabled = true;
        cacheName = "partitioned";
        cacheMode = PARTITIONED;
        depMode = SHARED;

        startGrid(3);

        // 4th grid with null cache mode (legal case, it should turn to REPLICATED mode).
        cacheEnabled = true;
        cacheName = "replicated";
        cacheMode = null;
        depMode = SHARED;

        startGrid(4);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentDeploymentModes() throws Exception {
        // 1st grid with SHARED mode.
        cacheEnabled = true;
        cacheName = "partitioned";
        cacheMode = PARTITIONED;
        depMode = SHARED;

        startGrid(1);

        // 2nd grid with CONTINUOUS mode.
        cacheEnabled = true;
        cacheName = "partitioned";
        cacheMode = PARTITIONED;
        depMode = CONTINUOUS;

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(2);
            }
        }, IgniteCheckedException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentAffinities() throws Exception {
        cacheMode = PARTITIONED;

        checkSecondGridStartFails(
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setAffinity(new GridCacheConsistentHashAffinityFunction() {/*No-op.*/});
                    return null;
                }
            },
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setAffinity(new GridCacheConsistentHashAffinityFunction());
                    return null;
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentPreloadModes() throws Exception {
        checkSecondGridStartFails(
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setPreloadMode(NONE);
                    return null;
                }
            },
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setPreloadMode(ASYNC);
                    return null;
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentEvictionEnabled() throws Exception {
        checkSecondGridStartFails(
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setEvictionPolicy(new GridCacheFifoEvictionPolicy());
                    return null;
                }
            },
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(GridCacheConfiguration cfg) {
                    return null;
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentEvictionPolicies() throws Exception {
        checkSecondGridStartFails(
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setEvictionPolicy(new GridCacheRandomEvictionPolicy());
                    return null;
                }
            },
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setEvictionPolicy(new GridCacheFifoEvictionPolicy());
                    return null;
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentEvictionFilters() throws Exception {
        checkSecondGridStartFails(
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setEvictionFilter(new GridCacheEvictionFilter<Object, Object>() {
                        @Override public boolean evictAllowed(GridCacheEntry<Object, Object> entry) {
                            return false;
                        }
                    });
                    return null;
                }
            },
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setEvictionFilter(new GridCacheEvictionFilter<Object, Object>() {
                        @Override public boolean evictAllowed(GridCacheEntry<Object, Object> entry) {
                            return true;
                        }
                    });
                    return null;
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentAffinityMappers() throws Exception {
        checkSecondGridStartFails(
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setAffinityMapper(new GridCacheDefaultAffinityKeyMapper() {
                    });
                    return null;
                }
            },
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setAffinityMapper(new GridCacheDefaultAffinityKeyMapper());
                    return null;
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentEvictSynchronized() throws Exception {
        cacheMode = PARTITIONED;

        checkSecondGridStartFails(
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setEvictSynchronized(true);
                    cfg.setEvictionPolicy(new GridCacheFifoEvictionPolicy(100));
                    return null;
                }
            },
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setEvictSynchronized(false);
                    cfg.setEvictionPolicy(new GridCacheFifoEvictionPolicy(100));
                    return null;
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentEvictNearSynchronized() throws Exception {
        cacheMode = PARTITIONED;

        checkSecondGridStartFails(
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @SuppressWarnings("deprecation")
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setDistributionMode(NEAR_PARTITIONED);
                    cfg.setEvictNearSynchronized(true);
                    return null;
                }
            },
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @SuppressWarnings("deprecation")
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setDistributionMode(NEAR_PARTITIONED);
                    cfg.setEvictNearSynchronized(false);
                    return null;
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentAtomicity() throws Exception {
        cacheMode = PARTITIONED;

        checkSecondGridStartFails(
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setDistributionMode(PARTITIONED_ONLY);
                    cfg.setAtomicityMode(ATOMIC);
                    return null;
                }
            },
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setDistributionMode(PARTITIONED_ONLY);
                    cfg.setAtomicityMode(TRANSACTIONAL);
                    return null;
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentSynchronization() throws Exception {
        cacheMode = PARTITIONED;

        checkSecondGridStartFails(
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setWriteSynchronizationMode(FULL_SYNC);
                    return null;
                }
            },
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setWriteSynchronizationMode(FULL_ASYNC);
                    return null;
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testAttributesError() throws Exception {
        checkSecondGridStartFails(
            new C1<GridCacheConfiguration, Void>() {
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setQueryIndexEnabled(true);
                    return null;
                }
            },
            new C1<GridCacheConfiguration, Void>() {
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setQueryIndexEnabled(false);
                    return null;
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityFunctionConsistency() throws Exception {
        cacheEnabled = true;
        cacheMode = PARTITIONED;

        backups = 1;

        aff = new GridCacheConsistentHashAffinityFunction(false, 100);

        startGrid(1);

        // 2nd grid with another affinity.
        // Check include neighbors.
        aff = new GridCacheConsistentHashAffinityFunction(true, 100);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(2);
            }
        }, IgniteCheckedException.class, "Affinity include neighbors mismatch");

        backups = 2;

        // Check backups.
        aff = new GridCacheConsistentHashAffinityFunction(false, 100);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(2);
            }
        }, IgniteCheckedException.class, "Affinity key backups mismatch");

        backups = 1;

        // Partitions count.
        aff = new GridCacheConsistentHashAffinityFunction(false, 1000);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(2);
            }
        }, IgniteCheckedException.class, "Affinity partitions count mismatch");

        // Replicas count.
        aff = new GridCacheConsistentHashAffinityFunction(false, 100);
        ((GridCacheConsistentHashAffinityFunction)aff).setDefaultReplicas(1024);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(2);
            }
        }, IgniteCheckedException.class, "Affinity replicas mismatch");

        // Replicas count attribute name.
        aff = new GridCacheConsistentHashAffinityFunction(false, 100);
        ((GridCacheConsistentHashAffinityFunction)aff).setReplicaCountAttributeName("attr_name");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(2);
            }
        }, IgniteCheckedException.class, "Affinity replica count attribute name mismatch");

        // Different hash ID resolver.
        GridCacheConsistentHashAffinityFunction aff0 = new GridCacheConsistentHashAffinityFunction(false, 100);

        aff0.setHashIdResolver(new GridCacheAffinityNodeIdHashResolver());

        aff = aff0;

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(2);
            }
        }, IgniteCheckedException.class, "Partitioned cache affinity hash ID resolver class mismatch");
    }

    /**
     * @throws Exception If failed.
     */
    public void testAttributesWarnings() throws Exception {
        cacheEnabled = true;

        initCache = new C1<GridCacheConfiguration, Void>() {
            /** {@inheritDoc} */
            @Override public Void apply(GridCacheConfiguration cfg) {
                cfg.setAtomicSequenceReserveSize(1000);
                cfg.setCloner(new GridCacheCloner() {
                    @Nullable @Override public <T> T cloneValue(T val) {
                        return null;
                    }
                });
                cfg.setDefaultLockTimeout(1000);
                cfg.setDefaultQueryTimeout(1000);
                cfg.setDefaultTimeToLive(1000);
                return null;
            }
        };

        startGrid(1);

        useStrLog = true;

        initCache = new C1<GridCacheConfiguration, Void>() {
            /** {@inheritDoc} */
            @Override public Void apply(GridCacheConfiguration cfg) {
                cfg.setAtomicSequenceReserveSize(2 * 1000);
                cfg.setCloner(new GridCacheCloner() {
                    @Nullable @Override public <T> T cloneValue(T val) {
                        return null;
                    }
                });
                cfg.setDefaultLockTimeout(2 * 1000);
                cfg.setDefaultQueryTimeout(2 * 1000);
                cfg.setDefaultTimeToLive(2 * 1000);
                return null;
            }
        };

        startGrid(2);

        String log = strLog.toString();

        assertTrue(log.contains("Atomic sequence reserve size mismatch"));
        assertTrue(log.contains("Cache cloner mismatch"));
        assertTrue(log.contains("Default lock timeout"));
        assertTrue(log.contains("Default query timeout"));
        assertTrue(log.contains("Default time to live"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedOnlyAttributesIgnoredForReplicated() throws Exception {
        cacheEnabled = true;

        cacheMode = REPLICATED;

        initCache = new C1<GridCacheConfiguration, Void>() {
            @Override public Void apply(GridCacheConfiguration cfg) {
                cfg.setEvictNearSynchronized(true);
                cfg.setNearEvictionPolicy(new GridCacheRandomEvictionPolicy());
                return null;
            }
        };

        startGrid(1);

        initCache = new C1<GridCacheConfiguration, Void>() {
            @Override public Void apply(GridCacheConfiguration cfg) {
                cfg.setEvictNearSynchronized(false);
                cfg.setNearEvictionPolicy(new GridCacheFifoEvictionPolicy());
                return null;
            }
        };

        startGrid(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgnoreMismatchForLocalCaches() throws Exception {
        cacheEnabled = true;

        cacheMode = LOCAL;

        initCache = new C1<GridCacheConfiguration, Void>() {
            /** {@inheritDoc} */
            @Override public Void apply(GridCacheConfiguration cfg) {
                cfg.setAffinity(new GridCacheConsistentHashAffinityFunction() {/*No-op.*/});
                cfg.setEvictionPolicy(new GridCacheFifoEvictionPolicy());
                cfg.setStore(new TestStore());
                return null;
            }
        };

        startGrid(1);

        initCache = new C1<GridCacheConfiguration, Void>() {
            /** {@inheritDoc} */
            @Override public Void apply(GridCacheConfiguration cfg) {
                cfg.setAffinity(new GridCacheConsistentHashAffinityFunction());
                cfg.setEvictionPolicy(new GridCacheLruEvictionPolicy());
                cfg.setStore(null);
                return null;
            }
        };

        startGrid(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgnoreStoreMismatchForAtomicClientCache() throws Exception {
        cacheEnabled = true;

        cacheMode = PARTITIONED;

        initCache = new C1<GridCacheConfiguration, Void>() {
            @Override public Void apply(GridCacheConfiguration cc) {
                cc.setAtomicityMode(ATOMIC);
                cc.setDistributionMode(PARTITIONED_ONLY);
                cc.setStore(new TestStore());

                return null;
            }
        };

        startGrid(1);

        initCache = new C1<GridCacheConfiguration, Void>() {
            @Override public Void apply(GridCacheConfiguration cc) {
                cc.setAtomicityMode(ATOMIC);
                cc.setDistributionMode(CLIENT_ONLY);
                cc.setStore(null);

                return null;
            }
        };

        startGrid(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStoreCheckAtomic() throws Exception {
        cacheEnabled = true;

        cacheMode = PARTITIONED;

        initCache = new C1<GridCacheConfiguration, Void>() {
            @Override public Void apply(GridCacheConfiguration cc) {
                cc.setAtomicityMode(ATOMIC);
                cc.setDistributionMode(PARTITIONED_ONLY);
                cc.setStore(new TestStore());

                return null;
            }
        };

        startGrid(1);

        initCache = new C1<GridCacheConfiguration, Void>() {
            @Override public Void apply(GridCacheConfiguration cc) {
                cc.setAtomicityMode(ATOMIC);
                cc.setDistributionMode(PARTITIONED_ONLY);
                cc.setStore(null);

                return null;
            }
        };

        GridTestUtils.assertThrows(log, new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                startGrid(2);

                return null;
            }
        }, IgniteCheckedException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStoreCheckTransactional() throws Exception {
        cacheEnabled = true;

        cacheMode = PARTITIONED;

        initCache = new C1<GridCacheConfiguration, Void>() {
            @Override public Void apply(GridCacheConfiguration cc) {
                cc.setAtomicityMode(TRANSACTIONAL);
                cc.setDistributionMode(PARTITIONED_ONLY);
                cc.setStore(new TestStore());

                return null;
            }
        };

        startGrid(1);

        initCache = new C1<GridCacheConfiguration, Void>() {
            @Override public Void apply(GridCacheConfiguration cc) {
                cc.setAtomicityMode(TRANSACTIONAL);
                cc.setDistributionMode(PARTITIONED_ONLY);
                cc.setStore(null);

                return null;
            }
        };

        GridTestUtils.assertThrows(log, new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                startGrid(2);

                return null;
            }
        }, IgniteCheckedException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStoreCheckTransactionalClient() throws Exception {
        cacheEnabled = true;

        cacheMode = PARTITIONED;

        initCache = new C1<GridCacheConfiguration, Void>() {
            @Override public Void apply(GridCacheConfiguration cc) {
                cc.setAtomicityMode(TRANSACTIONAL);
                cc.setDistributionMode(PARTITIONED_ONLY);
                cc.setStore(new TestStore());

                return null;
            }
        };

        startGrid(1);

        initCache = new C1<GridCacheConfiguration, Void>() {
            @Override public Void apply(GridCacheConfiguration cc) {
                cc.setAtomicityMode(TRANSACTIONAL);
                cc.setDistributionMode(CLIENT_ONLY);
                cc.setStore(null);

                return null;
            }
        };

        GridTestUtils.assertThrows(log, new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                startGrid(2);

                return null;
            }
        }, IgniteCheckedException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityForReplicatedCache() throws Exception {
        cacheEnabled = true;

        aff = new GridCachePartitionFairAffinity(); // Check cannot use GridCachePartitionFairAffinity.

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(1);
            }
        }, IgniteCheckedException.class, null);

        aff = new GridCacheConsistentHashAffinityFunction(true); // Check cannot set 'excludeNeighbors' flag.
        backups = Integer.MAX_VALUE;

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(1);
            }
        }, IgniteCheckedException.class, null);

        aff = new GridCacheConsistentHashAffinityFunction(false, 100);

        startGrid(1);

        // Try to start node with  different number of partitions.
        aff = new GridCacheConsistentHashAffinityFunction(false, 200);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return startGrid(2);
            }
        }, IgniteCheckedException.class, "Affinity partitions count mismatch");
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentInterceptors() throws Exception {
        cacheMode = PARTITIONED;

        checkSecondGridStartFails(
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(GridCacheConfiguration cfg) {
                    cfg.setInterceptor(new GridCacheInterceptorAdapter() {/*No-op.*/});
                    return null;
                }
            },
            new C1<GridCacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(GridCacheConfiguration cfg) {
                    return null;
                }
            }
        );
    }

    /**
     * @param initCache1 Closure.
     * @param initCache2 Closure.
     * @throws Exception If failed.
     */
    private void checkSecondGridStartFails(C1<GridCacheConfiguration, Void> initCache1,
                                           C1<GridCacheConfiguration, Void> initCache2) throws Exception {
        cacheEnabled = true;

        initCache = initCache1;

        startGrid(1);

        initCache = initCache2;

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(2);
            }
        }, IgniteCheckedException.class, null);
    }

    /** */
    private static class TestStore implements GridCacheStore<Object,Object> {
        /** {@inheritDoc} */
        @Nullable @Override public Object load(@Nullable IgniteTx tx, Object key)
            throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo,
            @Nullable Object... args) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void loadAll(@Nullable IgniteTx tx,
            @Nullable Collection<?> keys, IgniteBiInClosure<Object, Object> c) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void put(@Nullable IgniteTx tx, Object key,
            @Nullable Object val) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void putAll(@Nullable IgniteTx tx, @Nullable Map<?, ?> map)
            throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void remove(@Nullable IgniteTx tx, Object key)
            throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void removeAll(@Nullable IgniteTx tx,
            @Nullable Collection<?> keys) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void txEnd(IgniteTx tx, boolean commit) throws IgniteCheckedException {
            // No-op.
        }
    }
}
