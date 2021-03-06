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

package org.gridgain.loadtests.hashmap;

import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.dr.os.*;
import org.gridgain.grid.kernal.processors.cache.jta.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.query.continuous.*;
import org.gridgain.grid.kernal.processors.cache.transactions.*;
import org.gridgain.testframework.junits.*;

import static org.gridgain.testframework.junits.GridAbstractTest.*;

/**
 * Cache test context.
 */
public class GridCacheTestContext<K, V> extends GridCacheContext<K, V> {
    /**
     */
    @SuppressWarnings("NullableProblems")
    public GridCacheTestContext(GridTestKernalContext ctx) {
        super(
            ctx,
            new GridCacheSharedContext<>(
                ctx,
                new IgniteTxManager<K, V>(),
                new GridCacheVersionManager<K, V>(),
                new GridCacheMvccManager<K, V>(),
                new GridCacheDeploymentManager<K, V>(),
                new GridCachePartitionExchangeManager<K, V>(),
                new GridCacheIoManager<K, V>()
            ),
            defaultCacheConfiguration(),
            new GridCacheEventManager<K, V>(),
            new GridCacheSwapManager<K, V>(false),
            new GridCacheStoreManager<K, V>(null),
            new GridCacheEvictionManager<K, V>(),
            new GridCacheLocalQueryManager<K, V>(),
            new GridCacheContinuousQueryManager<K, V>(),
            new GridCacheAffinityManager<K, V>(),
            new GridCacheDataStructuresManager<K, V>(),
            new GridCacheTtlManager<K, V>(),
            new GridOsCacheDrManager<K, V>(),
            new GridCacheNoopJtaManager<K, V>());
    }
}
