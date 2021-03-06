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

package org.gridgain.grid.ggfs;

import static org.apache.ignite.fs.IgniteFsMode.*;
import static org.gridgain.grid.util.ipc.shmem.GridIpcSharedMemoryServerEndpoint.*;

/**
 * Tests Hadoop 2.x file system in primary mode.
 */
public class GridGgfsHadoop20FileSystemShmemPrimarySelfTest extends GridGgfsHadoop20FileSystemAbstractSelfTest {
    /**
     * Creates test in primary mode.
     */
    public GridGgfsHadoop20FileSystemShmemPrimarySelfTest() {
        super(PRIMARY);
    }

    /** {@inheritDoc} */
    @Override protected String primaryFileSystemUriPath() {
        return "ggfs://ggfs:" + getTestGridName(0) + "@/";
    }

    /** {@inheritDoc} */
    @Override protected String primaryFileSystemConfigPath() {
        return "/modules/core/src/test/config/hadoop/core-site.xml";
    }

    /** {@inheritDoc} */
    @Override protected String primaryIpcEndpointConfiguration(String gridName) {
        return "{type:'shmem', port:" + (DFLT_IPC_PORT + getTestGridIndex(gridName)) + "}";
    }

    /** {@inheritDoc} */
    @Override protected String secondaryFileSystemUriPath() {
        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override protected String secondaryFileSystemConfigPath() {
        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override protected String secondaryIpcEndpointConfiguration() {
        assert false;

        return null;
    }
}
