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

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.util.ipc.loopback.*;
import org.gridgain.grid.util.ipc.shmem.*;
import org.gridgain.grid.util.typedef.*;

import java.util.concurrent.*;

import static org.gridgain.testframework.GridTestUtils.*;

/**
 * Tests for {@link GridGgfsServerManager} that checks shmem IPC endpoint registration
 * forbidden for Windows.
 */
public class GridGgfsServerManagerIpcEndpointRegistrationOnWindowsSelfTest
    extends GridGgfsServerManagerIpcEndpointRegistrationAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testShmemEndpointsRegistration() throws Exception {
        Throwable e = assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteConfiguration cfg = gridConfiguration();

                cfg.setGgfsConfiguration(gridGgfsConfiguration(
                    "{type:'shmem', port:" + GridIpcSharedMemoryServerEndpoint.DFLT_IPC_PORT + "}"));

                return G.start(cfg);
            }
        }, IgniteCheckedException.class, null);

        assert e.getCause().getMessage().contains(" should not be configured on Windows (configure " +
            GridIpcServerTcpEndpoint.class.getSimpleName() + ")");
    }
}
