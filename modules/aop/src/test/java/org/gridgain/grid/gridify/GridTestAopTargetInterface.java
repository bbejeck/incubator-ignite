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

package org.gridgain.grid.gridify;

import org.apache.ignite.compute.gridify.*;

import java.io.*;

/**
 * Test AOP target interface.
 */
public interface GridTestAopTargetInterface extends Serializable {
    /**
     * @param arg Argument.
     * @return Result.
     */
    @Gridify(gridName="GridTestAopTargetInterface")
    public int gridifyDefault(String arg);

    /**
     * @param arg Argument.
     * @return Result.
     */
    @Gridify(gridName="GridTestAopTargetInterface", taskName = GridTestGridifyTask.TASK_NAME)
    public int gridifyNonDefaultName(String arg);

    /**
     * @param arg Argument.
     * @return Result.
     */
    @Gridify(gridName="GridTestAopTargetInterface", taskClass = GridTestGridifyTask.class)
    public int gridifyNonDefaultClass(String arg);

    /**
     * @param arg Argument.
     * @return Result.
     */
    @Gridify(gridName="GridTestAopTargetInterface", taskName = "")
    public int gridifyNoName(String arg);

    /**
     * @param arg Argument.
     * @return Result.
     * @throws GridTestGridifyException If failed.
     */
    @Gridify(gridName="GridTestAopTargetInterface")
    public int gridifyDefaultException(String arg) throws GridTestGridifyException;

    /**
     * @param arg Argument.
     * @return Result.
     * @throws GridTestGridifyException If failed.
     */
    @Gridify(gridName="GridTestAopTargetInterface")
    public int gridifyDefaultResource(String arg) throws GridTestGridifyException;

    /**
     * @param arg Argument.
     * @return Result.
     * @throws GridTestGridifyException If failed.
     */
    @Gridify(gridName="GridTestAopTargetInterface", taskClass = GridTestGridifyTask.class)
    public int gridifyNonDefaultClassResource(String arg) throws GridTestGridifyException;

    /**
     * @param arg Argument.
     * @return Result.
     * @throws GridTestGridifyException If failed.
     */
    @Gridify(gridName="GridTestAopTargetInterface", taskName = GridTestGridifyTask.TASK_NAME)
    public int gridifyNonDefaultNameResource(String arg) throws GridTestGridifyException;
}
