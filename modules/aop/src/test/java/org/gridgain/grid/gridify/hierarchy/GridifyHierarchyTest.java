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

package org.gridgain.grid.gridify.hierarchy;

import org.gridgain.testframework.junits.common.*;

/**
 * Gridify hierarchy test.
 */
public class GridifyHierarchyTest extends GridCommonAbstractTest {
    /** */
    public GridifyHierarchyTest() {
        super(true);
    }

    /** */
    public void noneTestGridifyHierarchyProtected() {
        GridTarget target = new GridTarget();

        target.methodA();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridifyHierarchyPrivate() throws Exception {
        GridTarget target = new GridTarget();

        target.methodB();
    }

   /** {@inheritDoc} */
    @Override public String getTestGridName() {
        return "GridifyHierarchyTest";
    }
}
