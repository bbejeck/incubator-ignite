/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.deployment.uri;

import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.apache.ignite.spi.deployment.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.spi.*;

import java.io.*;
import java.util.*;

/**
 *
 */
@GridSpiTest(spi = GridUriDeploymentSpi.class, group = "Deployment SPI")
public class GridUriDeploymentSimpleSelfTest extends GridSpiAbstractTest<GridUriDeploymentSpi> {
    /**
     * @return List of URI to use as deployment source.
     */
    @GridSpiTestConfig
    public List<String> getUriList() {
        // No real gar file is required. Add one just to avoid failure because of missed default directory.
        return Collections.singletonList(GridTestProperties.getProperty("ant.urideployment.gar.uri").
            replace("EXTDATA", U.resolveGridGainPath("modules/extdata").getAbsolutePath()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleDeploy() throws Exception {
        GridUriDeploymentSpi spi = getSpi();

        spi.register(TestTask.class.getClassLoader(), TestTask.class);

        DeploymentResource task = spi.findResource(TestTask.class.getName());

        assert task != null;
        assert task.getResourceClass() == TestTask.class;
        assert spi.findResource("TestTaskWithName") == null;

        spi.unregister(TestTask.class.getName());

        assert spi.findResource(TestTask.class.getName()) == null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleRedeploy() throws Exception {
        for (int i = 0; i < 100; i++)
            testSimpleDeploy();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleDeployWithName() throws Exception {
        GridUriDeploymentSpi spi = getSpi();

        spi.register(TestTaskWithName.class.getClassLoader(), TestTaskWithName.class);

        DeploymentResource task = spi.findResource("TestTaskWithName");

        assert task != null;
        assert task.getResourceClass() == TestTaskWithName.class;
        assert spi.findResource(TestTaskWithName.class.getName()) != null;

        spi.unregister("TestTaskWithName");

        assert spi.findResource("TestTaskWithName") == null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleRedeployWithName() throws Exception {
        for (int i = 0; i < 100; i++)
            testSimpleDeployWithName();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleDeployTwoTasks() throws Exception {
        GridUriDeploymentSpi spi = getSpi();

        spi.register(TestTask.class.getClassLoader(), TestTask.class);
        spi.register(TestTaskWithName.class.getClassLoader(), TestTaskWithName.class);

        DeploymentResource task1 = spi.findResource("TestTaskWithName");
        DeploymentResource task2 = spi.findResource(TestTask.class.getName());

        assert task1 != null;
        assert task1.getResourceClass() == TestTaskWithName.class;
        assert spi.findResource(TestTaskWithName.class.getName()) != null;

        assert task2 != null;
        assert task2.getResourceClass() == TestTask.class;
        assert spi.findResource("TestTask") == null;

        spi.unregister("TestTaskWithName");

        assert spi.findResource("TestTaskWithName") == null;

        spi.unregister(TestTask.class.getName());

        assert spi.findResource(TestTask.class.getName()) == null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleRedeployTwoTasks() throws Exception {
        for (int i = 0; i < 100; i++)
            testSimpleDeployTwoTasks();
    }

    /**
     * Test task.
     */
    private static class TestTask extends ComputeTaskAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) throws GridException {
            assert subgrid.size() == 1;

            return Collections.singletonMap(new ComputeJobAdapter() {
                @Override public Serializable execute() { return "result"; }
            }, subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
            assert results.size() == 1;

            return results.get(0).getData();
        }
    }

    /**
     * Named test task.
     */
    @ComputeTaskName("TestTaskWithName")
    private static class TestTaskWithName extends ComputeTaskAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) throws GridException {
            assert subgrid.size() == 1;

            return Collections.singletonMap(new ComputeJobAdapter() {
                @Override public Serializable execute() { return "result"; }
            }, subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
            assert results.size() == 1;

            return results.get(0).getData();
        }
    }
}