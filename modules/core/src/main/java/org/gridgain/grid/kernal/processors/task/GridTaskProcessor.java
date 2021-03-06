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

package org.gridgain.grid.kernal.processors.task;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.plugin.security.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.events.IgniteEventType.*;
import static org.gridgain.grid.kernal.GridTopic.*;
import static org.gridgain.grid.kernal.managers.communication.GridIoPolicy.*;
import static org.gridgain.grid.kernal.processors.task.GridTaskThreadContextKey.*;

/**
 * This class defines task processor.
 */
public class GridTaskProcessor extends GridProcessorAdapter {
    /** Wait for 5 seconds to allow discovery to take effect (best effort). */
    private static final long DISCO_TIMEOUT = 5000;

    /** */
    private static final Map<GridTaskThreadContextKey, Object> EMPTY_ENUM_MAP =
        new EnumMap<>(GridTaskThreadContextKey.class);

    /** */
    private final IgniteMarshaller marsh;

    /** */
    private final ConcurrentMap<IgniteUuid, GridTaskWorker<?, ?>> tasks = GridConcurrentFactory.newMap();

    /** */
    private boolean stopping;

    /** */
    private boolean waiting;

    /** */
    private final GridLocalEventListener discoLsnr;

    /** Total executed tasks. */
    private final LongAdder execTasks = new LongAdder();

    /** */
    private final ThreadLocal<Map<GridTaskThreadContextKey, Object>> thCtx =
        new ThreadLocal<>();

    /** */
    private final GridSpinReadWriteLock lock = new GridSpinReadWriteLock();

    /** Internal metadata cache. */
    private GridCache<GridTaskNameHashKey, String> tasksMetaCache;

    /**
     * @param ctx Kernal context.
     */
    public GridTaskProcessor(GridKernalContext ctx) {
        super(ctx);

        marsh = ctx.config().getMarshaller();

        discoLsnr = new TaskDiscoveryListener();

        tasksMetaCache = ctx.security().enabled() ? ctx.cache().<GridTaskNameHashKey, String>utilityCache() : null;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        ctx.event().addLocalEventListener(discoLsnr, EVT_NODE_FAILED, EVT_NODE_LEFT);

        ctx.io().addMessageListener(TOPIC_JOB_SIBLINGS, new JobSiblingsMessageListener());
        ctx.io().addMessageListener(TOPIC_TASK_CANCEL, new TaskCancelMessageListener());
        ctx.io().addMessageListener(TOPIC_TASK, new JobMessageListener(true));

        if (log.isDebugEnabled())
            log.debug("Started task processor.");
    }

    /** {@inheritDoc} */
    @SuppressWarnings("TooBroadScope")
    @Override public void onKernalStop(boolean cancel) {
        lock.writeLock();

        try {
            stopping = true;

            waiting = !cancel;
        }
        finally {
            lock.writeUnlock();
        }

        int size = tasks.size();

        if (size > 0) {
            if (cancel)
                U.warn(log, "Will cancel unfinished tasks due to stopping of the grid [cnt=" + size + "]");
            else
                U.warn(log, "Will wait for all job responses from worker nodes before stopping grid.");

            for (GridTaskWorker<?, ?> task : tasks.values()) {
                if (!cancel) {
                    try {
                        task.getTaskFuture().get();
                    }
                    catch (ComputeTaskCancelledException e) {
                        U.warn(log, e.getMessage());
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Task failed: " + task, e);
                    }
                }
                else {
                    for (ClusterNode node : ctx.discovery().nodes(task.getSession().getTopology())) {
                        if (ctx.localNodeId().equals(node.id()))
                            ctx.job().masterLeaveLocal(task.getSession().getId());
                    }

                    task.cancel();

                    Throwable ex = new ComputeTaskCancelledException("Task cancelled due to stopping of the grid: " +
                        task);

                    task.finishTask(null, ex, false);
                }
            }

            U.join(tasks.values(), log);
        }

        // Remove discovery and message listeners.
        ctx.event().removeLocalEventListener(discoLsnr);

        ctx.io().removeMessageListener(TOPIC_JOB_SIBLINGS);
        ctx.io().removeMessageListener(TOPIC_TASK_CANCEL);

        // Set waiting flag to false to make sure that we do not get
        // listener notifications any more.
        if (!cancel) {
            lock.writeLock();

            try {
                waiting = false;
            }
            finally {
                lock.writeUnlock();
            }
        }

        assert tasks.isEmpty();

        if (log.isDebugEnabled())
            log.debug("Finished executing task processor onKernalStop() callback.");
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        if (log.isDebugEnabled())
            log.debug("Stopped task processor.");
    }

    /**
     * Sets the thread-local context value.
     *
     * @param key Key.
     * @param val Value.
     */
    public void setThreadContext(GridTaskThreadContextKey key, Object val) {
        assert key != null;
        assert val != null;

        Map<GridTaskThreadContextKey, Object> map = thCtx.get();

        // NOTE: access to 'map' is always single-threaded since it's held
        // in a thread local.
        if (map == null)
            thCtx.set(map = new EnumMap<>(GridTaskThreadContextKey.class));

        map.put(key, val);
    }

    /**
     * Sets the thread-local context value, if it is not null.
     *
     * @param key Key.
     * @param val Value.
     */
    public void setThreadContextIfNotNull(GridTaskThreadContextKey key, @Nullable Object val) {
        if (val != null)
            setThreadContext(key, val);
    }

    /**
     * Gets thread-local context value for a given {@code key}.
     *
     * @param key Thread-local context key.
     * @return Thread-local context value associated with given {@code key} - or {@code null}
     *      if value with given {@code key} doesn't exist.
     */
    @Nullable public <T> T getThreadContext(GridTaskThreadContextKey key) {
        assert(key != null);

        Map<GridTaskThreadContextKey, Object> map = thCtx.get();

        return map == null ? null : (T)map.get(key);
    }

    /**
     * Gets currently used deployments.
     *
     * @return Currently used deployments.
     */
    public Collection<GridDeployment> getUsedDeployments() {
        return F.viewReadOnly(tasks.values(), new C1<GridTaskWorker<?, ?>, GridDeployment>() {
            @Override public GridDeployment apply(GridTaskWorker<?, ?> w) {
                return w.getDeployment();
            }
        });
    }

    /**
     * Gets currently used deployments mapped by task name or aliases.
     *
     * @return Currently used deployments.
     */
    public Map<String, GridDeployment> getUsedDeploymentMap() {
        Map<String, GridDeployment> deps = new HashMap<>();

        for (GridTaskWorker w : tasks.values()) {
            GridTaskSessionImpl ses = w.getSession();

            deps.put(ses.getTaskClassName(), w.getDeployment());

            if (ses.getTaskName() != null && ses.getTaskClassName().equals(ses.getTaskName()))
                deps.put(ses.getTaskName(), w.getDeployment());
        }

        return deps;
    }

    /**
     * @param taskCls Task class.
     * @param arg Optional execution argument.
     * @return Task future.
     * @param <T> Task argument type.
     * @param <R> Task return value type.
     */
    public <T, R> ComputeTaskFuture<R> execute(Class<? extends ComputeTask<T, R>> taskCls, @Nullable T arg) {
        assert taskCls != null;

        lock.readLock();

        try {
            if (stopping)
                throw new IllegalStateException("Failed to execute task due to grid shutdown: " + taskCls);

            return startTask(null, taskCls, null, IgniteUuid.fromUuid(ctx.localNodeId()), arg, false);
        }
        finally {
            lock.readUnlock();
        }
    }

    /**
     * @param task Actual task.
     * @param arg Optional task argument.
     * @return Task future.
     * @param <T> Task argument type.
     * @param <R> Task return value type.
     */
    public <T, R> ComputeTaskFuture<R> execute(ComputeTask<T, R> task, @Nullable T arg) {
        return execute(task, arg, false);
    }

    /**
     * @param task Actual task.
     * @param arg Optional task argument.
     * @param sys If {@code true}, then system pool will be used.
     * @return Task future.
     * @param <T> Task argument type.
     * @param <R> Task return value type.
     */
    public <T, R> ComputeTaskFuture<R> execute(ComputeTask<T, R> task, @Nullable T arg, boolean sys) {
        lock.readLock();

        try {
            if (stopping)
                throw new IllegalStateException("Failed to execute task due to grid shutdown: " + task);

            return startTask(null, null, task, IgniteUuid.fromUuid(ctx.localNodeId()), arg, sys);
        }
        finally {
            lock.readUnlock();
        }
    }

    /**
     * Resolves task name by task name hash.
     *
     * @param taskNameHash Task name hash.
     * @return Task name or {@code null} if not found.
     */
    public String resolveTaskName(int taskNameHash) {
        if (taskNameHash == 0)
            return null;

        assert ctx.security().enabled();

        return tasksMetaCache.peek(new GridTaskNameHashKey(taskNameHash));
    }

    /**
     * @param taskName Task name.
     * @param arg Optional execution argument.
     * @return Task future.
     * @param <T> Task argument type.
     * @param <R> Task return value type.
     */
    public <T, R> ComputeTaskFuture<R> execute(String taskName, @Nullable T arg) {
        assert taskName != null;

        lock.readLock();

        try {
            if (stopping)
                throw new IllegalStateException("Failed to execute task due to grid shutdown: " + taskName);

            return startTask(taskName, null, null, IgniteUuid.fromUuid(ctx.localNodeId()), arg, false);
        }
        finally {
            lock.readUnlock();
        }
    }

    /**
     * @param taskName Task name.
     * @param taskCls Task class.
     * @param task Task.
     * @param sesId Task session ID.
     * @param arg Optional task argument.
     * @param sys If {@code true}, then system pool will be used.
     * @return Task future.
     */
    @SuppressWarnings("unchecked")
    private <T, R> ComputeTaskFuture<R> startTask(
        @Nullable String taskName,
        @Nullable Class<?> taskCls,
        @Nullable ComputeTask<T, R> task,
        IgniteUuid sesId,
        @Nullable T arg,
        boolean sys) {
        assert sesId != null;

        String taskClsName;

        if (task != null)
            taskClsName = task.getClass().getName();
        else
            taskClsName = taskCls != null ? taskCls.getName() : taskName;

        ctx.security().authorize(taskClsName, GridSecurityPermission.TASK_EXECUTE, null);

        // Get values from thread-local context.
        Map<GridTaskThreadContextKey, Object> map = thCtx.get();

        if (map == null)
            map = EMPTY_ENUM_MAP;
        else
            // Reset thread-local context.
            thCtx.remove();

        Long timeout = (Long)map.get(TC_TIMEOUT);

        long timeout0 = timeout == null || timeout == 0 ? Long.MAX_VALUE : timeout;

        long startTime = U.currentTimeMillis();

        long endTime = timeout0 + startTime;

        // Account for overflow.
        if (endTime < 0)
            endTime = Long.MAX_VALUE;

        IgniteCheckedException deployEx = null;
        GridDeployment dep = null;

        // User provided task name.
        if (taskName != null) {
            assert taskCls == null;
            assert task == null;

            try {
                dep = ctx.deploy().getDeployment(taskName);

                if (dep == null)
                    throw new GridDeploymentException("Unknown task name or failed to auto-deploy " +
                        "task (was task (re|un)deployed?): " + taskName);

                taskCls = dep.deployedClass(taskName);

                if (taskCls == null)
                    throw new GridDeploymentException("Unknown task name or failed to auto-deploy " +
                        "task (was task (re|un)deployed?) [taskName=" + taskName + ", dep=" + dep + ']');

                if (!ComputeTask.class.isAssignableFrom(taskCls))
                    throw new IgniteCheckedException("Failed to auto-deploy task (deployed class is not a task) [taskName=" +
                        taskName + ", depCls=" + taskCls + ']');
            }
            catch (IgniteCheckedException e) {
                deployEx = e;
            }
        }
        // Deploy user task class.
        else if (taskCls != null) {
            assert task == null;

            try {
                // Implicit deploy.
                dep = ctx.deploy().deploy(taskCls, U.detectClassLoader(taskCls));

                if (dep == null)
                    throw new GridDeploymentException("Failed to auto-deploy task (was task (re|un)deployed?): " +
                        taskCls);

                taskName = taskName(dep, taskCls, map);
            }
            catch (IgniteCheckedException e) {
                taskName = taskCls.getName();

                deployEx = e;
            }
        }
        // Deploy user task.
        else if (task != null) {
            try {
                ClassLoader ldr;

                Class<?> cls;

                if (task instanceof GridPeerDeployAware) {
                    GridPeerDeployAware depAware = (GridPeerDeployAware)task;

                    cls = depAware.deployClass();
                    ldr = depAware.classLoader();

                    // Set proper class name to make peer-loading possible.
                    taskCls = cls;
                }
                else {
                    taskCls = task.getClass();

                    assert ComputeTask.class.isAssignableFrom(taskCls);

                    cls = task.getClass();
                    ldr = U.detectClassLoader(cls);
                }

                // Explicit deploy.
                dep = ctx.deploy().deploy(cls, ldr);

                if (dep == null)
                    throw new GridDeploymentException("Failed to auto-deploy task (was task (re|un)deployed?): " + cls);

                taskName = taskName(dep, taskCls, map);
            }
            catch (IgniteCheckedException e) {
                taskName = task.getClass().getName();

                deployEx = e;
            }
        }

        assert taskName != null;

        if (log.isDebugEnabled())
            log.debug("Task deployment: " + dep);

        boolean fullSup = dep != null && taskCls!= null &&
            dep.annotation(taskCls, ComputeTaskSessionFullSupport.class) != null;

        Collection<? extends ClusterNode> nodes = (Collection<? extends ClusterNode>)map.get(TC_SUBGRID);

        Collection<UUID> top = nodes != null ? F.nodeIds(nodes) : null;

        UUID subjId = getThreadContext(TC_SUBJ_ID);

        if (subjId == null)
            subjId = ctx.localNodeId();

        // Creates task session with task name and task version.
        GridTaskSessionImpl ses = ctx.session().createTaskSession(
            sesId,
            ctx.config().getNodeId(),
            taskName,
            dep,
            taskCls == null ? null : taskCls.getName(),
            top,
            startTime,
            endTime,
            Collections.<ComputeJobSibling>emptyList(),
            Collections.emptyMap(),
            fullSup,
            subjId);

        GridTaskFutureImpl<R> fut = new GridTaskFutureImpl<>(ses, ctx);

        IgniteCheckedException securityEx = null;

        if (ctx.security().enabled() && deployEx == null) {
            try {
                saveTaskMetadata(taskName);
            }
            catch (IgniteCheckedException e) {
                securityEx = e;
            }
        }

        if (deployEx == null && securityEx == null) {
            if (dep == null || !dep.acquire())
                handleException(new GridDeploymentException("Task not deployed: " + ses.getTaskName()), fut);
            else {
                GridTaskWorker<?, ?> taskWorker = new GridTaskWorker<>(
                    ctx,
                    arg,
                    ses,
                    fut,
                    taskCls,
                    task,
                    dep,
                    new TaskEventListener(),
                    map,
                    subjId);

                if (task != null) {
                    // Check if someone reuses the same task instance by walking
                    // through the "tasks" map
                    for (GridTaskWorker worker : tasks.values()) {
                        ComputeTask workerTask = worker.getTask();

                        // Check that the same instance of task is being used by comparing references.
                        if (workerTask != null && task == workerTask)
                            U.warn(log, "Most likely the same task instance is being executed. " +
                                "Please avoid executing the same task instances in parallel because " +
                                "they may have concurrent resources access and conflict each other: " + task);
                    }
                }

                GridTaskWorker<?, ?> taskWorker0 = tasks.putIfAbsent(sesId, taskWorker);

                assert taskWorker0 == null : "Session ID is not unique: " + sesId;

                if (dep.annotation(taskCls, ComputeTaskMapAsync.class) != null) {
                    try {
                        // Start task execution in another thread.
                        if (sys)
                            ctx.config().getSystemExecutorService().execute(taskWorker);
                        else
                            ctx.config().getExecutorService().execute(taskWorker);
                    }
                    catch (RejectedExecutionException e) {
                        tasks.remove(sesId);

                        release(dep);

                        handleException(new ComputeExecutionRejectedException("Failed to execute task " +
                            "due to thread pool execution rejection: " + taskName, e), fut);
                    }
                }
                else
                    taskWorker.run();
            }
        }
        else {
            if (deployEx != null)
                handleException(deployEx, fut);
            else
                handleException(securityEx, fut);
        }

        return fut;
    }

    /**
     * @param sesId Task's session id.
     * @return A {@link org.apache.ignite.compute.ComputeTaskFuture} instance or {@code null} if no such task found.
     */
    @Nullable public <R> ComputeTaskFuture<R> taskFuture(IgniteUuid sesId) {
        GridTaskWorker<?, ?> taskWorker = tasks.get(sesId);

        return taskWorker != null ? (ComputeTaskFuture<R>)taskWorker.getTaskFuture() : null;
    }

    /**
     * @return Active task futures.
     */
    @SuppressWarnings("unchecked")
    public <R> Map<IgniteUuid, ComputeTaskFuture<R>> taskFutures() {
        Map<IgniteUuid, ComputeTaskFuture<R>> res = U.newHashMap(tasks.size());

        for (GridTaskWorker taskWorker : tasks.values()) {
            ComputeTaskFuture<R> fut = taskWorker.getTaskFuture();

            res.put(fut.getTaskSession().getId(), fut);
        }

        return res;
    }

    /**
     * Gets task name for a task class. It firstly checks
     * {@link @GridComputeTaskName} annotation, then thread context
     * map. If both are empty, class name is returned.
     *
     * @param dep Deployment.
     * @param cls Class.
     * @param map Thread context map.
     * @return Task name.
     * @throws IgniteCheckedException If {@link @GridComputeTaskName} annotation is found, but has empty value.
     */
    private String taskName(GridDeployment dep, Class<?> cls,
        Map<GridTaskThreadContextKey, Object> map) throws IgniteCheckedException {
        assert dep != null;
        assert cls != null;
        assert map != null;

        String taskName;

        ComputeTaskName ann = dep.annotation(cls, ComputeTaskName.class);

        if (ann != null) {
            taskName = ann.value();

            if (F.isEmpty(taskName))
                throw new IgniteCheckedException("Task name specified by @GridComputeTaskName annotation" +
                    " cannot be empty for class: " + cls);
        }
        else
            taskName = map.containsKey(TC_TASK_NAME) ? (String)map.get(TC_TASK_NAME) : cls.getName();

        return taskName;
    }

    /**
     * Saves task name metadata to utility cache.
     *
     * @param taskName Task name.
     */
    private void saveTaskMetadata(String taskName) throws IgniteCheckedException {
        if (ctx.isDaemon())
            return;

        assert ctx.security().enabled();

        int nameHash = taskName.hashCode();

        // 0 is reserved for no task.
        if (nameHash == 0)
            nameHash = 1;

        GridTaskNameHashKey key = new GridTaskNameHashKey(nameHash);

        String existingName = tasksMetaCache.get(key);

        if (existingName == null)
            existingName = tasksMetaCache.putIfAbsent(key, taskName);

        if (existingName != null && !F.eq(existingName, taskName))
            throw new IgniteCheckedException("Task name hash collision for security-enabled node [taskName=" + taskName +
                ", existing taskName=" + existingName + ']');
    }

    /**
     * @param dep Deployment to release.
     */
    private void release(GridDeployment dep) {
        assert dep != null;

        dep.release();

        if (dep.obsolete())
            ctx.resource().onUndeployed(dep);
    }

    /**
     * @param ex Exception.
     * @param fut Task future.
     * @param <R> Result type.
     */
    private <R> void handleException(Throwable ex, GridTaskFutureImpl<R> fut) {
        assert ex != null;
        assert fut != null;

        fut.onDone(ex);
    }

    /**
     * @param ses Task session.
     * @param attrs Attributes.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void setAttributes(GridTaskSessionImpl ses, Map<?, ?> attrs) throws IgniteCheckedException {
        long timeout = ses.getEndTime() - U.currentTimeMillis();

        if (timeout <= 0) {
            U.warn(log, "Task execution timed out (remote session attributes won't be set): " + ses);

            return;
        }

        // If setting from task or future.
        if (log.isDebugEnabled())
            log.debug("Setting session attribute(s) from task or future: " + ses);

        sendSessionAttributes(attrs, ses);
    }

    /**
     * This method will make the best attempt to send attributes to all jobs.
     *
     * @param attrs Deserialized session attributes.
     * @param ses Task session.
     * @throws IgniteCheckedException If send to any of the jobs failed.
     */
    @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter", "BusyWait"})
    private void sendSessionAttributes(Map<?, ?> attrs, GridTaskSessionImpl ses)
        throws IgniteCheckedException {
        assert attrs != null;
        assert ses != null;

        Collection<ComputeJobSibling> siblings = ses.getJobSiblings();

        GridIoManager commMgr = ctx.io();

        long timeout = ses.getEndTime() - U.currentTimeMillis();

        if (timeout <= 0) {
            U.warn(log, "Session attributes won't be set due to task timeout: " + attrs);

            return;
        }

        Map<UUID, Long> msgIds = new HashMap<>(siblings.size(), 1.0f);

        UUID locNodeId = ctx.localNodeId();

        synchronized (ses) {
            if (ses.isClosed()) {
                if (log.isDebugEnabled())
                    log.debug("Setting session attributes on closed session (will ignore): " + ses);

                return;
            }

            ses.setInternal(attrs);

            // Do this inside of synchronization block, so every message
            // ID will be associated with a certain session state.
            for (ComputeJobSibling s : siblings) {
                GridJobSiblingImpl sib = (GridJobSiblingImpl)s;

                UUID nodeId = sib.nodeId();

                if (!nodeId.equals(locNodeId) && !sib.isJobDone() && !msgIds.containsKey(nodeId))
                    msgIds.put(nodeId, commMgr.nextMessageId(sib.jobTopic(), nodeId));
            }
        }

        if (ctx.event().isRecordable(EVT_TASK_SESSION_ATTR_SET)) {
            IgniteEvent evt = new IgniteTaskEvent(
                ctx.discovery().localNode(),
                "Changed attributes: " + attrs,
                EVT_TASK_SESSION_ATTR_SET,
                ses.getId(),
                ses.getTaskName(),
                ses.getTaskClassName(),
                false,
                null);

            ctx.event().record(evt);
        }

        IgniteCheckedException ex = null;

        // Every job gets an individual message to keep track of ghost requests.
        for (ComputeJobSibling s : ses.getJobSiblings()) {
            GridJobSiblingImpl sib = (GridJobSiblingImpl)s;

            UUID nodeId = sib.nodeId();

            Long msgId = msgIds.remove(nodeId);

            // Pair can be null if job is finished.
            if (msgId != null) {
                assert msgId > 0;

                ClusterNode node = ctx.discovery().node(nodeId);

                // Check that node didn't change (it could happen in case of failover).
                if (node != null) {
                    boolean loc = node.id().equals(ctx.localNodeId()) && !ctx.config().isMarshalLocalJobs();

                    GridTaskSessionRequest req = new GridTaskSessionRequest(
                        ses.getId(),
                        null,
                        loc ? null : marsh.marshal(attrs),
                        attrs);

                    // Make sure to go through IO manager always, since order
                    // should be preserved here.
                    try {
                        commMgr.sendOrderedMessage(
                            node,
                            sib.jobTopic(),
                            msgId,
                            req,
                            SYSTEM_POOL,
                            timeout,
                            false);
                    }
                    catch (IgniteCheckedException e) {
                        node = ctx.discovery().node(nodeId);

                        if (node != null) {
                            try {
                                // Since communication on remote node may stop before
                                // we get discovery notification, we give ourselves the
                                // best effort to detect it.
                                Thread.sleep(DISCO_TIMEOUT);
                            }
                            catch (InterruptedException ignore) {
                                U.warn(log, "Got interrupted while sending session attributes.");
                            }

                            node = ctx.discovery().node(nodeId);
                        }

                        String err = "Failed to send session attribute request message to node " +
                            "(normal case if node left grid) [node=" + node + ", req=" + req + ']';

                        if (node != null)
                            U.warn(log, err);
                        else if (log.isDebugEnabled())
                            log.debug(err);

                        if (ex == null)
                            ex = e;
                    }
                }
            }
        }

        if (ex != null)
            throw ex;
    }

    /**
     * @param nodeId Node ID.
     * @param msg Execute response message.
     */
    public void processJobExecuteResponse(UUID nodeId, GridJobExecuteResponse msg) {
        assert nodeId != null;
        assert msg != null;

        lock.readLock();

        try {
            if (stopping && !waiting) {
                U.warn(log, "Received job execution response while stopping grid (will ignore): " + msg);

                return;
            }

            GridTaskWorker<?, ?> task = tasks.get(msg.getSessionId());

            if (task == null) {
                if (log.isDebugEnabled())
                    log.debug("Received job execution response for unknown task (was task already reduced?): " + msg);

                return;
            }

            if (log.isDebugEnabled())
                log.debug("Received grid job response message [msg=" + msg + ", nodeId=" + nodeId + ']');

            task.onResponse(msg);
        }
        finally {
            lock.readUnlock();
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Task session request.
     */
    @SuppressWarnings({"unchecked"})
    private void processTaskSessionRequest(UUID nodeId, GridTaskSessionRequest msg) {
        assert nodeId != null;
        assert msg != null;

        lock.readLock();

        try {
            if (stopping && !waiting) {
                U.warn(log, "Received task session request while stopping grid (will ignore): " + msg);

                return;
            }

            GridTaskWorker<?, ?> task = tasks.get(msg.getSessionId());

            if (task == null) {
                if (log.isDebugEnabled())
                    log.debug("Received task session request for unknown task (was task already reduced?): " + msg);

                return;
            }

            boolean loc = ctx.localNodeId().equals(nodeId) && !ctx.config().isMarshalLocalJobs();

            Map<?, ?> attrs = loc ? msg.getAttributes() :
                marsh.<Map<?, ?>>unmarshal(msg.getAttributesBytes(), task.getTask().getClass().getClassLoader());

            GridTaskSessionImpl ses = task.getSession();

            sendSessionAttributes(attrs, ses);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to deserialize session request: " + msg, e);
        }
        finally {
            lock.readUnlock();
        }
    }

    /**
     * Handles user cancellation.
     *
     * @param sesId Session ID.
     */
    public void onCancelled(IgniteUuid sesId) {
        assert sesId != null;

        lock.readLock();

        try {
            if (stopping && !waiting) {
                U.warn(log, "Attempt to cancel task while stopping grid (will ignore): " + sesId);

                return;
            }

            GridTaskWorker<?, ?> task = tasks.get(sesId);

            if (task == null) {
                if (log.isDebugEnabled())
                    log.debug("Attempt to cancel unknown task (was task already reduced?): " + sesId);

                return;
            }

            task.finishTask(null, new ComputeTaskCancelledException("Task was cancelled."), true);
        }
        finally {
            lock.readUnlock();
        }
    }

    /**
     * @return Number of executed tasks.
     */
    public int getTotalExecutedTasks() {
        return execTasks.intValue();
    }

    /**
     * Resets processor metrics.
     */
    public void resetMetrics() {
        // Can't use 'reset' method because it is not thread-safe
        // according to javadoc.
        execTasks.add(-execTasks.sum());
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> Task processor memory stats [grid=" + ctx.gridName() + ']');
        X.println(">>>  tasksSize: " + tasks.size());
    }

    /**
     * Listener for individual task events.
     */
    @SuppressWarnings({"deprecation"})
    private class TaskEventListener implements GridTaskEventListener {
        /** */
        private final GridMessageListener msgLsnr = new JobMessageListener(false);

        /** {@inheritDoc} */
        @Override public void onTaskStarted(GridTaskWorker<?, ?> worker) {
            // Register for timeout notifications.
            if (worker.endTime() < Long.MAX_VALUE)
                ctx.timeout().addTimeoutObject(worker);
        }

        /** {@inheritDoc} */
        @Override public void onJobSend(GridTaskWorker<?, ?> worker, GridJobSiblingImpl sib) {
            if (worker.getSession().isFullSupport())
                // Listener is stateless, so same listener can be reused for all jobs.
                ctx.io().addMessageListener(sib.taskTopic(), msgLsnr);
        }

        /** {@inheritDoc} */
        @Override public void onJobFailover(GridTaskWorker<?, ?> worker, GridJobSiblingImpl sib, UUID nodeId) {
            GridIoManager ioMgr = ctx.io();

            // Remove message ID registration and old listener.
            if (worker.getSession().isFullSupport()) {
                ioMgr.removeMessageId(sib.jobTopic());
                ioMgr.removeMessageListener(sib.taskTopic(), msgLsnr);

                synchronized (worker.getSession()) {
                    // Reset ID on sibling prior to sending request.
                    sib.nodeId(nodeId);
                }

                // Register new listener on new topic.
                ioMgr.addMessageListener(sib.taskTopic(), msgLsnr);
            }
            else {
                // Update node ID only in case attributes are not enabled.
                synchronized (worker.getSession()) {
                    // Reset ID on sibling prior to sending request.
                    sib.nodeId(nodeId);
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void onJobFinished(GridTaskWorker<?, ?> worker, GridJobSiblingImpl sib) {
            // Mark sibling finished for the purpose of setting session attributes.
            synchronized (worker.getSession()) {
                sib.onJobDone();
            }
        }

        /** {@inheritDoc} */
        @Override public void onTaskFinished(GridTaskWorker<?, ?> worker) {
            GridTaskSessionImpl ses = worker.getSession();

            if (ses.isFullSupport()) {
                synchronized (worker.getSession()) {
                    worker.getSession().onClosed();
                }

                ctx.checkpoint().onSessionEnd(ses, false);

                // Delete session altogether.
                ctx.session().removeSession(ses.getId());
            }

            boolean rmv = tasks.remove(worker.getTaskSessionId(), worker);

            assert rmv;

            // Unregister from timeout notifications.
            if (worker.endTime() < Long.MAX_VALUE)
                ctx.timeout().removeTimeoutObject(worker);

            release(worker.getDeployment());

            if (!worker.isInternal())
                execTasks.increment();

            // Unregister job message listener from all job topics.
            if (ses.isFullSupport()) {
                try {
                    for (ComputeJobSibling sibling : worker.getSession().getJobSiblings()) {
                        GridJobSiblingImpl s = (GridJobSiblingImpl)sibling;

                        ctx.io().removeMessageId(s.jobTopic());
                        ctx.io().removeMessageListener(s.taskTopic(), msgLsnr);
                    }
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to unregister job communication message listeners and counters.", e);
                }
            }
        }
    }

    /**
     * Handles job execution responses and session requests.
     */
    private class JobMessageListener implements GridMessageListener {
        /** */
        private final boolean jobResOnly;

        /**
         * @param jobResOnly {@code True} if this listener is allowed to process
         *      job responses only (for tasks with disabled sessions).
         */
        private JobMessageListener(boolean jobResOnly) {
            this.jobResOnly = jobResOnly;
        }

        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg) {
            if (!(msg instanceof GridTaskMessage)) {
                U.warn(log, "Received message of unknown type: " + msg);

                return;
            }

            if (msg instanceof GridJobExecuteResponse)
                processJobExecuteResponse(nodeId, (GridJobExecuteResponse)msg);
            else if (jobResOnly)
                U.warn(log, "Received message of type other than job response: " + msg);
            else if (msg instanceof GridTaskSessionRequest)
                processTaskSessionRequest(nodeId, (GridTaskSessionRequest)msg);
            else
                U.warn(log, "Received message of unknown type: " + msg);
        }
    }

    /**
     * Listener to node discovery events.
     */
    private class TaskDiscoveryListener implements GridLocalEventListener {
        /** {@inheritDoc} */
        @Override public void onEvent(IgniteEvent evt) {
            assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT;

            UUID nodeId = ((IgniteDiscoveryEvent)evt).eventNode().id();

            lock.readLock();

            try {
                for (GridTaskWorker<?, ?> task : tasks.values())
                    task.onNodeLeft(nodeId);
            }
            finally {
                lock.readUnlock();
            }
        }
    }

    /**
     *
     */
    private class JobSiblingsMessageListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg) {
            if (!(msg instanceof GridJobSiblingsRequest)) {
                U.warn(log, "Received unexpected message instead of siblings request: " + msg);

                return;
            }

            lock.readLock();

            try {
                if (stopping && !waiting) {
                    U.warn(log, "Received job siblings request while stopping grid (will ignore): " + msg);

                    return;
                }

                GridJobSiblingsRequest req = (GridJobSiblingsRequest)msg;

                GridTaskWorker<?, ?> worker = tasks.get(req.sessionId());

                Collection<ComputeJobSibling> siblings;

                if (worker != null) {
                    try {
                        siblings = worker.getSession().getJobSiblings();
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to get job siblings [request=" + msg +
                            ", ses=" + worker.getSession() + ']', e);

                        siblings = null;
                    }
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Received job siblings request for unknown or finished task (will ignore): " + msg);

                    siblings = null;
                }

                try {
                    Object topic = req.topic();

                    if (topic == null) {
                        assert req.topicBytes() != null;

                        topic = marsh.unmarshal(req.topicBytes(), null);
                    }

                    boolean loc = ctx.localNodeId().equals(nodeId);

                    ctx.io().send(nodeId, topic,
                        new GridJobSiblingsResponse(
                            loc ? siblings : null,
                            loc ? null : marsh.marshal(siblings)),
                        SYSTEM_POOL);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to send job sibling response.", e);
                }
            }
            finally {
                lock.readUnlock();
            }
        }
    }

    /**
     * Listener for task cancel requests.
     */
    private class TaskCancelMessageListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg) {
            assert msg != null;

            if (!(msg instanceof GridTaskCancelRequest)) {
                U.warn(log, "Received unexpected message instead of task cancel request: " + msg);

                return;
            }

            GridTaskCancelRequest req = (GridTaskCancelRequest)msg;

            lock.readLock();

            try {
                if (stopping && !waiting) {
                    U.warn(log, "Received task cancel request while stopping grid (will ignore): " + msg);

                    return;
                }

                GridTaskWorker<?, ?> gridTaskWorker = tasks.get(req.sessionId());

                if (gridTaskWorker != null) {
                    try {
                        gridTaskWorker.getTaskFuture().cancel();
                    }
                    catch (IgniteCheckedException e) {
                        log.warning("Failed to cancel task: " + gridTaskWorker.getTask(), e);
                    }
                }
            }
            finally {
                lock.readUnlock();
            }
        }
    }
}
