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
import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import static org.apache.ignite.events.IgniteEventType.*;
import static org.gridgain.grid.kernal.GridTopic.*;
import static org.gridgain.grid.kernal.processors.ggfs.GridGgfsFileInfo.*;

/**
 * GGFS worker for removal from the trash directory.
 */
public class GridGgfsDeleteWorker extends GridGgfsThread {
    /** Awake frequency, */
    private static final long FREQUENCY = 1000;

    /** How many files/folders to delete at once (i.e in a single transaction). */
    private static final int MAX_DELETE_BATCH = 100;

    /** GGFS context. */
    private final GridGgfsContext ggfsCtx;

    /** Metadata manager. */
    private final GridGgfsMetaManager meta;

    /** Data manager. */
    private final GridGgfsDataManager data;

    /** Event manager. */
    private final GridEventStorageManager evts;

    /** Logger. */
    private final IgniteLogger log;

    /** Lock. */
    private final Lock lock = new ReentrantLock();

    /** Condition. */
    private final Condition cond = lock.newCondition();

    /** Force worker to perform actual delete. */
    private boolean force;

    /** Cancellation flag. */
    private volatile boolean cancelled;

    /** Message topic. */
    private Object topic;

    /**
     * Constructor.
     *
     * @param ggfsCtx GGFS context.
     */
    GridGgfsDeleteWorker(GridGgfsContext ggfsCtx) {
        super("ggfs-delete-worker%" + ggfsCtx.ggfs().name() + "%" + ggfsCtx.kernalContext().localNodeId() + "%");

        this.ggfsCtx = ggfsCtx;

        meta = ggfsCtx.meta();
        data = ggfsCtx.data();

        evts = ggfsCtx.kernalContext().event();

        String ggfsName = ggfsCtx.ggfs().name();

        topic = F.isEmpty(ggfsName) ? TOPIC_GGFS : TOPIC_GGFS.topic(ggfsName);

        assert meta != null;
        assert data != null;

        log = ggfsCtx.kernalContext().log(GridGgfsDeleteWorker.class);
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException {
        if (log.isDebugEnabled())
            log.debug("Delete worker started.");

        while (!cancelled) {
            lock.lock();

            try {
                if (!cancelled && !force)
                    cond.await(FREQUENCY, TimeUnit.MILLISECONDS);

                force = false; // Reset force flag.
            }
            finally {
                lock.unlock();
            }

            if (!cancelled)
                delete();
        }
    }

    /**
     * Notify the worker that new entry to delete appeared.
     */
    void signal() {
        lock.lock();

        try {
            force = true;

            cond.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    void cancel() {
        cancelled = true;

        interrupt();
    }

    /**
     * Perform cleanup of the trash directory.
     */
    private void delete() {
        GridGgfsFileInfo info = null;

        try {
            info = meta.info(TRASH_ID);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Cannot obtain trash directory info.", e);
        }

        if (info != null) {
            for (Map.Entry<String, GridGgfsListingEntry> entry : info.listing().entrySet()) {
                IgniteUuid fileId = entry.getValue().fileId();

                if (log.isDebugEnabled())
                    log.debug("Deleting GGFS trash entry [name=" + entry.getKey() + ", fileId=" + fileId + ']');

                try {
                    if (!cancelled) {
                        if (delete(entry.getKey(), fileId)) {
                            if (log.isDebugEnabled())
                                log.debug("Sending delete confirmation message [name=" + entry.getKey() +
                                    ", fileId=" + fileId + ']');

                            sendDeleteMessage(new GridGgfsDeleteMessage(fileId));
                        }
                    }
                    else
                        break;
                }
                catch (GridInterruptedException ignored) {
                    // Ignore this exception while stopping.
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to delete entry from the trash directory: " + entry.getKey(), e);

                    sendDeleteMessage(new GridGgfsDeleteMessage(fileId, e));
                }
            }
        }
    }

    /**
     * Remove particular entry from the TRASH directory.
     *
     * @param name Entry name.
     * @param id Entry ID.
     * @return {@code True} in case the entry really was deleted form the file system by this call.
     * @throws IgniteCheckedException If failed.
     */
    private boolean delete(String name, IgniteUuid id) throws IgniteCheckedException {
        assert name != null;
        assert id != null;

        while (true) {
            GridGgfsFileInfo info = meta.info(id);

            if (info != null) {
                if (info.isDirectory()) {
                    deleteDirectory(TRASH_ID, id);

                    if (meta.delete(TRASH_ID, name, id))
                        return true;
                }
                else {
                    assert info.isFile();

                    // Delete file content first.
                    // In case this node crashes, other node will re-delete the file.
                    data.delete(info).get();

                    boolean ret = meta.delete(TRASH_ID, name, id);

                    if (evts.isRecordable(EVT_GGFS_FILE_PURGED)) {
                        if (info.path() != null)
                            evts.record(new IgniteFsEvent(info.path(),
                                ggfsCtx.kernalContext().discovery().localNode(), EVT_GGFS_FILE_PURGED));
                        else
                            LT.warn(log, null, "Removing file without path info: " + info);
                    }

                    return ret;
                }
            }
            else
                return false; // Entry was deleted concurrently.
        }
    }

    /**
     * Remove particular entry from the trash directory or subdirectory.
     *
     * @param parentId Parent ID.
     * @param id Entry id.
     * @throws IgniteCheckedException If delete failed for some reason.
     */
    private void deleteDirectory(IgniteUuid parentId, IgniteUuid id) throws IgniteCheckedException {
        assert parentId != null;
        assert id != null;

        while (true) {
            GridGgfsFileInfo info = meta.info(id);

            if (info != null) {
                assert info.isDirectory();

                Map<String, GridGgfsListingEntry> listing = info.listing();

                if (listing.isEmpty())
                    return; // Directory is empty.

                Map<String, GridGgfsListingEntry> delListing;

                if (listing.size() <= MAX_DELETE_BATCH)
                    delListing = listing;
                else {
                    delListing = new HashMap<>(MAX_DELETE_BATCH, 1.0f);

                    int i = 0;

                    for (Map.Entry<String, GridGgfsListingEntry> entry : listing.entrySet()) {
                        delListing.put(entry.getKey(), entry.getValue());

                        if (++i == MAX_DELETE_BATCH)
                            break;
                    }
                }

                GridCompoundFuture<Object, ?> fut = new GridCompoundFuture<>(ggfsCtx.kernalContext());

                // Delegate to child folders.
                for (GridGgfsListingEntry entry : delListing.values()) {
                    if (!cancelled) {
                        if (entry.isDirectory())
                            deleteDirectory(id, entry.fileId());
                        else {
                            GridGgfsFileInfo fileInfo = meta.info(entry.fileId());

                            if (fileInfo != null) {
                                assert fileInfo.isFile();

                                fut.add(data.delete(fileInfo));
                            }
                        }
                    }
                    else
                        return;
                }

                fut.markInitialized();

                // Wait for data cache to delete values before clearing meta cache.
                try {
                    fut.get();
                }
                catch (IgniteFutureCancelledException ignore) {
                    // This future can be cancelled only due to GGFS shutdown.
                    cancelled = true;

                    return;
                }

                // Actual delete of folder content.
                Collection<IgniteUuid> delIds = meta.delete(id, delListing);

                if (delListing == listing && delListing.size() == delIds.size())
                    break; // All entries were deleted.
            }
            else
                break; // Entry was deleted concurrently.
        }
    }

    /**
     * Send delete message to all meta cache nodes in the grid.
     *
     * @param msg Message to send.
     */
    private void sendDeleteMessage(GridGgfsDeleteMessage msg) {
        assert msg != null;

        Collection<ClusterNode> nodes = meta.metaCacheNodes();

        boolean first = true;

        for (ClusterNode node : nodes) {
            GridGgfsCommunicationMessage msg0 = first ? msg : (GridGgfsCommunicationMessage)msg.clone();

            first = false;

            try {
                ggfsCtx.send(node, topic, msg0, GridIoPolicy.SYSTEM_POOL);
            }
            catch (IgniteCheckedException e) {
                U.warn(log, "Failed to send GGFS delete message to node [nodeId=" + node.id() +
                    ", msg=" + msg + ", err=" + e.getMessage() + ']');
            }
        }
    }
}
