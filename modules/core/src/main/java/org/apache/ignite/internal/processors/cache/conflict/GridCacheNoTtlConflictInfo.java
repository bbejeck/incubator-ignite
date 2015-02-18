package org.apache.ignite.internal.processors.cache.conflict;

import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;

/**
 * Conflict info without TTL.
 */
public class GridCacheNoTtlConflictInfo extends GridCacheConflictInfo {
    /** Version. */
    private GridCacheVersion ver;

    /**
     * {@link Externalizable} support.
     */
    public GridCacheNoTtlConflictInfo() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param ver Version.
     */
    public GridCacheNoTtlConflictInfo(GridCacheVersion ver) {
        this.ver = ver;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public long ttl() {
        return CU.TTL_NOT_CHANGED;
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return CU.EXPIRE_TIME_CALCULATE;
    }

    /** {@inheritDoc} */
    @Override public boolean hasExpirationInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ver);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ver = (GridCacheVersion)in.readObject();
    }
}
