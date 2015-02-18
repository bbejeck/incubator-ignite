package org.apache.ignite.internal.processors.cache.conflict;

import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.nio.*;

/**
 * Conflict info with TTL.
 */
public class GridCacheTtlConflictInfo extends GridCacheConflictInfo {
    /** Version. */
    private GridCacheVersion ver;

    /** TTL. */
    private long ttl;

    /** Expire time. */
    private long expireTime;

    /**
     * {@link Externalizable} support.
     */
    public GridCacheTtlConflictInfo() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param ver Version.
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    public GridCacheTtlConflictInfo(GridCacheVersion ver, long ttl, long expireTime) {
        assert ttl != CU.TTL_ZERO && ttl != CU.TTL_NOT_CHANGED;
        assert expireTime != CU.EXPIRE_TIME_CALCULATE;

        this.ver = ver;
        this.ttl = ttl;
        this.expireTime = expireTime;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public long ttl() {
        return ttl;
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return expireTime;
    }

    /** {@inheritDoc} */
    @Override public boolean hasExpirationInfo() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ver);
        out.writeLong(ttl);
        out.writeLong(expireTime);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ver = (GridCacheVersion)in.readObject();
        ttl = in.readLong();
        expireTime = in.readLong();
    }
}
