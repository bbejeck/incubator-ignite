package org.apache.ignite.internal.processors.cache.conflict;

import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

/**
 * Conflict inner update info.
 */
public class GridCacheConflictInnerUpdate {
    /** Resolve flag. */
    private final boolean resolve;

    /** Version. */
    private GridCacheVersion ver;

    /** TTL. */
    private final long ttl;

    /** Expire time. */
    private final long expireTime;

    /**
     * Conflict inner update info.
     *
     * @param resolve Resolve flag.
     * @param ver Version.
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    public GridCacheConflictInnerUpdate(boolean resolve, GridCacheVersion ver, long ttl, long expireTime) {
        // TODO: IGNITE-283: Add assertion for invariants.

        this.resolve = resolve;
        this.ver = ver;
        this.ttl = ttl;
        this.expireTime = expireTime;
    }

    /**
     * @return Resolve flag.
     */
    public boolean resolve() {
        return resolve;
    }

    /**
     * @return Version.
     */
    @Nullable public GridCacheVersion version() {
        return ver;
    }

    /**
     * Clear version so that update will be considered local.
     */
    public void clearVersion() {
        ver = null;
    }

    /*
     * @return TTL.
     */
    public long ttl() {
        return ttl;
    }

    /**
     * @return {@code True} if explicit TTL is set.
     */
    public boolean hasExplicitTtl() {
        return ttl != CU.TTL_NOT_CHANGED;
    }

    /**
     * @return {@code True} if explicit expire time is set.
     */
    public boolean hasExplicitExpireTime() {
        return expireTime != CU.EXPIRE_TIME_CALCULATE;
    }

    /**
     * @return Expire time.
     */
    public long expireTime() {
        return expireTime;
    }
}
