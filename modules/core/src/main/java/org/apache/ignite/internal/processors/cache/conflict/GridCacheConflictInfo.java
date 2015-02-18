package org.apache.ignite.internal.processors.cache.conflict;

import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;

/**
 * Cache conflict info which is passed over the wire.
 */
public abstract class GridCacheConflictInfo implements Externalizable {
    /**
     * Create conflict info.
     *
     * @param ver Version.
     * @param ttl TTL.
     * @param expireTime Expire time.
     * @return Conflict info.
     */
    public static GridCacheConflictInfo create(GridCacheVersion ver, long ttl, long expireTime) {
        if (ttl == CU.TTL_NOT_CHANGED) {
            assert expireTime == CU.EXPIRE_TIME_CALCULATE;

            return new GridCacheNoTtlConflictInfo(ver);
        }
        else {
            assert ttl != CU.TTL_ZERO && ttl >= 0;
            assert expireTime != CU.EXPIRE_TIME_CALCULATE && expireTime >= 0;

            return new GridCacheTtlConflictInfo(ver, ttl, expireTime);
        }
    }

    /**
     * @return Version.
     */
    public abstract GridCacheVersion version();

    /**
     * @return TTL.
     */
    public abstract long ttl();

    /**
     * @return Expire time.
     */
    public abstract long expireTime();

    /**
     * @return {@code True} if has expiration info.
     */
    public abstract boolean hasExpirationInfo();
}
