package org.apache.ignite.internal.processors.cache.conflict;

import org.apache.ignite.internal.processors.cache.version.*;

import java.io.*;

/**
 * Cache conflict info which is passed over the wire.
 */
public interface GridCacheConflictInfo {
    /**
     * @return Version.
     */
    public GridCacheVersion version();

    /**
     * @return TTL.
     */
    public long ttl();

    /**
     * @return Expire time.
     */
    public long expireTime();

    /**
     * @return {@code True} if has expiration info.
     */
    public boolean hasExpirationInfo();
}
