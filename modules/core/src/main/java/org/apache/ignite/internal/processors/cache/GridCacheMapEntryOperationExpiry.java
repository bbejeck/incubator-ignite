package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;

import javax.cache.expiry.*;

/**
 *
 */
public class GridCacheMapEntryOperationExpiry {
    /** */
    private final IgniteCacheExpiryPolicy expiry;

    /** */
    private final long explicitTtl;

    /** */
    private final long explicitExpireTime;

    /**
     *
     * @param expiry
     * @param explicitTtl
     * @param explicitExpireTime
     */
    public GridCacheMapEntryOperationExpiry(IgniteCacheExpiryPolicy expiry, long explicitTtl, long explicitExpireTime) {
        this.expiry = expiry;
        this.explicitTtl = explicitTtl;
        this.explicitExpireTime = explicitExpireTime;
    }

    /**
     *
     * @return
     */
    public boolean hasExpiry() {
        return expiry != null;
    }

    /**
     *
     * @return
     */
    public IgniteCacheExpiryPolicy expiry() {
        return expiry;
    }

    /**
     *
     * @return
     */
    public boolean hasExplicitTtl() {
        return explicitTtl != CU.TTL_NOT_CHANGED;
    }

    /**
     *
     * @return
     */
    public long explicitTtl() {
        return explicitTtl;
    }

    /**
     *
     * @return
     */
    public boolean hasExplicitExpireTime() {
        return explicitExpireTime != CU.EXPIRE_TIME_CALCULATE;
    }

    /**
     *
     * @return
     */
    public long explicitExpireTime() {
        return explicitExpireTime;
    }

    /**
     * @return
     */
    public IgniteBiTuple<Long, Long> initialTtlAndExpireTime() {
        assert hasExpiry();

        long initTtl = expiry().forCreate();
        long initExpireTime;

        if (initTtl == CU.TTL_ZERO) {
            initTtl = CU.TTL_MINIMUM;
            initExpireTime = CU.expireTimeInPast();
        }
        else if (initTtl == CU.TTL_NOT_CHANGED) {
            initTtl = CU.TTL_ETERNAL;
            initExpireTime = CU.EXPIRE_TIME_ETERNAL;
        }
        else
            initExpireTime = CU.toExpireTime(initTtl);

        return F.t(initTtl, initExpireTime);
    }

    /**
     *
     * @param entry
     * @return
     */
    public GridTuple3<Long, Long, Boolean> ttlAndExpireTime(GridCacheMapEntry entry) {
        long ttl;
        long expireTime;
        boolean rmv;

        if (hasExplicitTtl()) {
            // TTL is set explicitly.
            assert explicitTtl != CU.TTL_NOT_CHANGED && explicitTtl != CU.TTL_MINIMUM && explicitTtl >= 0L;

            ttl = explicitTtl;
            expireTime = hasExplicitExpireTime() ? explicitExpireTime : CU.toExpireTime(explicitTtl);
            rmv = false;
        }
        else {
            // Need to calculate TTL.
            if (hasExpiry()) {
                // Expiry exists.
                long sysTtl = entry.hasValueUnlocked() ? expiry.forUpdate() : expiry.forCreate();

                if (sysTtl == CU.TTL_ZERO) {
                    // Entry must be expired immediately.
                    ttl = CU.TTL_MINIMUM;
                    expireTime = CU.expireTimeInPast();
                    rmv = true;
                }
                else if (sysTtl == CU.TTL_NOT_CHANGED) {
                    // TTL is not changed.
                    ttl = entry.ttlExtras();
                    expireTime = CU.toExpireTime(ttl);
                    rmv = false;
                }
                else {
                    // TTL is changed.
                    assert sysTtl >= 0;

                    ttl = sysTtl;
                    expireTime = CU.toExpireTime(ttl);
                    rmv = false;
                }
            }
            else {
                // No expiry, entry is immortal.
                ttl = CU.TTL_ETERNAL;
                expireTime = CU.EXPIRE_TIME_ETERNAL;
                rmv = false;
            }
        }

        return F.t(ttl, expireTime, rmv);
    }
}
