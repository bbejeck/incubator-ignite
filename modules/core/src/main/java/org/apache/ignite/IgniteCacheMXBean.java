/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.management.*;

/**
 *
 */
public class IgniteCacheMXBean implements CacheMXBean {
    /** */
    private final Cache<?, ?> cache;

    /**
     * @param cache Cache.
     */
    public IgniteCacheMXBean(Cache<?, ?> cache) {
        this.cache = cache;
    }

    /** {@inheritDoc} */
    @Override public String getKeyType() {
        return cache.getConfiguration(CompleteConfiguration.class).getKeyType().getName();
    }

    /** {@inheritDoc} */
    @Override public String getValueType() {
        return cache.getConfiguration(CompleteConfiguration.class).getValueType().getName();
    }

    /** {@inheritDoc} */
    @Override public boolean isReadThrough() {
        return cache.getConfiguration(CompleteConfiguration.class).isReadThrough();
    }

    /** {@inheritDoc} */
    @Override public boolean isWriteThrough() {
        return cache.getConfiguration(CompleteConfiguration.class).isWriteThrough();
    }

    /** {@inheritDoc} */
    @Override public boolean isStoreByValue() {
        return cache.getConfiguration(CompleteConfiguration.class).isStoreByValue();
    }

    /** {@inheritDoc} */
    @Override public boolean isStatisticsEnabled() {
        return cache.getConfiguration(CompleteConfiguration.class).isStatisticsEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isManagementEnabled() {
        return cache.getConfiguration(CompleteConfiguration.class).isManagementEnabled();
    }
}