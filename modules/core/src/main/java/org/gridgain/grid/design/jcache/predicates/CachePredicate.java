// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.jcache.predicates;

import org.gridgain.grid.lang.*;

import javax.cache.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface CachePredicate<K, V> extends GridPredicate<Cache.Entry<K, V>> {
    // No-op.
}