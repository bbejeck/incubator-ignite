/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.discovery;

import java.lang.annotation.*;

/**
 * This annotation is for all implementations of {@link DiscoverySpi} that support
 * topology snapshots history.
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface DiscoverySpiHistorySupport {
    /**
     * Whether or not target SPI supports topology snapshots history.
     */
    @SuppressWarnings({"JavaDoc"})
    public boolean value();
}