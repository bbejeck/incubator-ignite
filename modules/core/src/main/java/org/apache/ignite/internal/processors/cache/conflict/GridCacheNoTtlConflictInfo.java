package org.apache.ignite.internal.processors.cache.conflict;

import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;
import sun.plugin2.message.*;

import java.io.*;
import java.nio.*;

/**
 * Conflict info without TTL.
 */
public class GridCacheNoTtlConflictInfo extends MessageAdapter implements GridCacheConflictInfo {
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

    @Override
    public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isTypeWritten()) {
            if (!writer.writeByte(null, directType()))
                return false;

            writer.onTypeWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeMessage("ver", ver))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    @Override
    public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        switch (readState) {
            case 0:
                ver = reader.readMessage("ver");

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    @Override
    public byte directType() {
        return 0;
    }
}
