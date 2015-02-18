package org.apache.ignite.internal.processors.cache.conflict;

import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;

/**
 * Conflict info with TTL.
 */
public class GridCacheTtlConflictInfo extends MessageAdapter implements GridCacheConflictInfo {
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
                if (!writer.writeLong("expireTime", expireTime))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("ttl", ttl))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeMessage("ver", ver))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        switch (readState) {
            case 0:
                expireTime = reader.readLong("expireTime");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 1:
                ttl = reader.readLong("ttl");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 2:
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
