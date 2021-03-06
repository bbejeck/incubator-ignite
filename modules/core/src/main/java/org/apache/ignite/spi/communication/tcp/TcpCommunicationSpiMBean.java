/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.mbean.*;
import org.apache.ignite.spi.*;

/**
 * MBean provide access to TCP-based communication SPI.
 */
@IgniteMBeanDescription("MBean provide access to TCP-based communication SPI.")
public interface TcpCommunicationSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Gets local host address for socket binding.
     * Beside loopback address physical node could have
     * several other ones, but only one is assigned to grid node.
     *
     * @return Grid node IP address.
     */
    @IgniteMBeanDescription("Grid node IP address.")
    public String getLocalAddress();

    /**
     * Gets local port for socket binding.
     *
     * @return Port number.
     */
    @IgniteMBeanDescription("Port number.")
    public int getLocalPort();

    /**
     * Gets local port for shared memory communication.
     *
     * @return Port number.
     */
    @IgniteMBeanDescription("Shared memory endpoint port number.")
    public int getSharedMemoryPort();

    /**
     * Gets maximum number of local ports tried if all previously
     * tried ports are occupied.
     *
     * @return Local port range.
     */
    @IgniteMBeanDescription("Local port range.")
    public int getLocalPortRange();

    /**
     * Gets maximum idle connection time upon which idle connections
     * will be closed.
     *
     * @return Maximum idle connection time.
     */
    @IgniteMBeanDescription("Maximum idle connection time.")
    public long getIdleConnectionTimeout();

    /**
     * Gets flag that indicates whether direct or heap allocated buffer is used.
     *
     * @return Flag that indicates whether direct or heap allocated buffer is used.
     */
    @IgniteMBeanDescription("Flag that indicates whether direct or heap allocated buffer is used.")
    public boolean isDirectBuffer();

    /**
     * Gets count of selectors used in TCP server. Default value equals to the
     * number of CPUs available in the system.
     *
     * @return Count of selectors in TCP server.
     */
    @IgniteMBeanDescription("Count of selectors used in TCP server.")
    public int getSelectorsCount();

    /**
     * Gets sent messages count.
     *
     * @return Sent messages count.
     */
    @IgniteMBeanDescription("Sent messages count.")
    public int getSentMessagesCount();

    /**
     * Gets sent bytes count.
     *
     * @return Sent bytes count.
     */
    @IgniteMBeanDescription("Sent bytes count.")
    public long getSentBytesCount();

    /**
     * Gets received messages count.
     *
     * @return Received messages count.
     */
    @IgniteMBeanDescription("Received messages count.")
    public int getReceivedMessagesCount();

    /**
     * Gets received bytes count.
     *
     * @return Received bytes count.
     */
    @IgniteMBeanDescription("Received bytes count.")
    public long getReceivedBytesCount();

    /**
     * Gets outbound messages queue size.
     *
     * @return Outbound messages queue size.
     */
    @IgniteMBeanDescription("Outbound messages queue size.")
    public int getOutboundMessagesQueueSize();

    /**
     * Gets connect timeout used when establishing connection
     * with remote nodes.
     *
     * @return Connect timeout.
     */
    @IgniteMBeanDescription("Connect timeout.")
    public long getConnectTimeout();

    /**
     * Gets maximum connect timeout.
     *
     * @return Maximum connect timeout.
     */
    @IgniteMBeanDescription("Maximum connect timeout.")
    public long getMaxConnectTimeout();

    /**
     * Gets maximum number of reconnect attempts used when establishing connection
     * with remote nodes.
     *
     * @return Reconnects count.
     */
    @IgniteMBeanDescription("Reconnect count on connection failure.")
    public int getReconnectCount();

    /**
     * Gets value for {@code TCP_NODELAY} socket option.
     *
     * @return {@code True} if TCP delay is disabled.
     */
    @IgniteMBeanDescription("TCP_NODELAY socket option value.")
    public boolean isTcpNoDelay();

    /**
     * Gets connection buffer flush frequency.
     * <p>
     * Client connections to other nodes in topology use buffered output.
     * This frequency defines how often system will advice to flush
     * connection buffer.
     *
     * @return Flush frequency.
     */
    @IgniteMBeanDescription("Connection buffer flush frequency.")
    public long getConnectionBufferFlushFrequency();

    /**
     * Sets connection buffer flush frequency.
     * <p>
     * Client connections to other nodes in topology use buffered output.
     * This frequency defines how often system will advice to flush
     * connection buffer.
     * <p>
     * If not provided, default value is {@link TcpCommunicationSpi#DFLT_CONN_BUF_FLUSH_FREQ}.
     * <p>
     * This property is used only if {@link #getConnectionBufferSize()} is greater than {@code 0}.
     *
     * @param connBufFlushFreq Flush frequency.
     * @see #getConnectionBufferSize()
     */
    @IgniteMBeanDescription("Sets connection buffer flush frequency.")
    public void setConnectionBufferFlushFrequency(long connBufFlushFreq);

    /**
     * Gets connection buffer size.
     * <p>
     * If set to {@code 0} connection buffer is disabled.
     *
     * @return Connection buffer size.
     */
    @IgniteMBeanDescription("Connection buffer size.")
    public int getConnectionBufferSize();

    /**
     * Gets flag defining whether direct send buffer should be used.
     *
     * @return {@code True} if direct buffers should be used.
     */
    @IgniteMBeanDescription("Direct send buffer.")
    public boolean isDirectSendBuffer();

    /**
     * Gets receive buffer size for sockets created or accepted by this SPI.
     * <p>
     * If not provided, default is {@link TcpCommunicationSpi#DFLT_SOCK_BUF_SIZE}.
     *
     * @return Socket receive buffer size.
     */
    @IgniteMBeanDescription("Socket receive buffer.")
    public int getSocketReceiveBuffer();

    /**
     * Gets send buffer size for sockets created or accepted by this SPI.
     * <p>
     * If not provided, default is {@link TcpCommunicationSpi#DFLT_SOCK_BUF_SIZE}.
     *
     * @return Socket send buffer size.
     */
    @IgniteMBeanDescription("Socket send buffer.")
    public int getSocketSendBuffer();

    /**
     * Gets message queue limit for incoming and outgoing messages.
     *
     * @return Send queue size limit.
     */
    @IgniteMBeanDescription("Message queue size limit.")
    public int getMessageQueueLimit();

    /**
     * Gets the minimum number of messages for this SPI, that are buffered
     * prior to sending.
     *
     * @return Minimum buffered message count.
     */
    @IgniteMBeanDescription("Minimum buffered message count.")
    public int getMinimumBufferedMessageCount();

    /**
     * Gets the buffer size ratio for this SPI. As messages are sent,
     * the buffer size is adjusted using this ratio.
     *
     * @return Buffer size ratio.
     */
    @IgniteMBeanDescription("Buffer size ratio.")
    public double getBufferSizeRatio();

    /**
     * Gets socket write timeout for TCP connections. If message can not be written to
     * socket within this time then connection is closed and reconnect is attempted.
     *
     * @return Socket write timeout for TCP connections.
     */
    @IgniteMBeanDescription("Socket write timeout.")
    public long getSocketWriteTimeout();

    /**
     * Gets number of received messages per connection to node after which acknowledgment message is sent.
     *
     * @return Number of received messages after which acknowledgment is sent.
     */
    @IgniteMBeanDescription("Number of received messages after which acknowledgment is sent.")
    public int getAckSendThreshold();

    /**
     * Gets maximum number of stored unacknowledged messages per connection to node.
     * If number of unacknowledged messages exceeds this number then connection to node is
     * closed and reconnect is attempted.
     *
     * @return Maximum number of unacknowledged messages.
     */
    @IgniteMBeanDescription("Maximum number of unacknowledged messages.")
    public int getUnacknowledgedMessagesBufferSize();
}
