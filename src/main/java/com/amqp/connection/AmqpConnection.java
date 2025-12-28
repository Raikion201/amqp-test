package com.amqp.connection;

import com.amqp.amqp.AmqpFrame;
import com.amqp.amqp.AmqpMethod;
import com.amqp.server.AmqpBroker;
import com.amqp.security.User;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class AmqpConnection {
    private static final Logger logger = LoggerFactory.getLogger(AmqpConnection.class);

    // Protocol compliance constants
    public static final int DEFAULT_CHANNEL_MAX = 2047;  // Max channels per connection
    public static final int DEFAULT_FRAME_MAX = 131072;   // 128KB default frame max
    public static final int MIN_FRAME_MAX = 4096;         // Minimum frame size per spec

    private final Channel nettyChannel;
    private final AmqpBroker broker;
    private final ConcurrentMap<Short, AmqpChannel> channels;
    private final AtomicBoolean connected;
    private final AtomicBoolean authenticated;
    private volatile String virtualHost = "/";
    private volatile String username;
    private volatile User user;

    // Protocol negotiation values
    private volatile int channelMax = DEFAULT_CHANNEL_MAX;
    private volatile int frameMax = DEFAULT_FRAME_MAX;

    // Heartbeat tracking
    private volatile int heartbeatInterval = 0; // seconds, 0 = disabled
    private final AtomicLong lastHeartbeatReceived = new AtomicLong(System.currentTimeMillis());
    private volatile ScheduledFuture<?> heartbeatSender;
    private volatile ScheduledFuture<?> heartbeatMonitor;
    
    public AmqpConnection(Channel nettyChannel, AmqpBroker broker) {
        this.nettyChannel = nettyChannel;
        this.broker = broker;
        this.channels = new ConcurrentHashMap<>();
        this.connected = new AtomicBoolean(false);
        this.authenticated = new AtomicBoolean(false);

        AmqpChannel mainChannel = new AmqpChannel((short) 0, this, broker);
        channels.put((short) 0, mainChannel);
    }
    
    public void handleFrame(AmqpFrame frame) {
        short channelNumber = frame.getChannel();
        AmqpChannel channel = channels.get(channelNumber);

        if (channel == null) {
            // Check if this is a Channel.Open method (class=20, method=10)
            // In AMQP, Channel.Open is sent on the channel being opened
            if (frame.getType() == AmqpFrame.FrameType.METHOD.getValue() && channelNumber > 0) {
                ByteBuf payload = frame.getPayload();
                payload.markReaderIndex();
                if (payload.readableBytes() >= 4) {
                    short classId = payload.readShort();
                    short methodId = payload.readShort();
                    payload.resetReaderIndex();

                    // Channel.Open = class 20, method 10
                    if (classId == 20 && methodId == 10) {
                        logger.debug("Creating new channel: {}", channelNumber);
                        channel = openChannel(channelNumber);
                    }
                }
            }

            if (channel == null) {
                logger.warn("Received frame for unknown channel: {}", channelNumber);
                return;
            }
        }

        try {
            channel.handleFrame(frame);
        } catch (Exception e) {
            logger.error("Error handling frame on channel {}", channelNumber, e);
            closeChannel(channelNumber);
        }
        // Note: Frame release is handled by the Netty pipeline/decoder, not here
    }
    
    public AmqpChannel openChannel(short channelNumber) {
        // Protocol compliance: Enforce channel-max limit
        if (channelNumber > channelMax) {
            throw new IllegalArgumentException(
                "Channel number " + channelNumber + " exceeds channel-max limit of " + channelMax);
        }

        // Check if we've hit the channel count limit
        // Note: channel 0 is reserved for connection-level operations
        int currentChannelCount = channels.size();
        if (currentChannelCount >= channelMax) {
            throw new IllegalStateException(
                "Maximum channel count reached: " + channelMax + " channels already open");
        }

        if (channels.containsKey(channelNumber)) {
            throw new IllegalArgumentException("Channel already exists: " + channelNumber);
        }

        AmqpChannel channel = new AmqpChannel(channelNumber, this, broker);
        channels.put(channelNumber, channel);

        logger.debug("Opened channel: {} (total: {}/{})", channelNumber, channels.size(), channelMax);
        return channel;
    }
    
    public void closeChannel(short channelNumber) {
        AmqpChannel channel = channels.remove(channelNumber);
        if (channel != null) {
            channel.close();
            logger.debug("Closed channel: {}", channelNumber);
        }
    }
    
    public void sendFrame(AmqpFrame frame) {
        if (nettyChannel.isActive()) {
            nettyChannel.writeAndFlush(frame);
            logger.debug("Frame sent: type={}, channel={}, size={}", frame.getType(), frame.getChannel(), frame.getSize());
        } else {
            // Release the frame's ByteBuf if we can't send it
            frame.release();
            logger.warn("Cannot send frame - netty channel not active: type={}, channel={}, size={}", frame.getType(), frame.getChannel(), frame.getSize());
        }
    }
    
    public void sendMethod(short channelNumber, short classId, short methodId, ByteBuf payload) {
        ByteBuf methodFrame = Unpooled.buffer();
        try {
            methodFrame.writeShort(classId);
            methodFrame.writeShort(methodId);
            if (payload != null) {
                methodFrame.writeBytes(payload);
                payload.release(); // Release input buffer after copying
            }

            AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.METHOD.getValue(), channelNumber, methodFrame);
            sendFrame(frame);
        } catch (Exception e) {
            methodFrame.release(); // Release on error
            throw e;
        }
    }

    public void sendContentHeader(short channelNumber, short classId, long bodySize, ByteBuf properties) {
        ByteBuf headerFrame = Unpooled.buffer();
        try {
            headerFrame.writeShort(classId);
            headerFrame.writeShort(0);
            headerFrame.writeLong(bodySize);
            if (properties != null) {
                headerFrame.writeBytes(properties);
                properties.release(); // Release input buffer after copying
            }

            AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.HEADER.getValue(), channelNumber, headerFrame);
            sendFrame(frame);
        } catch (Exception e) {
            headerFrame.release(); // Release on error
            throw e;
        }
    }
    
    public void sendContentBody(short channelNumber, ByteBuf body) {
        AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.BODY.getValue(), channelNumber, body);
        sendFrame(frame);
    }
    
    public void close() {
        connected.set(false);

        // Stop heartbeat monitoring and sending
        stopHeartbeat();

        for (AmqpChannel channel : channels.values()) {
            channel.close();
        }
        channels.clear();

        if (nettyChannel.isActive()) {
            nettyChannel.close();
        }

        logger.info("Connection closed");
    }
    
    public boolean isConnected() {
        return connected.get() && nettyChannel.isActive();
    }
    
    public void setConnected(boolean connected) {
        this.connected.set(connected);
    }
    
    public String getVirtualHost() {
        return virtualHost;
    }
    
    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }
    
    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
        if (user != null) {
            this.username = user.getUsername();
            this.authenticated.set(true);
        }
    }

    public boolean isAuthenticated() {
        return authenticated.get();
    }

    public AmqpBroker getBroker() {
        return broker;
    }

    public AmqpChannel getChannel(short channelNumber) {
        return channels.get(channelNumber);
    }

    /**
     * Start heartbeat monitoring and sending based on negotiated interval.
     * @param intervalSeconds Heartbeat interval in seconds (0 = disabled)
     * @param scheduler ScheduledExecutorService for heartbeat tasks
     */
    public void startHeartbeat(int intervalSeconds, ScheduledExecutorService scheduler) {
        if (intervalSeconds <= 0) {
            logger.debug("Heartbeat disabled (interval=0)");
            return;
        }

        this.heartbeatInterval = intervalSeconds;
        lastHeartbeatReceived.set(System.currentTimeMillis());

        // Send heartbeat frames at the negotiated interval
        heartbeatSender = scheduler.scheduleAtFixedRate(() -> {
            try {
                if (isConnected()) {
                    sendHeartbeat();
                }
            } catch (Exception e) {
                logger.error("Error sending heartbeat", e);
            }
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);

        // Monitor for missed heartbeats (close if no heartbeat received in 2x interval)
        heartbeatMonitor = scheduler.scheduleAtFixedRate(() -> {
            try {
                if (isConnected()) {
                    long timeSinceLastHeartbeat = System.currentTimeMillis() - lastHeartbeatReceived.get();
                    long timeoutMs = intervalSeconds * 2000L; // 2x interval

                    if (timeSinceLastHeartbeat > timeoutMs) {
                        logger.warn("Connection heartbeat timeout: no heartbeat received for {}ms (limit: {}ms)",
                                  timeSinceLastHeartbeat, timeoutMs);
                        close();
                    }
                }
            } catch (Exception e) {
                logger.error("Error monitoring heartbeat", e);
            }
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);

        logger.info("Heartbeat started: interval={}s", intervalSeconds);
    }

    /**
     * Stop heartbeat monitoring and sending.
     */
    public void stopHeartbeat() {
        if (heartbeatSender != null) {
            heartbeatSender.cancel(false);
            heartbeatSender = null;
        }
        if (heartbeatMonitor != null) {
            heartbeatMonitor.cancel(false);
            heartbeatMonitor = null;
        }
        logger.debug("Heartbeat stopped");
    }

    /**
     * Send a heartbeat frame to the client.
     */
    private void sendHeartbeat() {
        ByteBuf emptyPayload = Unpooled.EMPTY_BUFFER;
        AmqpFrame heartbeatFrame = new AmqpFrame(AmqpFrame.FrameType.HEARTBEAT.getValue(), (short) 0, emptyPayload);
        sendFrame(heartbeatFrame);
        logger.trace("Sent heartbeat frame");
    }

    /**
     * Record that a heartbeat was received from the client.
     */
    public void recordHeartbeatReceived() {
        lastHeartbeatReceived.set(System.currentTimeMillis());
        logger.trace("Received heartbeat from client");
    }

    /**
     * Get the negotiated channel-max value.
     */
    public int getChannelMax() {
        return channelMax;
    }

    /**
     * Set the negotiated channel-max value.
     * Called during Connection.Tune-Ok handling.
     */
    public void setChannelMax(int channelMax) {
        // Use server's value if client sends 0 (meaning "use server's value")
        if (channelMax == 0) {
            this.channelMax = DEFAULT_CHANNEL_MAX;
        } else {
            // Use the minimum of client and server values per AMQP spec
            this.channelMax = Math.min(channelMax, DEFAULT_CHANNEL_MAX);
        }
        logger.debug("Channel-max negotiated to: {}", this.channelMax);
    }

    /**
     * Get the negotiated frame-max value.
     */
    public int getFrameMax() {
        return frameMax;
    }

    /**
     * Set the negotiated frame-max value.
     * Called during Connection.Tune-Ok handling.
     */
    public void setFrameMax(int frameMax) {
        // Use server's value if client sends 0 (meaning "unlimited" or "use server's value")
        if (frameMax == 0) {
            this.frameMax = DEFAULT_FRAME_MAX;
        } else if (frameMax < MIN_FRAME_MAX) {
            throw new IllegalArgumentException(
                "Frame-max " + frameMax + " is below minimum required: " + MIN_FRAME_MAX);
        } else {
            // Use the minimum of client and server values per AMQP spec
            this.frameMax = Math.min(frameMax, DEFAULT_FRAME_MAX);
        }
        logger.debug("Frame-max negotiated to: {}", this.frameMax);
    }

    /**
     * Get the current number of open channels (excluding channel 0).
     */
    public int getOpenChannelCount() {
        // Channel 0 is always present but is for connection-level operations
        return Math.max(0, channels.size() - 1);
    }
}