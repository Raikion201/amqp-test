package com.amqp.protocol.v10.connection;

import com.amqp.protocol.v10.transport.*;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AMQP 1.0 Connection state machine.
 *
 * Manages the connection lifecycle, sessions, and connection-level configuration.
 */
public class Amqp10Connection {

    private static final Logger log = LoggerFactory.getLogger(Amqp10Connection.class);

    // Default connection parameters
    public static final int DEFAULT_MAX_FRAME_SIZE = 16384;
    public static final int DEFAULT_CHANNEL_MAX = 255;
    public static final int DEFAULT_IDLE_TIMEOUT = 60000; // ms

    private final Channel channel;
    private final String containerId;

    private volatile ConnectionState state = ConnectionState.START;

    // Connection parameters
    private int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;
    private int channelMax = DEFAULT_CHANNEL_MAX;
    private int idleTimeout = DEFAULT_IDLE_TIMEOUT;
    private String remoteContainerId;
    private String hostname;

    // Sessions indexed by local channel number
    private final Map<Integer, Amqp10Session> sessions = new ConcurrentHashMap<>();
    private final AtomicInteger nextChannelId = new AtomicInteger(0);

    // Error tracking
    private ErrorCondition error;

    // Authentication info
    private String authenticatedUser;
    private boolean authenticated = false;

    public Amqp10Connection(Channel channel, String containerId) {
        this.channel = channel;
        this.containerId = containerId;
    }

    // State management
    public ConnectionState getState() {
        return state;
    }

    public void setState(ConnectionState state) {
        log.debug("Connection {} state change: {} -> {}", containerId, this.state, state);
        this.state = state;
    }

    public boolean isOpen() {
        return state == ConnectionState.OPENED;
    }

    public boolean isClosed() {
        return state == ConnectionState.END || state == ConnectionState.ERROR;
    }

    // Header exchange
    public void onHeaderReceived() {
        switch (state) {
            case START:
                setState(ConnectionState.HDR_RCVD);
                break;
            case HDR_SENT:
                setState(ConnectionState.HDR_EXCH);
                break;
            default:
                log.warn("Unexpected header in state {}", state);
        }
    }

    public void onHeaderSent() {
        switch (state) {
            case START:
                setState(ConnectionState.HDR_SENT);
                break;
            case HDR_RCVD:
                setState(ConnectionState.HDR_EXCH);
                break;
            default:
                log.warn("Unexpected header sent in state {}", state);
        }
    }

    // Open handling
    public void onOpenReceived(Open open) {
        this.remoteContainerId = open.getContainerId();

        // Negotiate parameters
        if (open.getMaxFrameSize() > 0 && open.getMaxFrameSize() < maxFrameSize) {
            this.maxFrameSize = (int) open.getMaxFrameSize();
        }
        if (open.getChannelMax() > 0 && open.getChannelMax() < channelMax) {
            this.channelMax = open.getChannelMax();
        }
        if (open.getIdleTimeout() > 0) {
            int remoteTimeout = (int) open.getIdleTimeout();
            // Use the smaller of the two timeouts, but not zero
            if (idleTimeout == 0 || remoteTimeout < idleTimeout) {
                this.idleTimeout = remoteTimeout;
            }
        }

        switch (state) {
            case HDR_EXCH:
            case HDR_RCVD:
                setState(ConnectionState.OPEN_RCVD);
                break;
            case OPEN_SENT:
                setState(ConnectionState.OPENED);
                break;
            default:
                log.warn("Unexpected Open in state {}", state);
        }
    }

    public void onOpenSent() {
        switch (state) {
            case HDR_EXCH:
            case HDR_SENT:
                setState(ConnectionState.OPEN_SENT);
                break;
            case OPEN_RCVD:
                setState(ConnectionState.OPENED);
                break;
            default:
                log.warn("Unexpected Open sent in state {}", state);
        }
    }

    // Close handling
    public void onCloseReceived(Close close) {
        if (close.getError() != null) {
            this.error = close.getError();
            log.warn("Connection closed by peer with error: {}", error);
        }

        switch (state) {
            case OPENED:
                setState(ConnectionState.CLOSE_RCVD);
                break;
            case CLOSE_SENT:
                setState(ConnectionState.END);
                break;
            default:
                setState(ConnectionState.END);
        }
    }

    public void onCloseSent() {
        switch (state) {
            case OPENED:
                setState(ConnectionState.CLOSE_SENT);
                break;
            case CLOSE_RCVD:
                setState(ConnectionState.END);
                break;
            default:
                setState(ConnectionState.END);
        }
    }

    // Session management
    public Amqp10Session createSession() {
        int channelId = nextChannelId.getAndIncrement();
        if (channelId > channelMax) {
            throw new IllegalStateException("Channel max exceeded");
        }

        Amqp10Session session = new Amqp10Session(this, channelId);
        sessions.put(channelId, session);
        return session;
    }

    public Amqp10Session getSession(int channel) {
        return sessions.get(channel);
    }

    public void removeSession(int channel) {
        sessions.remove(channel);
    }

    public Map<Integer, Amqp10Session> getSessions() {
        return sessions;
    }

    public void closeAllSessions() {
        for (Amqp10Session session : sessions.values()) {
            session.close(null);
        }
        sessions.clear();
    }

    // Create performatives
    public Open createOpen() {
        Open open = new Open(containerId);
        open.setMaxFrameSize((long) maxFrameSize);
        open.setChannelMax(channelMax);
        if (idleTimeout > 0) {
            open.setIdleTimeout((long) idleTimeout);
        }
        if (hostname != null) {
            open.setHostname(hostname);
        }
        return open;
    }

    public Close createClose() {
        return error != null ? new Close(error) : new Close();
    }

    public Close createClose(ErrorCondition error) {
        this.error = error;
        return new Close(error);
    }

    // Getters
    public Channel getChannel() {
        return channel;
    }

    public String getContainerId() {
        return containerId;
    }

    public String getRemoteContainerId() {
        return remoteContainerId;
    }

    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    public int getChannelMax() {
        return channelMax;
    }

    public int getIdleTimeout() {
        return idleTimeout;
    }

    public String getHostname() {
        return hostname;
    }

    public ErrorCondition getError() {
        return error;
    }

    public String getAuthenticatedUser() {
        return authenticatedUser;
    }

    public boolean isAuthenticated() {
        return authenticated;
    }

    // Setters
    public void setMaxFrameSize(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    public void setChannelMax(int channelMax) {
        this.channelMax = channelMax;
    }

    public void setIdleTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public void setError(ErrorCondition error) {
        this.error = error;
        this.state = ConnectionState.ERROR;
    }

    public void setAuthenticatedUser(String user) {
        this.authenticatedUser = user;
        this.authenticated = user != null;
    }

    @Override
    public String toString() {
        return String.format("Amqp10Connection{containerId='%s', remoteContainerId='%s', state=%s, sessions=%d}",
                containerId, remoteContainerId, state, sessions.size());
    }
}
