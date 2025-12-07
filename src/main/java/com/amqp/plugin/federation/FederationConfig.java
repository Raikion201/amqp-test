package com.amqp.plugin.federation;

/**
 * Configuration for Federation links.
 */
public class FederationConfig {
    private int maxHops = 1;
    private long receiveTimeout = 5000; // milliseconds
    private long reconnectDelay = 5000; // milliseconds
    private int prefetchCount = 1000;
    private boolean ackMode = true; // Acknowledge messages
    private boolean exposeUri = false; // Expose upstream URI in headers
    private String trustUserId; // Trust user ID from upstream

    public int getMaxHops() {
        return maxHops;
    }

    public void setMaxHops(int maxHops) {
        this.maxHops = maxHops;
    }

    public long getReceiveTimeout() {
        return receiveTimeout;
    }

    public void setReceiveTimeout(long receiveTimeout) {
        this.receiveTimeout = receiveTimeout;
    }

    public long getReconnectDelay() {
        return reconnectDelay;
    }

    public void setReconnectDelay(long reconnectDelay) {
        this.reconnectDelay = reconnectDelay;
    }

    public int getPrefetchCount() {
        return prefetchCount;
    }

    public void setPrefetchCount(int prefetchCount) {
        this.prefetchCount = prefetchCount;
    }

    public boolean isAckMode() {
        return ackMode;
    }

    public void setAckMode(boolean ackMode) {
        this.ackMode = ackMode;
    }

    public boolean isExposeUri() {
        return exposeUri;
    }

    public void setExposeUri(boolean exposeUri) {
        this.exposeUri = exposeUri;
    }

    public String getTrustUserId() {
        return trustUserId;
    }

    public void setTrustUserId(String trustUserId) {
        this.trustUserId = trustUserId;
    }

    @Override
    public String toString() {
        return String.format("FederationConfig{maxHops=%d, receiveTimeout=%d, reconnectDelay=%d}",
                           maxHops, receiveTimeout, reconnectDelay);
    }
}
