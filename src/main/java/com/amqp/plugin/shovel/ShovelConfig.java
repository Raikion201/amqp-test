package com.amqp.plugin.shovel;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for a Shovel.
 */
public class ShovelConfig {
    private String ackMode = "on-confirm"; // on-publish, on-confirm, no-ack
    private int prefetchCount = 1000;
    private long prefetchInterval = 100; // milliseconds
    private long reconnectDelay = 5000; // milliseconds
    private boolean deleteAfter = false; // Delete shovel after completion
    private Map<String, Object> transformations;

    public String getAckMode() {
        return ackMode;
    }

    public void setAckMode(String ackMode) {
        this.ackMode = ackMode;
    }

    public int getPrefetchCount() {
        return prefetchCount;
    }

    public void setPrefetchCount(int prefetchCount) {
        this.prefetchCount = prefetchCount;
    }

    public long getPrefetchInterval() {
        return prefetchInterval;
    }

    public void setPrefetchInterval(long prefetchInterval) {
        this.prefetchInterval = prefetchInterval;
    }

    public long getReconnectDelay() {
        return reconnectDelay;
    }

    public void setReconnectDelay(long reconnectDelay) {
        this.reconnectDelay = reconnectDelay;
    }

    public boolean isDeleteAfter() {
        return deleteAfter;
    }

    public void setDeleteAfter(boolean deleteAfter) {
        this.deleteAfter = deleteAfter;
    }

    public Map<String, Object> getTransformations() {
        return transformations;
    }

    public void setTransformations(Map<String, Object> transformations) {
        this.transformations = new HashMap<>(transformations);
    }

    @Override
    public String toString() {
        return String.format("ShovelConfig{ackMode='%s', prefetchCount=%d, reconnectDelay=%d}",
                           ackMode, prefetchCount, reconnectDelay);
    }
}
