package com.amqp.flow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class FlowControl {
    private static final Logger logger = LoggerFactory.getLogger(FlowControl.class);

    // Memory thresholds
    private static final long DEFAULT_MEMORY_HIGH_WATERMARK = 400 * 1024 * 1024; // 400 MB
    private static final long DEFAULT_MEMORY_LOW_WATERMARK = 200 * 1024 * 1024;  // 200 MB

    // Disk thresholds
    private static final long DEFAULT_DISK_FREE_LIMIT = 50 * 1024 * 1024; // 50 MB

    private final AtomicBoolean connectionBlocked = new AtomicBoolean(false);
    private final AtomicBoolean channelFlowActive = new AtomicBoolean(true);
    private final AtomicLong memoryUsed = new AtomicLong(0);

    private long memoryHighWatermark = DEFAULT_MEMORY_HIGH_WATERMARK;
    private long memoryLowWatermark = DEFAULT_MEMORY_LOW_WATERMARK;
    private long diskFreeLimit = DEFAULT_DISK_FREE_LIMIT;

    private FlowControlListener listener;

    public interface FlowControlListener {
        void onConnectionBlocked(String reason);
        void onConnectionUnblocked();
        void onChannelFlow(boolean active);
    }

    public void setListener(FlowControlListener listener) {
        this.listener = listener;
    }

    public void setMemoryHighWatermark(long bytes) {
        this.memoryHighWatermark = bytes;
    }

    public void setMemoryLowWatermark(long bytes) {
        this.memoryLowWatermark = bytes;
    }

    public void setDiskFreeLimit(long bytes) {
        this.diskFreeLimit = bytes;
    }

    public void addMemoryUsage(long bytes) {
        long current = memoryUsed.addAndGet(bytes);
        checkMemoryThreshold(current);
    }

    public void releaseMemoryUsage(long bytes) {
        long current = memoryUsed.addAndGet(-bytes);
        checkMemoryThreshold(current);
    }

    public long getMemoryUsed() {
        return memoryUsed.get();
    }

    public boolean isConnectionBlocked() {
        return connectionBlocked.get();
    }

    public boolean isChannelFlowActive() {
        return channelFlowActive.get();
    }

    private void checkMemoryThreshold(long currentMemory) {
        if (currentMemory > memoryHighWatermark && !connectionBlocked.get()) {
            blockConnection("Memory usage above high watermark");
        } else if (currentMemory < memoryLowWatermark && connectionBlocked.get()) {
            unblockConnection();
        }
    }

    public void checkDiskSpace(long freeSpace) {
        if (freeSpace < diskFreeLimit && !connectionBlocked.get()) {
            blockConnection("Disk space below limit");
        } else if (freeSpace >= diskFreeLimit * 2 && connectionBlocked.get()) {
            unblockConnection();
        }
    }

    private void blockConnection(String reason) {
        if (connectionBlocked.compareAndSet(false, true)) {
            logger.warn("Blocking connection: {}", reason);
            if (listener != null) {
                listener.onConnectionBlocked(reason);
            }
        }
    }

    private void unblockConnection() {
        if (connectionBlocked.compareAndSet(true, false)) {
            logger.info("Unblocking connection");
            if (listener != null) {
                listener.onConnectionUnblocked();
            }
        }
    }

    public void setChannelFlow(boolean active) {
        if (channelFlowActive.compareAndSet(!active, active)) {
            logger.info("Channel flow set to: {}", active);
            if (listener != null) {
                listener.onChannelFlow(active);
            }
        }
    }

    public void reset() {
        connectionBlocked.set(false);
        channelFlowActive.set(true);
        memoryUsed.set(0);
    }
}
