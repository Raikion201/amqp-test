package com.amqp.management;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Statistics {
    private final AtomicLong messagesPublished = new AtomicLong(0);
    private final AtomicLong messagesDelivered = new AtomicLong(0);
    private final AtomicLong messagesAcknowledged = new AtomicLong(0);
    private final AtomicLong messagesRedelivered = new AtomicLong(0);
    private final AtomicLong messagesReturned = new AtomicLong(0);
    private final AtomicLong connectionsCreated = new AtomicLong(0);
    private final AtomicLong connectionsClosed = new AtomicLong(0);
    private final AtomicLong channelsCreated = new AtomicLong(0);
    private final AtomicLong channelsClosed = new AtomicLong(0);

    public void recordPublish() {
        messagesPublished.incrementAndGet();
    }

    public void recordPublish(long count) {
        messagesPublished.addAndGet(count);
    }

    public void recordDeliver() {
        messagesDelivered.incrementAndGet();
    }

    public void recordAck() {
        messagesAcknowledged.incrementAndGet();
    }

    public void recordRedeliver() {
        messagesRedelivered.incrementAndGet();
    }

    public void recordReturn() {
        messagesReturned.incrementAndGet();
    }

    public void recordConnectionCreated() {
        connectionsCreated.incrementAndGet();
    }

    public void recordConnectionClosed() {
        connectionsClosed.incrementAndGet();
    }

    public void recordChannelCreated() {
        channelsCreated.incrementAndGet();
    }

    public void recordChannelClosed() {
        channelsClosed.incrementAndGet();
    }

    public long getMessagesPublished() {
        return messagesPublished.get();
    }

    public long getMessagesDelivered() {
        return messagesDelivered.get();
    }

    public long getMessagesAcknowledged() {
        return messagesAcknowledged.get();
    }

    public long getMessagesRedelivered() {
        return messagesRedelivered.get();
    }

    public long getMessagesReturned() {
        return messagesReturned.get();
    }

    public long getConnectionsCreated() {
        return connectionsCreated.get();
    }

    public long getConnectionsClosed() {
        return connectionsClosed.get();
    }

    public long getChannelsCreated() {
        return channelsCreated.get();
    }

    public long getChannelsClosed() {
        return channelsClosed.get();
    }

    public long getCurrentConnections() {
        return connectionsCreated.get() - connectionsClosed.get();
    }

    public long getCurrentChannels() {
        return channelsCreated.get() - channelsClosed.get();
    }

    public Map<String, Object> getGlobalStatistics() {
        Map<String, Object> stats = new HashMap<>();

        Map<String, Long> publish = new HashMap<>();
        publish.put("publish", messagesPublished.get());
        stats.put("message_stats", publish);

        Map<String, Long> queueStats = new HashMap<>();
        queueStats.put("messages", 0L);
        queueStats.put("messages_ready", 0L);
        queueStats.put("messages_unacknowledged", 0L);
        stats.put("queue_totals", queueStats);

        return stats;
    }

    public void reset() {
        messagesPublished.set(0);
        messagesDelivered.set(0);
        messagesAcknowledged.set(0);
        messagesRedelivered.set(0);
        messagesReturned.set(0);
        connectionsCreated.set(0);
        connectionsClosed.set(0);
        channelsCreated.set(0);
        channelsClosed.set(0);
    }
}
