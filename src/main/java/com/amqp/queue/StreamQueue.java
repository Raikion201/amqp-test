package com.amqp.queue;

import com.amqp.model.Message;
import com.amqp.model.Queue;
import com.amqp.model.QueueArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stream Queue implementation - an append-only, replayed message log similar to Apache Kafka.
 * Unlike regular queues, stream queues:
 * - Messages are never deleted on consumption
 * - Consumers track their own offset
 * - Messages can be replayed from any offset
 * - Retention is based on size or time limits
 * - Ideal for event sourcing and message replay scenarios
 */
public class StreamQueue extends Queue {
    private static final Logger logger = LoggerFactory.getLogger(StreamQueue.class);

    private final QueueArguments arguments;
    private final ConcurrentMap<Long, StreamEntry> stream;
    private final AtomicLong nextOffset;
    private final ConcurrentMap<String, Long> consumerOffsets;
    private final long maxAge; // Maximum age in milliseconds
    private final long maxBytes; // Maximum stream size in bytes
    private final long maxSegmentSize; // Maximum segment size
    private final AtomicLong totalBytes;

    // Retention cleanup
    private final ScheduledExecutorService cleanupScheduler;
    private volatile ScheduledFuture<?> cleanupTask;

    public StreamQueue(String name, boolean durable, QueueArguments arguments) {
        super(name, durable, false, false); // Streams are never exclusive or auto-delete
        this.arguments = arguments != null ? arguments : new QueueArguments();
        this.stream = new ConcurrentSkipListMap<>();
        this.nextOffset = new AtomicLong(0);
        this.consumerOffsets = new ConcurrentHashMap<>();
        this.totalBytes = new AtomicLong(0);

        // Configure retention
        this.maxAge = arguments.hasMaxAge() ? arguments.getMaxAge() : Long.MAX_VALUE;
        this.maxBytes = arguments.hasMaxBytes() ? arguments.getMaxBytes() : Long.MAX_VALUE;
        this.maxSegmentSize = arguments.hasMaxSegmentSize() ? arguments.getMaxSegmentSize() : 100_000_000L; // 100MB default

        // Start retention cleanup task
        this.cleanupScheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "stream-cleanup-" + name));
        startCleanupTask();

        logger.info("Stream queue created: {} with maxAge={}ms, maxBytes={}",
                   name, maxAge, maxBytes);
    }

    @Override
    public void enqueue(Message message) {
        long offset = nextOffset.getAndIncrement();
        long timestamp = System.currentTimeMillis();

        StreamEntry entry = new StreamEntry(offset, timestamp, message);
        stream.put(offset, entry);

        long messageSize = calculateMessageSize(message);
        totalBytes.addAndGet(messageSize);

        logger.debug("Appended message at offset {} to stream {}", offset, getName());

        // Check if retention cleanup is needed
        enforceRetention();
    }

    @Override
    public Message dequeue() {
        throw new UnsupportedOperationException(
            "Cannot dequeue from stream - use consumeFrom(offset) instead");
    }

    /**
     * Consume a message from a specific offset.
     * Does NOT remove the message from the stream.
     */
    public Message consumeFrom(String consumerTag, long offset) {
        StreamEntry entry = stream.get(offset);
        if (entry == null) {
            return null;
        }

        // Update consumer offset
        consumerOffsets.put(consumerTag, offset + 1);

        logger.debug("Consumer {} read offset {} from stream {}", consumerTag, offset, getName());
        return entry.message;
    }

    /**
     * Consume next message for a consumer (using tracked offset).
     */
    public Message consumeNext(String consumerTag) {
        long offset = consumerOffsets.getOrDefault(consumerTag, 0L);
        return consumeFrom(consumerTag, offset);
    }

    /**
     * Consume a batch of messages from an offset.
     */
    public List<Message> consumeBatch(String consumerTag, long startOffset, int maxMessages) {
        List<Message> batch = new ArrayList<>();

        for (long i = startOffset; i < startOffset + maxMessages && i < nextOffset.get(); i++) {
            StreamEntry entry = stream.get(i);
            if (entry != null) {
                batch.add(entry.message);
            }
        }

        if (!batch.isEmpty()) {
            consumerOffsets.put(consumerTag, startOffset + batch.size());
            logger.debug("Consumer {} read batch of {} messages from stream {}",
                        consumerTag, batch.size(), getName());
        }

        return batch;
    }

    /**
     * Get the current offset for a consumer.
     */
    public long getConsumerOffset(String consumerTag) {
        return consumerOffsets.getOrDefault(consumerTag, 0L);
    }

    /**
     * Set the offset for a consumer (seek operation).
     */
    public void setConsumerOffset(String consumerTag, long offset) {
        if (offset < 0 || offset >= nextOffset.get()) {
            throw new IllegalArgumentException("Invalid offset: " + offset);
        }
        consumerOffsets.put(consumerTag, offset);
        logger.debug("Consumer {} offset set to {} on stream {}", consumerTag, offset, getName());
    }

    /**
     * Reset consumer offset to beginning.
     */
    public void resetConsumerOffset(String consumerTag) {
        long firstOffset = stream.isEmpty() ? 0 : stream.keySet().iterator().next();
        consumerOffsets.put(consumerTag, firstOffset);
        logger.debug("Consumer {} offset reset to {} on stream {}", consumerTag, firstOffset, getName());
    }

    /**
     * Get the first (oldest) available offset.
     */
    public long getFirstOffset() {
        return stream.isEmpty() ? 0 : stream.keySet().iterator().next();
    }

    /**
     * Get the last (newest) offset.
     */
    public long getLastOffset() {
        return nextOffset.get() - 1;
    }

    @Override
    public int size() {
        return stream.size();
    }

    /**
     * Get total bytes stored in stream.
     */
    public long getTotalBytes() {
        return totalBytes.get();
    }

    @Override
    public void purge() {
        logger.info("Purging stream queue: {}", getName());
        stream.clear();
        consumerOffsets.clear();
        nextOffset.set(0);
        totalBytes.set(0);
    }

    /**
     * Enforce retention policy by removing old messages.
     */
    private void enforceRetention() {
        long now = System.currentTimeMillis();
        long bytesToRemove = totalBytes.get() - maxBytes;

        if (bytesToRemove <= 0 && maxAge == Long.MAX_VALUE) {
            return; // No retention needed
        }

        List<Long> toRemove = new ArrayList<>();

        for (Map.Entry<Long, StreamEntry> entry : stream.entrySet()) {
            StreamEntry streamEntry = entry.getValue();

            // Check age-based retention
            if (now - streamEntry.timestamp > maxAge) {
                toRemove.add(entry.getKey());
                continue;
            }

            // Check size-based retention
            if (bytesToRemove > 0) {
                toRemove.add(entry.getKey());
                bytesToRemove -= calculateMessageSize(streamEntry.message);
            } else {
                break; // Retention satisfied
            }
        }

        // Remove entries
        for (Long offset : toRemove) {
            StreamEntry removed = stream.remove(offset);
            if (removed != null) {
                long size = calculateMessageSize(removed.message);
                totalBytes.addAndGet(-size);
            }
        }

        if (!toRemove.isEmpty()) {
            logger.info("Removed {} messages from stream {} due to retention policy",
                       toRemove.size(), getName());
        }
    }

    /**
     * Start background cleanup task for retention.
     */
    private void startCleanupTask() {
        cleanupTask = cleanupScheduler.scheduleAtFixedRate(
            () -> {
                try {
                    enforceRetention();
                } catch (Exception e) {
                    logger.error("Error during retention cleanup on stream {}", getName(), e);
                }
            },
            60, // Initial delay
            60, // Period
            TimeUnit.SECONDS
        );
    }

    /**
     * Calculate approximate size of a message.
     */
    private long calculateMessageSize(Message message) {
        long size = 0;

        if (message.getBody() != null) {
            size += message.getBody().length;
        }

        if (message.getHeaders() != null) {
            size += message.getHeaders().size() * 50; // Approximate
        }

        size += 200; // Fixed overhead

        return size;
    }

    /**
     * Get stream statistics.
     */
    public StreamStats getStats() {
        return new StreamStats(
            getName(),
            stream.size(),
            totalBytes.get(),
            getFirstOffset(),
            getLastOffset(),
            consumerOffsets.size()
        );
    }

    /**
     * Shutdown the stream queue.
     */
    public void shutdown() {
        logger.info("Shutting down stream queue: {}", getName());

        if (cleanupTask != null) {
            cleanupTask.cancel(false);
        }

        cleanupScheduler.shutdown();
        try {
            if (!cleanupScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toString() {
        return String.format("StreamQueue{name='%s', messages=%d, bytes=%d, firstOffset=%d, lastOffset=%d}",
                           getName(), size(), totalBytes.get(), getFirstOffset(), getLastOffset());
    }

    /**
     * Stream entry containing message and metadata.
     */
    private static class StreamEntry {
        final long offset;
        final long timestamp;
        final Message message;

        StreamEntry(long offset, long timestamp, Message message) {
            this.offset = offset;
            this.timestamp = timestamp;
            this.message = message;
        }
    }

    /**
     * Stream statistics.
     */
    public static class StreamStats {
        public final String name;
        public final int messageCount;
        public final long totalBytes;
        public final long firstOffset;
        public final long lastOffset;
        public final int consumerCount;

        public StreamStats(String name, int messageCount, long totalBytes,
                          long firstOffset, long lastOffset, int consumerCount) {
            this.name = name;
            this.messageCount = messageCount;
            this.totalBytes = totalBytes;
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
            this.consumerCount = consumerCount;
        }

        @Override
        public String toString() {
            return String.format("StreamStats{name='%s', messages=%d, bytes=%d, range=[%d,%d], consumers=%d}",
                               name, messageCount, totalBytes, firstOffset, lastOffset, consumerCount);
        }
    }
}
