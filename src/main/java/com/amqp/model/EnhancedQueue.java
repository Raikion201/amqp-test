package com.amqp.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class EnhancedQueue extends Queue {
    private static final Logger logger = LoggerFactory.getLogger(EnhancedQueue.class);

    private final QueueArguments arguments;
    private final PriorityBlockingQueue<MessageWrapper> priorityMessages;
    private final long createdAt;
    private volatile long lastAccessTime;
    private final AtomicLong totalBytesSize = new AtomicLong(0);
    private final ScheduledExecutorService ttlScheduler;
    private final List<MessageWrapper> deadLetterQueue = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<Message, Long> messageTimestamps = new ConcurrentHashMap<>();

    public EnhancedQueue(String name, boolean durable, boolean exclusive,
                        boolean autoDelete, QueueArguments arguments) {
        super(name, durable, exclusive, autoDelete);
        this.arguments = arguments != null ? arguments : new QueueArguments();
        this.createdAt = System.currentTimeMillis();
        this.lastAccessTime = createdAt;

        // Initialize priority queue if needed
        if (this.arguments.isPriorityQueue()) {
            this.priorityMessages = new PriorityBlockingQueue<>(1000,
                Comparator.comparingInt(MessageWrapper::getPriority).reversed()
                    .thenComparingLong(MessageWrapper::getTimestamp));
        } else {
            this.priorityMessages = null;
        }

        // Start TTL scheduler if TTL is configured
        if (this.arguments.hasTTL()) {
            this.ttlScheduler = Executors.newScheduledThreadPool(1);
            startTTLChecker();
        } else {
            this.ttlScheduler = null;
        }
    }

    @Override
    public void enqueue(Message message) {
        lastAccessTime = System.currentTimeMillis();

        // Check if queue has expired
        if (hasExpired()) {
            logger.warn("Cannot enqueue to expired queue: {}", getName());
            return;
        }

        // Check length limits
        if (arguments.hasMaxLength() && size() >= arguments.getMaxLength()) {
            handleOverflow(message);
            return;
        }

        // Check byte size limits
        if (arguments.hasMaxLengthBytes()) {
            long messageSize = calculateMessageSize(message);
            if (totalBytesSize.get() + messageSize > arguments.getMaxLengthBytes()) {
                handleOverflow(message);
                return;
            }
            totalBytesSize.addAndGet(messageSize);
        }

        // Wrap message with metadata
        long timestamp = System.currentTimeMillis();
        MessageWrapper wrapper = new MessageWrapper(message, timestamp);

        // Enqueue based on queue type
        if (arguments.isPriorityQueue()) {
            priorityMessages.offer(wrapper);
        } else {
            messageTimestamps.put(message, timestamp);
            super.enqueue(message);
        }
    }

    @Override
    public Message dequeue() {
        lastAccessTime = System.currentTimeMillis();

        MessageWrapper wrapper = null;

        if (arguments.isPriorityQueue()) {
            wrapper = priorityMessages.poll();
        } else {
            Message msg = super.dequeue();
            if (msg != null) {
                Long timestamp = messageTimestamps.remove(msg);
                if (timestamp == null) {
                    timestamp = System.currentTimeMillis();
                }
                wrapper = new MessageWrapper(msg, timestamp);
            }
        }

        if (wrapper == null) {
            return null;
        }

        // Check if message has expired
        if (isExpired(wrapper)) {
            handleExpiredMessage(wrapper);
            return dequeue(); // Try next message
        }

        // Update byte size
        if (arguments.hasMaxLengthBytes()) {
            long messageSize = calculateMessageSize(wrapper.getMessage());
            totalBytesSize.addAndGet(-messageSize);
        }

        return wrapper.getMessage();
    }

    @Override
    public int size() {
        if (arguments.isPriorityQueue()) {
            return priorityMessages.size();
        } else {
            return super.size();
        }
    }

    @Override
    public void purge() {
        super.purge();
        if (priorityMessages != null) {
            priorityMessages.clear();
        }
        messageTimestamps.clear();
        totalBytesSize.set(0);
    }

    public QueueArguments getArguments() {
        return arguments;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public long getTotalBytesSize() {
        return totalBytesSize.get();
    }

    public boolean hasExpired() {
        if (!arguments.hasExpiry()) {
            return false;
        }

        long age = System.currentTimeMillis() - lastAccessTime;
        return age > arguments.getExpires();
    }

    private boolean isExpired(MessageWrapper wrapper) {
        // Check message-level TTL
        Message message = wrapper.getMessage();
        if (message.getExpiration() != null) {
            try {
                long expiration = Long.parseLong(message.getExpiration());
                long age = System.currentTimeMillis() - wrapper.getTimestamp();
                if (age > expiration) {
                    return true;
                }
            } catch (NumberFormatException e) {
                logger.warn("Invalid message expiration: {}", message.getExpiration());
            }
        }

        // Check queue-level TTL
        if (arguments.hasTTL()) {
            long age = System.currentTimeMillis() - wrapper.getTimestamp();
            return age > arguments.getMessageTtl();
        }

        return false;
    }

    private void handleExpiredMessage(MessageWrapper wrapper) {
        logger.debug("Message expired in queue {}", getName());

        if (arguments.hasDLX()) {
            sendToDeadLetterExchange(wrapper.getMessage(), "expired");
        }
    }

    private void handleOverflow(Message message) {
        String behavior = arguments.getOverflowBehavior();

        switch (behavior) {
            case "drop-head":
                // Remove oldest message and add new one
                Message dropped = dequeue();
                if (dropped != null && arguments.hasDLX()) {
                    sendToDeadLetterExchange(dropped, "maxlen");
                }
                enqueue(message);
                break;

            case "reject-publish":
                // Reject the message (caller should handle)
                logger.debug("Message rejected due to queue overflow: {}", getName());
                throw new IllegalStateException("Queue overflow: " + getName());

            case "reject-publish-dlx":
                // Reject and send to DLX
                if (arguments.hasDLX()) {
                    sendToDeadLetterExchange(message, "maxlen");
                }
                throw new IllegalStateException("Queue overflow: " + getName());

            default:
                logger.warn("Unknown overflow behavior: {}", behavior);
                break;
        }
    }

    private void sendToDeadLetterExchange(Message message, String reason) {
        // Add headers to indicate dead-lettering
        Map<String, Object> headers = message.getHeaders();
        if (headers == null) {
            headers = new HashMap<>();
            message.setHeaders(headers);
        }

        String routingKey = message.getRoutingKey() != null ? message.getRoutingKey() : "";
        headers.put("x-death", Map.of(
            "reason", reason,
            "queue", getName(),
            "time", System.currentTimeMillis(),
            "exchange", arguments.getDeadLetterExchange(),
            "routing-keys", List.of(routingKey)
        ));

        // Store for later processing by broker
        deadLetterQueue.add(new MessageWrapper(message, System.currentTimeMillis()));

        logger.debug("Message sent to DLX: {} from queue: {}", arguments.getDeadLetterExchange(), getName());
    }

    public List<MessageWrapper> getAndClearDeadLetterQueue() {
        List<MessageWrapper> messages = new ArrayList<>(deadLetterQueue);
        deadLetterQueue.clear();
        return messages;
    }

    private long calculateMessageSize(Message message) {
        long size = 0;

        if (message.getBody() != null) {
            size += message.getBody().length;
        }

        if (message.getHeaders() != null) {
            // Approximate header size
            size += message.getHeaders().size() * 50; // rough estimate
        }

        // Add fixed overhead for properties
        size += 200;

        return size;
    }

    private void startTTLChecker() {
        ttlScheduler.scheduleAtFixedRate(() -> {
            try {
                checkExpiredMessages();
            } catch (Exception e) {
                logger.error("Error checking expired messages in queue: {}", getName(), e);
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    private void checkExpiredMessages() {
        if (arguments.isPriorityQueue()) {
            // Check priority queue messages
            Iterator<MessageWrapper> iterator = priorityMessages.iterator();
            while (iterator.hasNext()) {
                MessageWrapper wrapper = iterator.next();
                if (isExpired(wrapper)) {
                    iterator.remove();
                    handleExpiredMessage(wrapper);
                }
            }
        }
        // For regular queue, messages are checked on dequeue
    }

    public void shutdown() {
        if (ttlScheduler != null) {
            ttlScheduler.shutdown();
            try {
                if (!ttlScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    ttlScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                ttlScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public String toString() {
        return String.format("EnhancedQueue{name='%s', size=%d, bytes=%d, args=%s}",
                           getName(), size(), totalBytesSize.get(), arguments);
    }

    // Inner class to wrap messages with metadata
    public static class MessageWrapper {
        private final Message message;
        private final long timestamp;

        public MessageWrapper(Message message, long timestamp) {
            this.message = message;
            this.timestamp = timestamp;
        }

        public Message getMessage() {
            return message;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public int getPriority() {
            return message.getPriority();
        }
    }
}
