package com.amqp.exchange;

import com.amqp.model.Exchange;
import com.amqp.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Delayed Message Exchange - holds messages for a specified delay before routing them.
 * This implements the RabbitMQ delayed message plugin functionality.
 *
 * Messages can specify delay in milliseconds using:
 * - x-delay header (takes precedence)
 * - x-message-ttl header (fallback)
 */
public class DelayedMessageExchange extends Exchange {
    private static final Logger logger = LoggerFactory.getLogger(DelayedMessageExchange.class);

    private final Type wrappedType; // The actual exchange type (direct, topic, fanout)
    private final ScheduledExecutorService scheduler;
    private final ConcurrentMap<String, DelayedMessage> delayedMessages;
    private final DelayedMessageCallback callback;

    public interface DelayedMessageCallback {
        void onDelayExpired(Message message, String exchangeName, String routingKey);
    }

    public DelayedMessageExchange(String name, Type wrappedType, boolean durable,
                                  boolean autoDelete, boolean internal,
                                  DelayedMessageCallback callback) {
        super(name, wrappedType, durable, autoDelete, internal);
        this.wrappedType = wrappedType;
        this.scheduler = Executors.newScheduledThreadPool(2,
            r -> {
                Thread t = new Thread(r, "delayed-exchange-" + name);
                t.setDaemon(true);
                return t;
            });
        this.delayedMessages = new ConcurrentHashMap<>();
        this.callback = callback;
    }

    /**
     * Route message with delay. Instead of routing immediately, schedule for later delivery.
     */
    public String routeWithDelay(Message message, String routingKey) {
        long delay = extractDelay(message);

        if (delay <= 0) {
            // No delay, route immediately using parent logic
            return null; // Indicates immediate routing
        }

        // Generate unique message ID
        String messageId = UUID.randomUUID().toString();

        DelayedMessage delayed = new DelayedMessage(messageId, message, routingKey, delay);
        delayedMessages.put(messageId, delayed);

        // Schedule delayed routing
        ScheduledFuture<?> future = scheduler.schedule(
            () -> deliverDelayedMessage(messageId),
            delay,
            TimeUnit.MILLISECONDS
        );

        delayed.setFuture(future);

        logger.debug("Scheduled message {} for delivery after {} ms on exchange {}",
                    messageId, delay, getName());

        return messageId;
    }

    /**
     * Extract delay from message headers.
     * Priority: x-delay > x-message-ttl > 0
     */
    private long extractDelay(Message message) {
        Map<String, Object> headers = message.getHeaders();
        if (headers == null) {
            return 0;
        }

        // Check x-delay header (milliseconds)
        Object xDelay = headers.get("x-delay");
        if (xDelay != null) {
            return parseLong(xDelay);
        }

        // Fallback to x-message-ttl (milliseconds)
        Object xTtl = headers.get("x-message-ttl");
        if (xTtl != null) {
            return parseLong(xTtl);
        }

        return 0;
    }

    private long parseLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                logger.warn("Invalid delay value: {}", value);
                return 0;
            }
        }
        return 0;
    }

    /**
     * Deliver a delayed message when its delay expires.
     */
    private void deliverDelayedMessage(String messageId) {
        DelayedMessage delayed = delayedMessages.remove(messageId);
        if (delayed == null) {
            return; // Message was cancelled
        }

        try {
            if (callback != null) {
                callback.onDelayExpired(delayed.message, getName(), delayed.routingKey);
            }
            logger.debug("Delivered delayed message {} on exchange {}", messageId, getName());
        } catch (Exception e) {
            logger.error("Error delivering delayed message {}", messageId, e);
        }
    }

    /**
     * Cancel a delayed message.
     */
    public boolean cancelDelayedMessage(String messageId) {
        DelayedMessage delayed = delayedMessages.remove(messageId);
        if (delayed != null && delayed.future != null) {
            delayed.future.cancel(false);
            logger.debug("Cancelled delayed message {}", messageId);
            return true;
        }
        return false;
    }

    /**
     * Get count of currently delayed messages.
     */
    public int getDelayedMessageCount() {
        return delayedMessages.size();
    }

    /**
     * Get all delayed messages (for monitoring).
     */
    public List<DelayedMessageInfo> getDelayedMessages() {
        List<DelayedMessageInfo> infos = new ArrayList<>();
        for (DelayedMessage delayed : delayedMessages.values()) {
            long remaining = delayed.future != null ?
                           delayed.future.getDelay(TimeUnit.MILLISECONDS) : 0;
            infos.add(new DelayedMessageInfo(delayed.messageId, delayed.routingKey,
                                            delayed.delay, remaining));
        }
        return infos;
    }

    /**
     * Shutdown the scheduler and cancel all delayed messages.
     */
    public void shutdown() {
        logger.info("Shutting down delayed exchange: {}", getName());

        // Cancel all pending messages
        for (DelayedMessage delayed : delayedMessages.values()) {
            if (delayed.future != null) {
                delayed.future.cancel(false);
            }
        }
        delayedMessages.clear();

        // Shutdown scheduler
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static class DelayedMessage {
        final String messageId;
        final Message message;
        final String routingKey;
        final long delay;
        ScheduledFuture<?> future;

        DelayedMessage(String messageId, Message message, String routingKey, long delay) {
            this.messageId = messageId;
            this.message = message;
            this.routingKey = routingKey;
            this.delay = delay;
        }

        void setFuture(ScheduledFuture<?> future) {
            this.future = future;
        }
    }

    public static class DelayedMessageInfo {
        public final String messageId;
        public final String routingKey;
        public final long originalDelay;
        public final long remainingDelay;

        public DelayedMessageInfo(String messageId, String routingKey,
                                 long originalDelay, long remainingDelay) {
            this.messageId = messageId;
            this.routingKey = routingKey;
            this.originalDelay = originalDelay;
            this.remainingDelay = remainingDelay;
        }
    }

    @Override
    public String toString() {
        return String.format("DelayedMessageExchange{name='%s', type=%s, delayed=%d}",
                           getName(), wrappedType, delayedMessages.size());
    }
}
