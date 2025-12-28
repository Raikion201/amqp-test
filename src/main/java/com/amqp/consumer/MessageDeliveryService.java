package com.amqp.consumer;

import com.amqp.connection.AmqpConnection;
import com.amqp.model.Message;
import com.amqp.model.Queue;
import com.amqp.amqp.AmqpCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service responsible for delivering messages from queues to consumers.
 * Runs background threads that continuously poll queues and push messages to registered consumers.
 */
public class MessageDeliveryService {
    private static final Logger logger = LoggerFactory.getLogger(MessageDeliveryService.class);

    private final ConsumerManager consumerManager;
    private final ScheduledExecutorService scheduler;
    private final ConcurrentMap<String, DeliveryWorker> queueWorkers = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public MessageDeliveryService(ConsumerManager consumerManager) {
        this.consumerManager = consumerManager;
        this.scheduler = Executors.newScheduledThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors()),
            new ThreadFactory() {
                private final AtomicLong counter = new AtomicLong(0);
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "MessageDelivery-" + counter.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                }
            }
        );
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("Message Delivery Service started");
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping Message Delivery Service...");
            queueWorkers.values().forEach(DeliveryWorker::stop);
            queueWorkers.clear();
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("Message Delivery Service stopped");
        }
    }

    /**
     * Start delivering messages from a queue to its consumers.
     */
    public void startDelivery(Queue queue) {
        if (!running.get()) {
            return;
        }

        String queueName = queue.getName();
        queueWorkers.computeIfAbsent(queueName, k -> {
            DeliveryWorker worker = new DeliveryWorker(queue);
            worker.start();
            return worker;
        });
    }

    /**
     * Stop delivering messages from a queue.
     */
    public void stopDelivery(String queueName) {
        DeliveryWorker worker = queueWorkers.remove(queueName);
        if (worker != null) {
            worker.stop();
        }
    }

    /**
     * Trigger immediate delivery check for a queue (e.g., after new message published).
     */
    public void triggerDelivery(String queueName) {
        DeliveryWorker worker = queueWorkers.get(queueName);
        if (worker != null) {
            worker.trigger();
        }
    }

    /**
     * Worker that continuously delivers messages from a queue to its consumers.
     */
    private class DeliveryWorker implements Runnable {
        private final Queue queue;
        private final AtomicBoolean active = new AtomicBoolean(false);
        private final AtomicLong deliveryTag = new AtomicLong(0);
        private ScheduledFuture<?> scheduledFuture;

        DeliveryWorker(Queue queue) {
            this.queue = queue;
        }

        void start() {
            if (active.compareAndSet(false, true)) {
                // Schedule periodic delivery checks (every 10ms)
                scheduledFuture = scheduler.scheduleAtFixedRate(
                    this,
                    0,
                    10,
                    TimeUnit.MILLISECONDS
                );
                logger.info("Started delivery worker for queue: {}", queue.getName());
            }
        }

        void stop() {
            if (active.compareAndSet(true, false)) {
                if (scheduledFuture != null) {
                    scheduledFuture.cancel(false);
                }
                logger.debug("Stopped delivery worker for queue: {}", queue.getName());
            }
        }

        void trigger() {
            // Just run immediately since we're already scheduled frequently
            scheduler.submit(this);
        }

        @Override
        public void run() {
            if (!active.get() || !running.get()) {
                return;
            }

            try {
                deliverMessages();
            } catch (Exception e) {
                logger.error("Error delivering messages from queue: {}", queue.getName(), e);
            }
        }

        private void deliverMessages() {
            Set<ConsumerManager.Consumer> consumers = consumerManager.getConsumersForQueue(queue.getName());
            logger.debug("deliverMessages: queue={}, consumers={}", queue.getName(), consumers.size());

            if (consumers.isEmpty()) {
                return; // No consumers, skip
            }

            int queueSize = queue.size();
            if (queueSize == 0) {
                return; // No messages
            }

            logger.debug("Delivering messages: queue={}, consumers={}, queueSize={}",
                        queue.getName(), consumers.size(), queueSize);

            // Deliver messages in round-robin fashion to active consumers
            for (ConsumerManager.Consumer consumer : consumers) {
                if (!consumer.isActive()) {
                    logger.debug("Consumer {} is not active, skipping", consumer.getConsumerTag());
                    continue;
                }

                // Verify connection and channel are still valid
                AmqpConnection consumerConnection = (AmqpConnection) consumer.getConnection();
                if (consumerConnection == null) {
                    logger.debug("Consumer {} has no connection, skipping", consumer.getConsumerTag());
                    continue;
                }

                com.amqp.connection.AmqpChannel channel = consumerConnection.getChannel(consumer.getChannelNumber());
                if (channel == null || !channel.isOpen()) {
                    logger.debug("Consumer {} channel is closed or null, skipping", consumer.getConsumerTag());
                    consumer.setActive(false); // Mark as inactive since channel is gone
                    continue;
                }

                // Check prefetch limit per channel
                if (!consumer.isNoAck()) {
                    int prefetchCount = channel.getPrefetchCount();
                    int unackedCount = channel.getUnackedMessageCount();
                    if (prefetchCount > 0 && unackedCount >= prefetchCount) {
                        logger.debug("Prefetch limit reached for consumer {}: unacked={}, prefetch={}",
                                   consumer.getConsumerTag(), unackedCount, prefetchCount);
                        continue; // Skip this consumer - at prefetch limit
                    }
                }

                // Atomically dequeue and deliver to prevent race conditions
                // The actual dequeue happens inside synchronized block in deliverMessage
                long tag = deliveryTag.incrementAndGet();
                boolean delivered = tryDeliverMessage(consumer, tag);
                if (!delivered) {
                    // No more messages or delivery failed - check queue state
                    if (queue.size() == 0) {
                        break; // Queue empty, stop trying
                    }
                    // Otherwise, try next consumer
                }
            }
        }

        /**
         * Attempts to dequeue and deliver a message atomically.
         * Returns true if a message was successfully delivered, false otherwise.
         */
        private boolean tryDeliverMessage(ConsumerManager.Consumer consumer, long workerDeliveryTag) {
            // Get the connection for this specific consumer
            AmqpConnection consumerConnection = (AmqpConnection) consumer.getConnection();
            if (consumerConnection == null) {
                logger.warn("Connection not found for consumer: {}", consumer.getConsumerTag());
                return false;
            }

            // Verify channel is still open before delivery
            com.amqp.connection.AmqpChannel channel = consumerConnection.getChannel(consumer.getChannelNumber());
            if (channel == null || !channel.isOpen()) {
                logger.warn("Channel closed for consumer: {}", consumer.getConsumerTag());
                return false;
            }

            Message message = null;
            try {
                // Synchronize on the connection to prevent frame interleaving
                // Each message must be sent atomically: METHOD → HEADER → BODY
                synchronized (consumerConnection) {
                    // Double-check consumer is still active and channel is open
                    // This check is done inside the lock to prevent race with channel close
                    if (!consumer.isActive() || !channel.isOpen()) {
                        logger.debug("Consumer {} cancelled or channel closed during delivery",
                                   consumer.getConsumerTag());
                        return false;
                    }

                    // CRITICAL: Dequeue INSIDE the synchronized block
                    // This ensures we only remove the message when we can definitely deliver it
                    message = queue.dequeue();
                    if (message == null) {
                        return false; // Queue empty
                    }

                    // Check if message has expired
                    if (message.isExpired()) {
                        logger.debug("Message expired, discarding: id={}, ttl={}, absoluteExpiry={}",
                                message.getMessageId(), message.getTtl(), message.getAbsoluteExpiryTime());
                        // Don't re-queue expired messages, but continue trying
                        // We need to return true to indicate we processed something
                        return true;
                    }

                    // Track delivery in the channel if not in noAck mode
                    // Done inside synchronized block to ensure consistency
                    long channelDeliveryTag = workerDeliveryTag; // Default for noAck
                    if (!consumer.isNoAck()) {
                        channelDeliveryTag = channel.trackDelivery(
                            message, queue.getName(), consumerConnection.getVirtualHost(), false
                        );
                    }

                    // Now deliver the message (still inside sync block)
                    deliverMessageContent(consumer, consumerConnection, message, channelDeliveryTag);

                    logger.debug("Message delivered to consumer: {}, deliveryTag: {}",
                               consumer.getConsumerTag(), channelDeliveryTag);
                    return true;
                } // End synchronized
            } catch (Exception e) {
                logger.error("Exception while delivering message to consumer: {}", consumer.getConsumerTag(), e);
                // Re-queue the message if we dequeued it
                if (message != null) {
                    queue.enqueue(message);
                }
                return false;
            }
        }

        /**
         * Delivers message content to consumer. Called from inside synchronized block.
         * Does not handle re-queueing - caller is responsible for that.
         */
        private void deliverMessageContent(ConsumerManager.Consumer consumer,
                                           AmqpConnection consumerConnection,
                                           Message message,
                                           long channelDeliveryTag) {
            // Build Basic.Deliver frame
            ByteBuf deliverPayload = Unpooled.buffer();

            // Basic.Deliver method: class=60, method=60
            AmqpCodec.encodeShortString(deliverPayload, consumer.getConsumerTag());
            deliverPayload.writeLong(channelDeliveryTag);
            AmqpCodec.encodeBoolean(deliverPayload, false); // redelivered
            AmqpCodec.encodeShortString(deliverPayload, ""); // exchange (not stored in message)
            AmqpCodec.encodeShortString(deliverPayload, message.getRoutingKey() != null ? message.getRoutingKey() : "");

            consumerConnection.sendMethod(consumer.getChannelNumber(), (short) 60, (short) 60, deliverPayload);

            // Send Content Header
            long bodySize = message.getBody() != null ? message.getBody().length : 0;
            ByteBuf headerPayload = Unpooled.buffer();

            // Property flags
            short propertyFlags = 0;
            Short deliveryMode = message.getDeliveryMode();
            Short priority = message.getPriority();
            Long timestamp = message.getTimestamp();
            java.util.Map<String, Object> headers = message.getHeaders();

            if (message.getContentType() != null) propertyFlags |= (1 << 15);
            if (message.getContentEncoding() != null) propertyFlags |= (1 << 14);
            if (headers != null && !headers.isEmpty()) propertyFlags |= (1 << 13);
            if (deliveryMode != null) propertyFlags |= (1 << 12);
            if (priority != null) propertyFlags |= (1 << 11);
            if (message.getCorrelationId() != null) propertyFlags |= (1 << 10);
            if (message.getReplyTo() != null) propertyFlags |= (1 << 9);
            if (message.getExpiration() != null) propertyFlags |= (1 << 8);
            if (message.getMessageId() != null) propertyFlags |= (1 << 7);
            if (timestamp != null) propertyFlags |= (1 << 6);
            if (message.getType() != null) propertyFlags |= (1 << 5);
            if (message.getUserId() != null) propertyFlags |= (1 << 4);
            if (message.getAppId() != null) propertyFlags |= (1 << 3);

            headerPayload.writeShort(propertyFlags);

            // Encode properties
            if (message.getContentType() != null) {
                AmqpCodec.encodeShortString(headerPayload, message.getContentType());
            }
            if (message.getContentEncoding() != null) {
                AmqpCodec.encodeShortString(headerPayload, message.getContentEncoding());
            }
            if (headers != null && !headers.isEmpty()) {
                AmqpCodec.encodeTable(headerPayload, headers);
            }
            if (deliveryMode != null) {
                headerPayload.writeByte(deliveryMode);
            }
            if (priority != null) {
                headerPayload.writeByte(priority);
            }
            if (message.getCorrelationId() != null) {
                AmqpCodec.encodeShortString(headerPayload, message.getCorrelationId());
            }
            if (message.getReplyTo() != null) {
                AmqpCodec.encodeShortString(headerPayload, message.getReplyTo());
            }
            if (message.getExpiration() != null) {
                AmqpCodec.encodeShortString(headerPayload, message.getExpiration());
            }
            if (message.getMessageId() != null) {
                AmqpCodec.encodeShortString(headerPayload, message.getMessageId());
            }
            if (timestamp != null) {
                headerPayload.writeLong(timestamp);
            }
            if (message.getType() != null) {
                AmqpCodec.encodeShortString(headerPayload, message.getType());
            }
            if (message.getUserId() != null) {
                AmqpCodec.encodeShortString(headerPayload, message.getUserId());
            }
            if (message.getAppId() != null) {
                AmqpCodec.encodeShortString(headerPayload, message.getAppId());
            }

            consumerConnection.sendContentHeader(consumer.getChannelNumber(), (short) 60, bodySize, headerPayload);

            // Send Content Body
            if (message.getBody() != null && message.getBody().length > 0) {
                ByteBuf bodyBuf = Unpooled.wrappedBuffer(message.getBody());
                consumerConnection.sendContentBody(consumer.getChannelNumber(), bodyBuf);
            }
        }
    }
}
