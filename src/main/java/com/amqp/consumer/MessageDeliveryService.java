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
    public void startDelivery(Queue queue, AmqpConnection connection) {
        if (!running.get()) {
            return;
        }

        String queueName = queue.getName();
        queueWorkers.computeIfAbsent(queueName, k -> {
            DeliveryWorker worker = new DeliveryWorker(queue, connection);
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
        private final AmqpConnection connection;
        private final AtomicBoolean active = new AtomicBoolean(false);
        private final AtomicLong deliveryTag = new AtomicLong(0);
        private ScheduledFuture<?> scheduledFuture;

        DeliveryWorker(Queue queue, AmqpConnection connection) {
            this.queue = queue;
            this.connection = connection;
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
                logger.debug("Started delivery worker for queue: {}", queue.getName());
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

            if (consumers.isEmpty()) {
                return; // No consumers, skip
            }

            // Deliver messages in round-robin fashion to active consumers
            for (ConsumerManager.Consumer consumer : consumers) {
                if (!consumer.isActive()) {
                    continue;
                }

                // Check prefetch limit (simplified - should track per-channel)
                // For now, just deliver one message at a time

                Message message = queue.dequeue();
                if (message == null) {
                    break; // Queue empty
                }

                try {
                    long tag = deliveryTag.incrementAndGet();
                    deliverMessage(consumer, message, tag);
                } catch (Exception e) {
                    logger.error("Failed to deliver message to consumer: {}", consumer.getConsumerTag(), e);
                    // Re-queue the message
                    queue.enqueue(message);
                }
            }
        }

        private void deliverMessage(ConsumerManager.Consumer consumer, Message message, long workerDeliveryTag) {
            logger.debug("Delivering message to consumer: {}, workerDeliveryTag: {}",
                        consumer.getConsumerTag(), workerDeliveryTag);

            // Track delivery in the channel if not in noAck mode
            long channelDeliveryTag = workerDeliveryTag; // Default for noAck
            if (!consumer.isNoAck()) {
                com.amqp.connection.AmqpChannel channel = connection.getChannel(consumer.getChannelNumber());
                if (channel != null) {
                    channelDeliveryTag = channel.trackDelivery(
                        message, queue.getName(), connection.getVirtualHost(), false
                    );
                } else {
                    logger.warn("Channel not found for consumer: {}", consumer.getConsumerTag());
                    // Re-queue the message since we can't track it
                    queue.enqueue(message);
                    return;
                }
            }

            // Build Basic.Deliver frame
            ByteBuf deliverPayload = Unpooled.buffer();

            // Basic.Deliver method: class=60, method=60
            AmqpCodec.encodeShortString(deliverPayload, consumer.getConsumerTag());
            deliverPayload.writeLong(channelDeliveryTag); // Use channel's delivery tag
            AmqpCodec.encodeBoolean(deliverPayload, false); // redelivered
            AmqpCodec.encodeShortString(deliverPayload, ""); // exchange (not stored in message)
            AmqpCodec.encodeShortString(deliverPayload, message.getRoutingKey() != null ? message.getRoutingKey() : "");

            connection.sendMethod(consumer.getChannelNumber(), (short) 60, (short) 60, deliverPayload);

            // Send Content Header
            long bodySize = message.getBody() != null ? message.getBody().length : 0;
            ByteBuf headerPayload = Unpooled.buffer();

            // Property flags (simplified - would normally encode all properties)
            short propertyFlags = 0;
            Short deliveryMode = message.getDeliveryMode();
            Short priority = message.getPriority();
            Long timestamp = message.getTimestamp();

            if (message.getContentType() != null) propertyFlags |= (1 << 15);
            if (message.getContentEncoding() != null) propertyFlags |= (1 << 14);
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

            connection.sendContentHeader(consumer.getChannelNumber(), (short) 60, bodySize, headerPayload);

            // Send Content Body
            if (message.getBody() != null && message.getBody().length > 0) {
                ByteBuf bodyBuf = Unpooled.wrappedBuffer(message.getBody());
                connection.sendContentBody(consumer.getChannelNumber(), bodyBuf);
            }

            logger.debug("Message delivered to consumer: {}, deliveryTag: {}, noAck: {}",
                       consumer.getConsumerTag(), channelDeliveryTag, consumer.isNoAck());
        }
    }
}
