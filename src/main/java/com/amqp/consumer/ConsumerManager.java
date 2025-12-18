package com.amqp.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConsumerManager {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerManager.class);

    private final ConcurrentMap<String, Consumer> consumers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<String>> queueConsumers = new ConcurrentHashMap<>();

    public static class Consumer {
        private final String consumerTag;
        private final String queueName;
        private final short channelNumber;
        private final boolean noAck;
        private final boolean exclusive;
        private final Map<String, Object> arguments;
        private final long createdAt;
        private volatile boolean active;
        private final Object connection; // Reference to AmqpConnection (stored as Object to avoid circular dependency)

        public Consumer(String consumerTag, String queueName, short channelNumber,
                       boolean noAck, boolean exclusive, Map<String, Object> arguments,
                       Object connection) {
            this.consumerTag = consumerTag;
            this.queueName = queueName;
            this.channelNumber = channelNumber;
            this.noAck = noAck;
            this.exclusive = exclusive;
            this.arguments = arguments != null ? new HashMap<>(arguments) : new HashMap<>();
            this.createdAt = System.currentTimeMillis();
            this.active = true;
            this.connection = connection;
        }

        public String getConsumerTag() {
            return consumerTag;
        }

        public String getQueueName() {
            return queueName;
        }

        public short getChannelNumber() {
            return channelNumber;
        }

        public boolean isNoAck() {
            return noAck;
        }

        public boolean isExclusive() {
            return exclusive;
        }

        public Map<String, Object> getArguments() {
            return new HashMap<>(arguments);
        }

        public long getCreatedAt() {
            return createdAt;
        }

        public boolean isActive() {
            return active;
        }

        public void setActive(boolean active) {
            this.active = active;
        }

        public Object getConnection() {
            return connection;
        }

        @Override
        public String toString() {
            return String.format("Consumer{tag='%s', queue='%s', channel=%d, exclusive=%s}",
                               consumerTag, queueName, channelNumber, exclusive);
        }
    }

    public Consumer addConsumer(String consumerTag, String queueName, short channelNumber,
                               boolean noAck, boolean exclusive, Map<String, Object> arguments,
                               Object connection) {
        // Check if queue already has exclusive consumer
        Set<String> existingConsumers = queueConsumers.get(queueName);
        if (existingConsumers != null) {
            for (String existingTag : existingConsumers) {
                Consumer existing = consumers.get(existingTag);
                if (existing != null && existing.isExclusive()) {
                    throw new IllegalStateException("Queue has exclusive consumer: " + queueName);
                }
            }
        }

        if (exclusive && existingConsumers != null && !existingConsumers.isEmpty()) {
            throw new IllegalStateException("Cannot create exclusive consumer - queue already has consumers: " + queueName);
        }

        Consumer consumer = new Consumer(consumerTag, queueName, channelNumber, noAck, exclusive, arguments, connection);
        consumers.put(consumerTag, consumer);

        queueConsumers.computeIfAbsent(queueName, k -> ConcurrentHashMap.newKeySet()).add(consumerTag);

        logger.info("Added consumer: {}", consumer);
        return consumer;
    }

    public Consumer removeConsumer(String consumerTag) {
        Consumer consumer = consumers.remove(consumerTag);
        if (consumer != null) {
            Set<String> tags = queueConsumers.get(consumer.getQueueName());
            if (tags != null) {
                tags.remove(consumerTag);
                if (tags.isEmpty()) {
                    queueConsumers.remove(consumer.getQueueName());
                }
            }
            logger.info("Removed consumer: {}", consumer);
        }
        return consumer;
    }

    public Consumer getConsumer(String consumerTag) {
        return consumers.get(consumerTag);
    }

    public Set<Consumer> getConsumersForQueue(String queueName) {
        Set<String> tags = queueConsumers.get(queueName);
        if (tags == null) {
            return Collections.emptySet();
        }

        Set<Consumer> result = new HashSet<>();
        for (String tag : tags) {
            Consumer consumer = consumers.get(tag);
            if (consumer != null) {
                result.add(consumer);
            }
        }
        return result;
    }

    public Set<Consumer> getConsumersForChannel(short channelNumber) {
        Set<Consumer> result = new HashSet<>();
        for (Consumer consumer : consumers.values()) {
            if (consumer.getChannelNumber() == channelNumber) {
                result.add(consumer);
            }
        }
        return result;
    }

    public List<Consumer> getAllConsumers() {
        return new ArrayList<>(consumers.values());
    }

    public void cancelConsumersForQueue(String queueName, CancelListener listener) {
        Set<String> tags = queueConsumers.get(queueName);
        if (tags != null) {
            for (String tag : new ArrayList<>(tags)) {
                Consumer consumer = removeConsumer(tag);
                if (consumer != null && listener != null) {
                    listener.onConsumerCancelled(consumer, "Queue deleted or unavailable");
                }
            }
        }
    }

    public void cancelConsumersForChannel(short channelNumber) {
        Set<Consumer> channelConsumers = getConsumersForChannel(channelNumber);
        for (Consumer consumer : channelConsumers) {
            removeConsumer(consumer.getConsumerTag());
        }
    }

    public int getConsumerCount() {
        return consumers.size();
    }

    public int getConsumerCountForQueue(String queueName) {
        Set<String> tags = queueConsumers.get(queueName);
        return tags != null ? tags.size() : 0;
    }

    /**
     * Check if a queue has any active consumers.
     */
    public boolean hasConsumersForQueue(String queueName) {
        Set<String> tags = queueConsumers.get(queueName);
        return tags != null && !tags.isEmpty();
    }

    /**
     * Remove all consumers for a queue.
     */
    public void removeConsumersForQueue(String queueName) {
        Set<String> tags = queueConsumers.get(queueName);
        if (tags != null) {
            for (String tag : new ArrayList<>(tags)) {
                removeConsumer(tag);
            }
        }
    }

    public interface CancelListener {
        void onConsumerCancelled(Consumer consumer, String reason);
    }
}
