package com.amqp.model;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Queue {
    private final String name;
    private final boolean durable;
    private final boolean exclusive;
    private final boolean autoDelete;
    private final BlockingQueue<Message> messages;
    private final int maxLength;
    private final Map<String, Object> arguments;

    // Default max queue length (100k messages) to prevent memory exhaustion
    private static final int DEFAULT_MAX_LENGTH = 100000;

    public Queue(String name, boolean durable, boolean exclusive, boolean autoDelete) {
        this(name, durable, exclusive, autoDelete, null);
    }

    public Queue(String name, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) {
        this.name = name;
        this.durable = durable;
        this.exclusive = exclusive;
        this.autoDelete = autoDelete;
        this.arguments = arguments;

        // Check for x-max-length argument
        int configuredMaxLength = DEFAULT_MAX_LENGTH;
        if (arguments != null && arguments.containsKey("x-max-length")) {
            Object maxLenArg = arguments.get("x-max-length");
            if (maxLenArg instanceof Number) {
                configuredMaxLength = ((Number) maxLenArg).intValue();
            }
        }
        this.maxLength = configuredMaxLength;
        this.messages = new LinkedBlockingQueue<>(maxLength);
    }
    
    public String getName() {
        return name;
    }
    
    public boolean isDurable() {
        return durable;
    }
    
    public boolean isExclusive() {
        return exclusive;
    }
    
    public boolean isAutoDelete() {
        return autoDelete;
    }
    
    public void enqueue(Message message) {
        // Try non-blocking add first
        if (!messages.offer(message)) {
            // Queue is full - drop the oldest message (head-drop policy like RabbitMQ)
            messages.poll();
            messages.offer(message);
        }
    }

    /**
     * Try to enqueue a message without blocking.
     * @return true if the message was added, false if the queue is full
     */
    public boolean tryEnqueue(Message message) {
        return messages.offer(message);
    }

    public int getMaxLength() {
        return maxLength;
    }

    public Map<String, Object> getQueueArguments() {
        return arguments;
    }
    
    public Message dequeue() {
        return messages.poll();
    }
    
    public Message dequeueBlocking() throws InterruptedException {
        return messages.take();
    }
    
    public int size() {
        return messages.size();
    }
    
    public boolean isEmpty() {
        return messages.isEmpty();
    }
    
    public void purge() {
        messages.clear();
    }

    /**
     * Clear all messages from the queue (alias for purge).
     */
    public void clear() {
        messages.clear();
    }
    
    @Override
    public String toString() {
        return String.format("Queue{name='%s', durable=%s, exclusive=%s, autoDelete=%s, size=%d}",
                name, durable, exclusive, autoDelete, size());
    }
}