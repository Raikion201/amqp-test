package com.amqp.model;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Queue {
    private final String name;
    private final boolean durable;
    private final boolean exclusive;
    private final boolean autoDelete;
    private final BlockingQueue<Message> messages;
    
    public Queue(String name, boolean durable, boolean exclusive, boolean autoDelete) {
        this.name = name;
        this.durable = durable;
        this.exclusive = exclusive;
        this.autoDelete = autoDelete;
        this.messages = new LinkedBlockingQueue<>();
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
        try {
            messages.put(message);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to enqueue message", e);
        }
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
    
    @Override
    public String toString() {
        return String.format("Queue{name='%s', durable=%s, exclusive=%s, autoDelete=%s, size=%d}",
                name, durable, exclusive, autoDelete, size());
    }
}