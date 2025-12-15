package com.amqp.security;

import com.amqp.model.Exchange;
import com.amqp.model.Queue;
import com.amqp.model.Message;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.List;
import java.util.ArrayList;

public class VirtualHost {
    private final String name;
    private final ConcurrentMap<String, Exchange> exchanges;
    private final ConcurrentMap<String, Queue> queues;
    private final long createdAt;
    private volatile boolean active;
    private volatile boolean tracing;

    // Resource limits
    private volatile int maxConnections = -1; // -1 means unlimited
    private volatile int maxQueues = -1;
    private volatile int maxExchanges = -1;
    private volatile long maxMemory = -1; // in bytes

    public VirtualHost(String name) {
        this.name = name;
        this.exchanges = new ConcurrentHashMap<>();
        this.queues = new ConcurrentHashMap<>();
        this.createdAt = System.currentTimeMillis();
        this.active = true;
        this.tracing = false;

        initializeDefaultExchanges();
    }

    private void initializeDefaultExchanges() {
        Exchange defaultExchange = new Exchange("", Exchange.Type.DIRECT, true, false, false);
        Exchange directExchange = new Exchange("amq.direct", Exchange.Type.DIRECT, true, false, false);
        Exchange topicExchange = new Exchange("amq.topic", Exchange.Type.TOPIC, true, false, false);
        Exchange fanoutExchange = new Exchange("amq.fanout", Exchange.Type.FANOUT, true, false, false);
        Exchange headersExchange = new Exchange("amq.headers", Exchange.Type.HEADERS, true, false, false);

        exchanges.put("", defaultExchange);
        exchanges.put("amq.direct", directExchange);
        exchanges.put("amq.topic", topicExchange);
        exchanges.put("amq.fanout", fanoutExchange);
        exchanges.put("amq.headers", headersExchange);
    }

    public String getName() {
        return name;
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

    public boolean isTracing() {
        return tracing;
    }

    public void setTracing(boolean tracing) {
        this.tracing = tracing;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public int getMaxQueues() {
        return maxQueues;
    }

    public void setMaxQueues(int maxQueues) {
        this.maxQueues = maxQueues;
    }

    public int getMaxExchanges() {
        return maxExchanges;
    }

    public void setMaxExchanges(int maxExchanges) {
        this.maxExchanges = maxExchanges;
    }

    public long getMaxMemory() {
        return maxMemory;
    }

    public void setMaxMemory(long maxMemory) {
        this.maxMemory = maxMemory;
    }

    public Exchange addExchange(Exchange exchange) {
        if (maxExchanges > 0 && exchanges.size() >= maxExchanges) {
            throw new IllegalStateException("Maximum number of exchanges reached for vhost: " + name);
        }
        return exchanges.put(exchange.getName(), exchange);
    }

    public Exchange getExchange(String name) {
        return exchanges.get(name);
    }

    public Exchange removeExchange(String name) {
        return exchanges.remove(name);
    }

    public List<Exchange> getAllExchanges() {
        return new ArrayList<>(exchanges.values());
    }

    public Queue addQueue(Queue queue) {
        if (maxQueues > 0 && queues.size() >= maxQueues) {
            throw new IllegalStateException("Maximum number of queues reached for vhost: " + name);
        }
        return queues.put(queue.getName(), queue);
    }

    public Queue getQueue(String name) {
        return queues.get(name);
    }

    public Queue removeQueue(String name) {
        Queue queue = queues.remove(name);

        // Properly cleanup QuorumQueue schedulers
        if (queue instanceof com.amqp.queue.QuorumQueue) {
            ((com.amqp.queue.QuorumQueue) queue).shutdown();
        }

        return queue;
    }

    public List<Queue> getAllQueues() {
        return new ArrayList<>(queues.values());
    }

    public int getExchangeCount() {
        return exchanges.size();
    }

    public int getQueueCount() {
        return queues.size();
    }

    @Override
    public String toString() {
        return String.format("VirtualHost{name='%s', exchanges=%d, queues=%d, active=%s}",
                           name, exchanges.size(), queues.size(), active);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VirtualHost that = (VirtualHost) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
