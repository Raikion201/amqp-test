package com.amqp.exchange;

import com.amqp.model.Exchange;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Consistent Hash Exchange - distributes messages across queues using consistent hashing.
 * This provides better distribution than simple modulo-based hashing and handles
 * queue additions/removals more gracefully.
 */
public class ConsistentHashExchange extends Exchange {
    private static final int VIRTUAL_NODES = 100; // Virtual nodes per queue
    private final ConcurrentMap<String, Integer> queueWeights;
    private final TreeMap<Long, String> hashRing;
    private volatile boolean ringDirty = true;

    public ConsistentHashExchange(String name, boolean durable, boolean autoDelete, boolean internal) {
        super(name, Type.DIRECT, durable, autoDelete, internal); // Use DIRECT as base type
        this.queueWeights = new ConcurrentHashMap<>();
        this.hashRing = new TreeMap<>();
    }

    @Override
    public void addBinding(String routingKey, String queueName) {
        super.addBinding(routingKey, queueName);

        // Extract weight from routing key if present (format: "weight:10")
        int weight = extractWeight(routingKey);
        queueWeights.put(queueName, weight);
        ringDirty = true;
    }

    @Override
    public void removeBinding(String routingKey, String queueName) {
        super.removeBinding(routingKey, queueName);
        queueWeights.remove(queueName);
        ringDirty = true;
    }

    @Override
    public List<String> route(String routingKey) {
        if (ringDirty) {
            synchronized (this) {
                if (ringDirty) {
                    rebuildHashRing();
                    ringDirty = false;
                }
            }
        }

        if (hashRing.isEmpty()) {
            return Collections.emptyList();
        }

        // Hash the routing key
        long hash = hash(routingKey);

        // Find the queue in the hash ring
        Map.Entry<Long, String> entry = hashRing.ceilingEntry(hash);
        if (entry == null) {
            // Wrap around to the first entry
            entry = hashRing.firstEntry();
        }

        return Collections.singletonList(entry.getValue());
    }

    /**
     * Rebuild the consistent hash ring with virtual nodes.
     */
    private void rebuildHashRing() {
        hashRing.clear();

        for (Map.Entry<String, Integer> entry : queueWeights.entrySet()) {
            String queueName = entry.getKey();
            int weight = entry.getValue();

            // Create virtual nodes based on weight
            int virtualNodes = VIRTUAL_NODES * weight;
            for (int i = 0; i < virtualNodes; i++) {
                String virtualNodeKey = queueName + ":" + i;
                long hash = hash(virtualNodeKey);
                hashRing.put(hash, queueName);
            }
        }
    }

    /**
     * Extract weight from routing key.
     * Format: "weight:10" or just "10" (defaults to 1 if not specified)
     */
    private int extractWeight(String routingKey) {
        if (routingKey == null || routingKey.isEmpty()) {
            return 1;
        }

        try {
            if (routingKey.startsWith("weight:")) {
                return Integer.parseInt(routingKey.substring(7));
            }
            return Integer.parseInt(routingKey);
        } catch (NumberFormatException e) {
            return 1; // Default weight
        }
    }

    /**
     * Hash function using MD5 (consistent with RabbitMQ implementation).
     */
    private long hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));

            // Convert first 8 bytes to long
            long hash = 0;
            for (int i = 0; i < 8 && i < digest.length; i++) {
                hash = (hash << 8) | (digest[i] & 0xFF);
            }
            return hash;
        } catch (NoSuchAlgorithmException e) {
            // Fallback to simple hash code
            return key.hashCode();
        }
    }

    /**
     * Get current distribution statistics.
     */
    public Map<String, Integer> getDistributionStats() {
        Map<String, Integer> stats = new HashMap<>();
        for (String queue : hashRing.values()) {
            stats.put(queue, stats.getOrDefault(queue, 0) + 1);
        }
        return stats;
    }

    @Override
    public String toString() {
        return String.format("ConsistentHashExchange{name='%s', queues=%d, virtualNodes=%d}",
                           getName(), queueWeights.size(), hashRing.size());
    }
}
