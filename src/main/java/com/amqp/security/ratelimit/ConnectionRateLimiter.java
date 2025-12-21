package com.amqp.security.ratelimit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Connection rate limiter.
 *
 * Limits the rate of new connections per IP address and globally.
 */
public class ConnectionRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(ConnectionRateLimiter.class);

    private final int globalRatePerSecond;
    private final int perIpRatePerSecond;
    private final int perIpBurst;
    private final int maxConcurrentPerIp;

    private final RateLimiter globalLimiter;
    private final Map<String, RateLimiter> perIpLimiters = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> connectionCounts = new ConcurrentHashMap<>();

    private final ScheduledExecutorService cleanupExecutor;

    public ConnectionRateLimiter(int globalRatePerSecond, int perIpRatePerSecond,
                                  int perIpBurst, int maxConcurrentPerIp) {
        this.globalRatePerSecond = globalRatePerSecond;
        this.perIpRatePerSecond = perIpRatePerSecond;
        this.perIpBurst = perIpBurst;
        this.maxConcurrentPerIp = maxConcurrentPerIp;

        this.globalLimiter = new RateLimiter(globalRatePerSecond * 2, globalRatePerSecond);

        // Cleanup old entries periodically
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "connection-ratelimit-cleanup");
            t.setDaemon(true);
            return t;
        });
        this.cleanupExecutor.scheduleAtFixedRate(this::cleanup, 1, 1, TimeUnit.MINUTES);
    }

    /**
     * Check if a new connection should be allowed.
     *
     * @param ipAddress the client's IP address
     * @return true if allowed, false if rate limited
     */
    public boolean allowConnection(String ipAddress) {
        // Check global rate
        if (!globalLimiter.tryAcquire()) {
            log.warn("Global connection rate limit exceeded");
            return false;
        }

        // Check per-IP rate
        RateLimiter ipLimiter = perIpLimiters.computeIfAbsent(ipAddress,
                ip -> new RateLimiter(perIpBurst, perIpRatePerSecond));

        if (!ipLimiter.tryAcquire()) {
            log.warn("Per-IP connection rate limit exceeded for {}", ipAddress);
            return false;
        }

        // Check concurrent connections
        AtomicInteger count = connectionCounts.computeIfAbsent(ipAddress,
                ip -> new AtomicInteger(0));

        if (count.get() >= maxConcurrentPerIp) {
            log.warn("Max concurrent connections exceeded for {}", ipAddress);
            return false;
        }

        count.incrementAndGet();
        return true;
    }

    /**
     * Record that a connection was closed.
     *
     * @param ipAddress the client's IP address
     */
    public void connectionClosed(String ipAddress) {
        AtomicInteger count = connectionCounts.get(ipAddress);
        if (count != null) {
            count.decrementAndGet();
        }
    }

    /**
     * Get current connection count for an IP.
     */
    public int getConnectionCount(String ipAddress) {
        AtomicInteger count = connectionCounts.get(ipAddress);
        return count != null ? count.get() : 0;
    }

    /**
     * Get total concurrent connections.
     */
    public int getTotalConnections() {
        return connectionCounts.values().stream()
                .mapToInt(AtomicInteger::get)
                .sum();
    }

    private void cleanup() {
        // Remove entries for IPs with zero connections
        connectionCounts.entrySet().removeIf(e -> e.getValue().get() <= 0);

        // Remove old rate limiters for IPs that haven't connected recently
        // (they'll be recreated if needed)
        perIpLimiters.keySet().removeIf(ip -> !connectionCounts.containsKey(ip));
    }

    public void shutdown() {
        cleanupExecutor.shutdown();
    }

    /**
     * Builder for ConnectionRateLimiter.
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int globalRatePerSecond = 100;
        private int perIpRatePerSecond = 10;
        private int perIpBurst = 20;
        private int maxConcurrentPerIp = 50;

        public Builder globalRate(int ratePerSecond) {
            this.globalRatePerSecond = ratePerSecond;
            return this;
        }

        public Builder perIpRate(int ratePerSecond, int burst) {
            this.perIpRatePerSecond = ratePerSecond;
            this.perIpBurst = burst;
            return this;
        }

        public Builder maxConcurrentPerIp(int max) {
            this.maxConcurrentPerIp = max;
            return this;
        }

        public ConnectionRateLimiter build() {
            return new ConnectionRateLimiter(
                    globalRatePerSecond, perIpRatePerSecond, perIpBurst, maxConcurrentPerIp);
        }
    }
}
