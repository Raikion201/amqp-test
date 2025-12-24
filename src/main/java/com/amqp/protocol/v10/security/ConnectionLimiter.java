package com.amqp.protocol.v10.security;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Connection limiter for AMQP 1.0 server.
 *
 * Enforces:
 * - Maximum connections per host
 * - Maximum total connections
 * - Connection rate limiting
 *
 * This handler should be added early in the pipeline to reject
 * connections before consuming significant resources.
 */
@ChannelHandler.Sharable
public class ConnectionLimiter extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(ConnectionLimiter.class);

    private final Amqp10SecurityConfig config;

    // Connection tracking
    private final AtomicInteger totalConnections = new AtomicInteger(0);
    private final Map<String, AtomicInteger> connectionsPerHost = new ConcurrentHashMap<>();

    // Rate limiting
    private final Map<String, RateLimiter> rateLimiters = new ConcurrentHashMap<>();

    public ConnectionLimiter(Amqp10SecurityConfig config) {
        this.config = config;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        String hostAddress = getRemoteHost(ctx);

        // Check total connection limit
        int total = totalConnections.incrementAndGet();
        if (total > config.getMaxTotalConnections()) {
            totalConnections.decrementAndGet();
            log.warn("Connection rejected from {}: max total connections ({}) exceeded",
                    hostAddress, config.getMaxTotalConnections());
            ctx.close();
            return;
        }

        // Check per-host connection limit
        AtomicInteger hostCount = connectionsPerHost.computeIfAbsent(
                hostAddress, k -> new AtomicInteger(0));
        int count = hostCount.incrementAndGet();
        if (count > config.getMaxConnectionsPerHost()) {
            hostCount.decrementAndGet();
            totalConnections.decrementAndGet();
            log.warn("Connection rejected from {}: max connections per host ({}) exceeded",
                    hostAddress, config.getMaxConnectionsPerHost());
            ctx.close();
            return;
        }

        // Check rate limit
        RateLimiter rateLimiter = rateLimiters.computeIfAbsent(
                hostAddress, k -> new RateLimiter(config.getConnectionRateLimitPerSecond()));
        if (!rateLimiter.tryAcquire()) {
            hostCount.decrementAndGet();
            totalConnections.decrementAndGet();
            log.warn("Connection rejected from {}: rate limit ({}/s) exceeded",
                    hostAddress, config.getConnectionRateLimitPerSecond());
            ctx.close();
            return;
        }

        log.debug("Connection accepted from {} (total: {}, from host: {})",
                hostAddress, total, count);

        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        String hostAddress = getRemoteHost(ctx);

        totalConnections.decrementAndGet();

        AtomicInteger hostCount = connectionsPerHost.get(hostAddress);
        if (hostCount != null) {
            int remaining = hostCount.decrementAndGet();
            if (remaining <= 0) {
                connectionsPerHost.remove(hostAddress);
            }
        }

        super.channelInactive(ctx);
    }

    private String getRemoteHost(ChannelHandlerContext ctx) {
        try {
            InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
            if (remoteAddress != null) {
                InetAddress addr = remoteAddress.getAddress();
                if (addr != null) {
                    return addr.getHostAddress();
                }
            }
        } catch (Exception e) {
            log.debug("Could not get remote host address", e);
        }
        return "unknown";
    }

    /**
     * Get current total connection count.
     */
    public int getTotalConnections() {
        return totalConnections.get();
    }

    /**
     * Get connection count for a specific host.
     */
    public int getConnectionsForHost(String hostAddress) {
        AtomicInteger count = connectionsPerHost.get(hostAddress);
        return count != null ? count.get() : 0;
    }

    /**
     * Simple token bucket rate limiter.
     */
    private static class RateLimiter {
        private final int tokensPerSecond;
        private final AtomicLong lastRefill = new AtomicLong(System.currentTimeMillis());
        private final AtomicInteger tokens;

        RateLimiter(int tokensPerSecond) {
            this.tokensPerSecond = tokensPerSecond;
            this.tokens = new AtomicInteger(tokensPerSecond);
        }

        boolean tryAcquire() {
            refill();
            return tokens.getAndUpdate(t -> t > 0 ? t - 1 : 0) > 0;
        }

        private void refill() {
            long now = System.currentTimeMillis();
            long lastRefillTime = lastRefill.get();
            long elapsed = now - lastRefillTime;

            if (elapsed >= 1000) {
                // Refill tokens for each second elapsed
                int tokensToAdd = (int) (elapsed / 1000) * tokensPerSecond;
                if (lastRefill.compareAndSet(lastRefillTime, now)) {
                    tokens.updateAndGet(t -> Math.min(t + tokensToAdd, tokensPerSecond * 2));
                }
            }
        }
    }

    /**
     * Statistics snapshot.
     */
    public static class Stats {
        public final int totalConnections;
        public final int uniqueHosts;
        public final Map<String, Integer> connectionsByHost;

        Stats(int totalConnections, Map<String, AtomicInteger> connectionsPerHost) {
            this.totalConnections = totalConnections;
            this.uniqueHosts = connectionsPerHost.size();
            this.connectionsByHost = new ConcurrentHashMap<>();
            connectionsPerHost.forEach((host, count) ->
                    connectionsByHost.put(host, count.get()));
        }
    }

    /**
     * Get current statistics.
     */
    public Stats getStats() {
        return new Stats(totalConnections.get(), connectionsPerHost);
    }
}
