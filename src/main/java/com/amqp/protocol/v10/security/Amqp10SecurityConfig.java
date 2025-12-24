package com.amqp.protocol.v10.security;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Security configuration for AMQP 1.0 connections.
 *
 * Provides centralized security settings including:
 * - TLS requirements
 * - Connection limits
 * - Message size limits
 * - Rate limiting
 * - Authentication requirements
 */
public class Amqp10SecurityConfig {

    // TLS settings
    private boolean requireTlsForPlainAuth = true;
    private boolean requireTlsForAllConnections = false;

    // Connection limits
    private int maxConnectionsPerHost = 100;
    private int maxTotalConnections = 10000;
    private int connectionRateLimitPerSecond = 50;

    // Session limits
    private int maxSessionsPerConnection = 64;
    private int maxLinksPerSession = 256;

    // Message limits
    private long maxMessageSize = 64 * 1024 * 1024; // 64 MB default
    private long maxFrameSize = 1024 * 1024; // 1 MB default
    private int maxMultiFrameAssemblyTime = 30000; // 30 seconds

    // Transaction limits
    private int maxTransactionsPerSession = 100;
    private long transactionTimeoutMs = 300000; // 5 minutes

    // Flow control
    private long defaultSessionWindow = 2048;
    private long defaultLinkCredit = 100;
    private boolean enforceFlowWindows = true;

    // Idle timeout
    private long minIdleTimeout = 10000; // 10 seconds minimum
    private long maxIdleTimeout = 600000; // 10 minutes maximum
    private int missedHeartbeatsBeforeClose = 2;

    // Authentication
    private boolean requireAuthentication = true;
    private boolean allowAnonymous = false;
    private Set<String> allowedSaslMechanisms = ConcurrentHashMap.newKeySet();

    public Amqp10SecurityConfig() {
        // Default allowed mechanisms
        allowedSaslMechanisms.add("PLAIN");
        allowedSaslMechanisms.add("SCRAM-SHA-256");
        allowedSaslMechanisms.add("EXTERNAL");
    }

    // Builder pattern for fluent configuration
    public static Amqp10SecurityConfig defaults() {
        return new Amqp10SecurityConfig();
    }

    public static Amqp10SecurityConfig production() {
        Amqp10SecurityConfig config = new Amqp10SecurityConfig();
        config.requireTlsForPlainAuth = true;
        config.requireAuthentication = true;
        config.allowAnonymous = false;
        config.enforceFlowWindows = true;
        return config;
    }

    public static Amqp10SecurityConfig development() {
        Amqp10SecurityConfig config = new Amqp10SecurityConfig();
        config.requireTlsForPlainAuth = false;
        config.requireAuthentication = false;
        config.allowAnonymous = true;
        config.maxConnectionsPerHost = 1000;
        return config;
    }

    // TLS getters/setters
    public boolean isRequireTlsForPlainAuth() {
        return requireTlsForPlainAuth;
    }

    public Amqp10SecurityConfig setRequireTlsForPlainAuth(boolean requireTlsForPlainAuth) {
        this.requireTlsForPlainAuth = requireTlsForPlainAuth;
        return this;
    }

    public boolean isRequireTlsForAllConnections() {
        return requireTlsForAllConnections;
    }

    public Amqp10SecurityConfig setRequireTlsForAllConnections(boolean requireTlsForAllConnections) {
        this.requireTlsForAllConnections = requireTlsForAllConnections;
        return this;
    }

    // Connection limits getters/setters
    public int getMaxConnectionsPerHost() {
        return maxConnectionsPerHost;
    }

    public Amqp10SecurityConfig setMaxConnectionsPerHost(int maxConnectionsPerHost) {
        this.maxConnectionsPerHost = maxConnectionsPerHost;
        return this;
    }

    public int getMaxTotalConnections() {
        return maxTotalConnections;
    }

    public Amqp10SecurityConfig setMaxTotalConnections(int maxTotalConnections) {
        this.maxTotalConnections = maxTotalConnections;
        return this;
    }

    public int getConnectionRateLimitPerSecond() {
        return connectionRateLimitPerSecond;
    }

    public Amqp10SecurityConfig setConnectionRateLimitPerSecond(int connectionRateLimitPerSecond) {
        this.connectionRateLimitPerSecond = connectionRateLimitPerSecond;
        return this;
    }

    // Session limits
    public int getMaxSessionsPerConnection() {
        return maxSessionsPerConnection;
    }

    public Amqp10SecurityConfig setMaxSessionsPerConnection(int maxSessionsPerConnection) {
        this.maxSessionsPerConnection = maxSessionsPerConnection;
        return this;
    }

    public int getMaxLinksPerSession() {
        return maxLinksPerSession;
    }

    public Amqp10SecurityConfig setMaxLinksPerSession(int maxLinksPerSession) {
        this.maxLinksPerSession = maxLinksPerSession;
        return this;
    }

    // Message limits
    public long getMaxMessageSize() {
        return maxMessageSize;
    }

    public Amqp10SecurityConfig setMaxMessageSize(long maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
        return this;
    }

    public long getMaxFrameSize() {
        return maxFrameSize;
    }

    public Amqp10SecurityConfig setMaxFrameSize(long maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
        return this;
    }

    public int getMaxMultiFrameAssemblyTime() {
        return maxMultiFrameAssemblyTime;
    }

    public Amqp10SecurityConfig setMaxMultiFrameAssemblyTime(int maxMultiFrameAssemblyTime) {
        this.maxMultiFrameAssemblyTime = maxMultiFrameAssemblyTime;
        return this;
    }

    // Transaction limits
    public int getMaxTransactionsPerSession() {
        return maxTransactionsPerSession;
    }

    public Amqp10SecurityConfig setMaxTransactionsPerSession(int maxTransactionsPerSession) {
        this.maxTransactionsPerSession = maxTransactionsPerSession;
        return this;
    }

    public long getTransactionTimeoutMs() {
        return transactionTimeoutMs;
    }

    public Amqp10SecurityConfig setTransactionTimeoutMs(long transactionTimeoutMs) {
        this.transactionTimeoutMs = transactionTimeoutMs;
        return this;
    }

    // Flow control
    public long getDefaultSessionWindow() {
        return defaultSessionWindow;
    }

    public Amqp10SecurityConfig setDefaultSessionWindow(long defaultSessionWindow) {
        this.defaultSessionWindow = defaultSessionWindow;
        return this;
    }

    public long getDefaultLinkCredit() {
        return defaultLinkCredit;
    }

    public Amqp10SecurityConfig setDefaultLinkCredit(long defaultLinkCredit) {
        this.defaultLinkCredit = defaultLinkCredit;
        return this;
    }

    public boolean isEnforceFlowWindows() {
        return enforceFlowWindows;
    }

    public Amqp10SecurityConfig setEnforceFlowWindows(boolean enforceFlowWindows) {
        this.enforceFlowWindows = enforceFlowWindows;
        return this;
    }

    // Idle timeout
    public long getMinIdleTimeout() {
        return minIdleTimeout;
    }

    public Amqp10SecurityConfig setMinIdleTimeout(long minIdleTimeout) {
        this.minIdleTimeout = minIdleTimeout;
        return this;
    }

    public long getMaxIdleTimeout() {
        return maxIdleTimeout;
    }

    public Amqp10SecurityConfig setMaxIdleTimeout(long maxIdleTimeout) {
        this.maxIdleTimeout = maxIdleTimeout;
        return this;
    }

    public int getMissedHeartbeatsBeforeClose() {
        return missedHeartbeatsBeforeClose;
    }

    public Amqp10SecurityConfig setMissedHeartbeatsBeforeClose(int missedHeartbeatsBeforeClose) {
        this.missedHeartbeatsBeforeClose = missedHeartbeatsBeforeClose;
        return this;
    }

    // Authentication
    public boolean isRequireAuthentication() {
        return requireAuthentication;
    }

    public Amqp10SecurityConfig setRequireAuthentication(boolean requireAuthentication) {
        this.requireAuthentication = requireAuthentication;
        return this;
    }

    public boolean isAllowAnonymous() {
        return allowAnonymous;
    }

    public Amqp10SecurityConfig setAllowAnonymous(boolean allowAnonymous) {
        this.allowAnonymous = allowAnonymous;
        return this;
    }

    public Set<String> getAllowedSaslMechanisms() {
        return allowedSaslMechanisms;
    }

    public Amqp10SecurityConfig addAllowedMechanism(String mechanism) {
        allowedSaslMechanisms.add(mechanism);
        return this;
    }

    public Amqp10SecurityConfig removeAllowedMechanism(String mechanism) {
        allowedSaslMechanisms.remove(mechanism);
        return this;
    }

    /**
     * Validate idle timeout against configured limits.
     */
    public long validateIdleTimeout(long requestedTimeout) {
        if (requestedTimeout == 0) {
            return 0; // No timeout
        }
        if (requestedTimeout < minIdleTimeout) {
            return minIdleTimeout;
        }
        if (requestedTimeout > maxIdleTimeout) {
            return maxIdleTimeout;
        }
        return requestedTimeout;
    }

    @Override
    public String toString() {
        return String.format(
            "Amqp10SecurityConfig{requireTlsForPlain=%s, maxConnPerHost=%d, maxMsgSize=%d, " +
            "txnTimeout=%dms, enforceFlowWindows=%s, requireAuth=%s}",
            requireTlsForPlainAuth, maxConnectionsPerHost, maxMessageSize,
            transactionTimeoutMs, enforceFlowWindows, requireAuthentication
        );
    }
}
