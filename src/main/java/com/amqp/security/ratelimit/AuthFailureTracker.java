package com.amqp.security.ratelimit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks authentication failures and implements backoff.
 *
 * After a certain number of failures, the IP/user is temporarily blocked.
 */
public class AuthFailureTracker {

    private static final Logger log = LoggerFactory.getLogger(AuthFailureTracker.class);

    private final int maxFailures;
    private final long lockoutDurationMs;
    private final long failureWindowMs;

    private final Map<String, FailureRecord> ipFailures = new ConcurrentHashMap<>();
    private final Map<String, FailureRecord> userFailures = new ConcurrentHashMap<>();

    private final ScheduledExecutorService cleanupExecutor;

    public AuthFailureTracker(int maxFailures, long lockoutDurationMs, long failureWindowMs) {
        this.maxFailures = maxFailures;
        this.lockoutDurationMs = lockoutDurationMs;
        this.failureWindowMs = failureWindowMs;

        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "auth-failure-cleanup");
            t.setDaemon(true);
            return t;
        });
        this.cleanupExecutor.scheduleAtFixedRate(this::cleanup, 1, 1, TimeUnit.MINUTES);
    }

    /**
     * Check if an IP address is currently locked out.
     */
    public boolean isIpLocked(String ipAddress) {
        FailureRecord record = ipFailures.get(ipAddress);
        if (record != null && record.isLocked()) {
            log.debug("IP {} is locked out", ipAddress);
            return true;
        }
        return false;
    }

    /**
     * Check if a user is currently locked out.
     */
    public boolean isUserLocked(String username) {
        FailureRecord record = userFailures.get(username);
        if (record != null && record.isLocked()) {
            log.debug("User {} is locked out", username);
            return true;
        }
        return false;
    }

    /**
     * Check if either IP or user is locked.
     */
    public boolean isLocked(String ipAddress, String username) {
        return isIpLocked(ipAddress) || (username != null && isUserLocked(username));
    }

    /**
     * Record a failed authentication attempt.
     *
     * @return true if the IP/user is now locked out
     */
    public boolean recordFailure(String ipAddress, String username) {
        long now = System.currentTimeMillis();
        boolean locked = false;

        // Record IP failure
        FailureRecord ipRecord = ipFailures.computeIfAbsent(ipAddress, k -> new FailureRecord());
        if (ipRecord.recordFailure(now, failureWindowMs, maxFailures, lockoutDurationMs)) {
            log.warn("IP {} locked out after {} failures", ipAddress, maxFailures);
            locked = true;
        }

        // Record user failure
        if (username != null && !username.isEmpty()) {
            FailureRecord userRecord = userFailures.computeIfAbsent(username, k -> new FailureRecord());
            if (userRecord.recordFailure(now, failureWindowMs, maxFailures, lockoutDurationMs)) {
                log.warn("User {} locked out after {} failures", username, maxFailures);
                locked = true;
            }
        }

        return locked;
    }

    /**
     * Record a successful authentication (resets failure count).
     */
    public void recordSuccess(String ipAddress, String username) {
        ipFailures.remove(ipAddress);
        if (username != null) {
            userFailures.remove(username);
        }
    }

    /**
     * Get the remaining lockout time for an IP.
     *
     * @return remaining time in milliseconds, or 0 if not locked
     */
    public long getRemainingLockoutTime(String ipAddress) {
        FailureRecord record = ipFailures.get(ipAddress);
        if (record != null) {
            return record.getRemainingLockoutTime();
        }
        return 0;
    }

    /**
     * Get the failure count for an IP.
     */
    public int getFailureCount(String ipAddress) {
        FailureRecord record = ipFailures.get(ipAddress);
        return record != null ? record.getFailureCount() : 0;
    }

    /**
     * Manually unlock an IP.
     */
    public void unlockIp(String ipAddress) {
        ipFailures.remove(ipAddress);
        log.info("Manually unlocked IP {}", ipAddress);
    }

    /**
     * Manually unlock a user.
     */
    public void unlockUser(String username) {
        userFailures.remove(username);
        log.info("Manually unlocked user {}", username);
    }

    private void cleanup() {
        long now = System.currentTimeMillis();

        // Remove expired lockouts and old failure records
        ipFailures.entrySet().removeIf(e ->
                !e.getValue().isLocked() && e.getValue().isExpired(now, failureWindowMs));
        userFailures.entrySet().removeIf(e ->
                !e.getValue().isLocked() && e.getValue().isExpired(now, failureWindowMs));
    }

    public void shutdown() {
        cleanupExecutor.shutdown();
    }

    /**
     * Failure record for a single IP or user.
     */
    private static class FailureRecord {
        private final AtomicInteger failureCount = new AtomicInteger(0);
        private volatile long firstFailureTime = 0;
        private volatile long lockoutUntil = 0;

        public synchronized boolean recordFailure(long now, long windowMs, int maxFailures, long lockoutMs) {
            // Reset if outside failure window
            if (firstFailureTime > 0 && now - firstFailureTime > windowMs) {
                failureCount.set(0);
                firstFailureTime = 0;
            }

            // If locked, check if lockout expired
            if (lockoutUntil > 0 && now >= lockoutUntil) {
                lockoutUntil = 0;
                failureCount.set(0);
            }

            // Record failure
            if (firstFailureTime == 0) {
                firstFailureTime = now;
            }
            int count = failureCount.incrementAndGet();

            // Check if should lock
            if (count >= maxFailures && lockoutUntil == 0) {
                lockoutUntil = now + lockoutMs;
                return true;
            }

            return false;
        }

        public boolean isLocked() {
            return lockoutUntil > 0 && System.currentTimeMillis() < lockoutUntil;
        }

        public long getRemainingLockoutTime() {
            if (!isLocked()) {
                return 0;
            }
            return Math.max(0, lockoutUntil - System.currentTimeMillis());
        }

        public int getFailureCount() {
            return failureCount.get();
        }

        public boolean isExpired(long now, long windowMs) {
            return firstFailureTime > 0 && now - firstFailureTime > windowMs * 2;
        }
    }

    /**
     * Builder for AuthFailureTracker.
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int maxFailures = 5;
        private long lockoutDurationMs = 300_000; // 5 minutes
        private long failureWindowMs = 60_000; // 1 minute

        public Builder maxFailures(int max) {
            this.maxFailures = max;
            return this;
        }

        public Builder lockoutDuration(long ms) {
            this.lockoutDurationMs = ms;
            return this;
        }

        public Builder failureWindow(long ms) {
            this.failureWindowMs = ms;
            return this;
        }

        public AuthFailureTracker build() {
            return new AuthFailureTracker(maxFailures, lockoutDurationMs, failureWindowMs);
        }
    }
}
