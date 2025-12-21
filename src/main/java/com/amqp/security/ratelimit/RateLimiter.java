package com.amqp.security.ratelimit;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Token bucket rate limiter.
 *
 * Implements the token bucket algorithm for rate limiting.
 */
public class RateLimiter {

    private final long capacity;
    private final double refillRate; // tokens per nanosecond
    private final AtomicLong tokens;
    private volatile long lastRefillTime;
    private final Object lock = new Object();

    /**
     * Create a rate limiter.
     *
     * @param capacity maximum number of tokens (burst size)
     * @param refillPerSecond tokens added per second
     */
    public RateLimiter(long capacity, double refillPerSecond) {
        this.capacity = capacity;
        this.refillRate = refillPerSecond / 1_000_000_000.0; // Convert to per nanosecond
        this.tokens = new AtomicLong(capacity);
        this.lastRefillTime = System.nanoTime();
    }

    /**
     * Try to acquire a token.
     *
     * @return true if token was acquired, false if rate limited
     */
    public boolean tryAcquire() {
        return tryAcquire(1);
    }

    /**
     * Try to acquire multiple tokens.
     *
     * @param count number of tokens to acquire
     * @return true if tokens were acquired, false if rate limited
     */
    public boolean tryAcquire(long count) {
        synchronized (lock) {
            refill();

            long current = tokens.get();
            if (current >= count) {
                tokens.addAndGet(-count);
                return true;
            }
            return false;
        }
    }

    /**
     * Acquire a token, blocking if necessary.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    public void acquire() throws InterruptedException {
        acquire(1);
    }

    /**
     * Acquire multiple tokens, blocking if necessary.
     *
     * @param count number of tokens to acquire
     * @throws InterruptedException if interrupted while waiting
     */
    public void acquire(long count) throws InterruptedException {
        while (!tryAcquire(count)) {
            // Calculate wait time for tokens to refill
            long needed = count - tokens.get();
            long waitNanos = (long) (needed / refillRate);
            long waitMillis = waitNanos / 1_000_000;
            int waitNanosRemainder = (int) (waitNanos % 1_000_000);

            Thread.sleep(waitMillis, waitNanosRemainder);
        }
    }

    private void refill() {
        long now = System.nanoTime();
        long elapsed = now - lastRefillTime;

        if (elapsed > 0) {
            long newTokens = (long) (elapsed * refillRate);
            if (newTokens > 0) {
                long current = tokens.get();
                long updated = Math.min(capacity, current + newTokens);
                tokens.set(updated);
                lastRefillTime = now;
            }
        }
    }

    /**
     * Get the current number of available tokens.
     */
    public long getAvailableTokens() {
        synchronized (lock) {
            refill();
            return tokens.get();
        }
    }

    /**
     * Get the capacity (maximum tokens).
     */
    public long getCapacity() {
        return capacity;
    }

    /**
     * Reset the limiter to full capacity.
     */
    public void reset() {
        synchronized (lock) {
            tokens.set(capacity);
            lastRefillTime = System.nanoTime();
        }
    }
}
