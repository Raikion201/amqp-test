package com.amqp.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class AuthenticationManager {
    private static final Logger logger = LoggerFactory.getLogger(AuthenticationManager.class);

    // Rate limiting constants
    private static final int MAX_FAILED_ATTEMPTS = 5;
    private static final long LOCKOUT_DURATION_MS = 300000; // 5 minutes
    private static final long FAILED_ATTEMPT_WINDOW_MS = 60000; // 1 minute

    private final ConcurrentMap<String, User> users;
    private final ConcurrentMap<String, VirtualHost> virtualHosts;
    private final ConcurrentMap<String, FailedLoginTracker> failedLogins;
    private final boolean enableDefaultGuest;

    /**
     * Tracks failed login attempts for brute-force protection.
     */
    private static class FailedLoginTracker {
        private int failedAttempts = 0;
        private long lastFailedAttempt = 0;
        private long lockedUntil = 0;

        synchronized boolean isLockedOut() {
            if (lockedUntil > 0 && System.currentTimeMillis() < lockedUntil) {
                return true;
            }
            // Reset if lockout expired
            if (lockedUntil > 0 && System.currentTimeMillis() >= lockedUntil) {
                reset();
            }
            return false;
        }

        synchronized void recordFailure() {
            long now = System.currentTimeMillis();
            // Reset count if outside the window
            if (now - lastFailedAttempt > FAILED_ATTEMPT_WINDOW_MS) {
                failedAttempts = 0;
            }
            failedAttempts++;
            lastFailedAttempt = now;

            if (failedAttempts >= MAX_FAILED_ATTEMPTS) {
                // Exponential backoff: 5 min, 10 min, 20 min, etc.
                long multiplier = Math.min(failedAttempts - MAX_FAILED_ATTEMPTS + 1, 4);
                lockedUntil = now + (LOCKOUT_DURATION_MS * multiplier);
                logger.warn("SECURITY: Account locked due to {} failed login attempts. Locked until {}ms from now",
                        failedAttempts, lockedUntil - now);
            }
        }

        synchronized void reset() {
            failedAttempts = 0;
            lastFailedAttempt = 0;
            lockedUntil = 0;
        }

        synchronized long getRemainingLockoutMs() {
            if (lockedUntil > 0) {
                long remaining = lockedUntil - System.currentTimeMillis();
                return Math.max(0, remaining);
            }
            return 0;
        }
    }

    public AuthenticationManager() {
        this(false); // Default: guest user DISABLED for security
    }

    public AuthenticationManager(boolean enableDefaultGuest) {
        this.users = new ConcurrentHashMap<>();
        this.virtualHosts = new ConcurrentHashMap<>();
        this.failedLogins = new ConcurrentHashMap<>();
        this.enableDefaultGuest = enableDefaultGuest;

        initializeDefaults();
    }

    private void initializeDefaults() {
        // Create default virtual host
        VirtualHost defaultVhost = new VirtualHost("/");
        virtualHosts.put("/", defaultVhost);

        if (enableDefaultGuest) {
            // Create default guest user for development/testing
            // NOTE: Guest has administrator privileges when enabled (for test compatibility)
            // In production, guest should be DISABLED (default)
            Set<String> guestTags = new HashSet<>();
            guestTags.add("administrator"); // Full permissions for testing
            String guestPasswordHash = PasswordHasher.hash("guest");
            User guestUser = new User("guest", guestPasswordHash, guestTags);

            // Grant full permissions on default vhost
            VirtualHostPermissions fullPermissions = new VirtualHostPermissions(".*", ".*", ".*");
            guestUser.setPermissions("/", fullPermissions);

            users.put("guest", guestUser);

            logger.warn("WARNING: Default guest user enabled with full administrator privileges. " +
                       "This should ONLY be used for development/testing, NEVER in production!");
        } else {
            logger.info("Initialized '/' virtual host. Guest user is DISABLED (recommended for production).");
        }
    }

    public User authenticate(String username, String password) {
        // Check rate limiting first
        FailedLoginTracker tracker = failedLogins.computeIfAbsent(username, k -> new FailedLoginTracker());

        if (tracker.isLockedOut()) {
            long remainingMs = tracker.getRemainingLockoutMs();
            logger.warn("SECURITY: Authentication blocked for user {}: account locked for {}ms more",
                    username, remainingMs);
            // Add delay to prevent timing attacks
            try { Thread.sleep(1000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return null;
        }

        User user = users.get(username);

        if (user == null) {
            tracker.recordFailure();
            logger.warn("SECURITY: Authentication failed: user not found: {}", username);
            // Add delay to prevent timing attacks on user enumeration
            try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return null;
        }

        if (!user.isActive()) {
            tracker.recordFailure();
            logger.warn("SECURITY: Authentication failed: user inactive: {}", username);
            try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return null;
        }

        if (!user.verifyPassword(password)) {
            tracker.recordFailure();
            logger.warn("SECURITY: Authentication failed: invalid password for user: {}", username);
            try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return null;
        }

        // Success - reset failed attempts
        tracker.reset();
        logger.info("User authenticated successfully: {}", username);
        return user;
    }

    /**
     * Check if a user account is currently locked out due to failed login attempts.
     */
    public boolean isAccountLocked(String username) {
        FailedLoginTracker tracker = failedLogins.get(username);
        return tracker != null && tracker.isLockedOut();
    }

    /**
     * Manually unlock a user account (admin action).
     */
    public void unlockAccount(String username) {
        FailedLoginTracker tracker = failedLogins.get(username);
        if (tracker != null) {
            tracker.reset();
            logger.info("SECURITY: Account {} unlocked by administrator", username);
        }
    }

    public boolean authorize(User user, String vhost, Permission permission, String resource) {
        if (user == null) {
            return false;
        }

        if (!user.isActive()) {
            return false;
        }

        VirtualHost vh = virtualHosts.get(vhost);
        if (vh == null || !vh.isActive()) {
            logger.warn("Authorization failed: virtual host not found or inactive: {}", vhost);
            return false;
        }

        boolean authorized = user.hasPermission(vhost, permission, resource);

        if (!authorized) {
            logger.warn("Authorization failed: user={}, vhost={}, permission={}, resource={}",
                       user.getUsername(), vhost, permission, resource);
        }

        return authorized;
    }

    public User createUser(String username, String password, Set<String> tags) {
        if (users.containsKey(username)) {
            throw new IllegalArgumentException("User already exists: " + username);
        }

        String passwordHash = PasswordHasher.hash(password);
        User user = new User(username, passwordHash, tags);
        users.put(username, user);

        logger.info("Created user: {}", username);
        return user;
    }

    public void deleteUser(String username) {
        if ("guest".equals(username)) {
            throw new IllegalArgumentException("Cannot delete default guest user");
        }

        User removed = users.remove(username);
        if (removed != null) {
            logger.info("Deleted user: {}", username);
        }
    }

    public User getUser(String username) {
        return users.get(username);
    }

    public List<User> getAllUsers() {
        return new ArrayList<>(users.values());
    }

    public void setUserPermissions(String username, String vhost,
                                   String configureRegex, String writeRegex, String readRegex) {
        User user = users.get(username);
        if (user == null) {
            throw new IllegalArgumentException("User not found: " + username);
        }

        VirtualHost vh = virtualHosts.get(vhost);
        if (vh == null) {
            throw new IllegalArgumentException("Virtual host not found: " + vhost);
        }

        VirtualHostPermissions permissions = new VirtualHostPermissions(configureRegex, writeRegex, readRegex);
        user.setPermissions(vhost, permissions);

        logger.info("Set permissions for user {} on vhost {}", username, vhost);
    }

    public VirtualHost createVirtualHost(String name) {
        if (virtualHosts.containsKey(name)) {
            throw new IllegalArgumentException("Virtual host already exists: " + name);
        }

        VirtualHost vhost = new VirtualHost(name);
        virtualHosts.put(name, vhost);

        logger.info("Created virtual host: {}", name);
        return vhost;
    }

    public void deleteVirtualHost(String name) {
        if ("/".equals(name)) {
            throw new IllegalArgumentException("Cannot delete default virtual host");
        }

        VirtualHost removed = virtualHosts.remove(name);
        if (removed != null) {
            logger.info("Deleted virtual host: {}", name);
        }
    }

    public VirtualHost getVirtualHost(String name) {
        return virtualHosts.get(name);
    }

    public List<VirtualHost> getAllVirtualHosts() {
        return new ArrayList<>(virtualHosts.values());
    }

    public boolean virtualHostExists(String name) {
        return virtualHosts.containsKey(name);
    }
}
