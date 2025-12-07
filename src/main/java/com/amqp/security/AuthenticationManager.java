package com.amqp.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class AuthenticationManager {
    private static final Logger logger = LoggerFactory.getLogger(AuthenticationManager.class);

    private final ConcurrentMap<String, User> users;
    private final ConcurrentMap<String, VirtualHost> virtualHosts;

    public AuthenticationManager() {
        this.users = new ConcurrentHashMap<>();
        this.virtualHosts = new ConcurrentHashMap<>();

        initializeDefaults();
    }

    private void initializeDefaults() {
        // Create default virtual host
        VirtualHost defaultVhost = new VirtualHost("/");
        virtualHosts.put("/", defaultVhost);

        // Create default guest user with full permissions on default vhost
        Set<String> guestTags = new HashSet<>();
        guestTags.add("administrator");
        String guestPasswordHash = PasswordHasher.hash("guest");
        User guestUser = new User("guest", guestPasswordHash, guestTags);

        // Grant full permissions on default vhost
        VirtualHostPermissions fullPermissions = new VirtualHostPermissions(".*", ".*", ".*");
        guestUser.setPermissions("/", fullPermissions);

        users.put("guest", guestUser);

        logger.info("Initialized default guest user and '/' virtual host");
    }

    public User authenticate(String username, String password) {
        User user = users.get(username);

        if (user == null) {
            logger.warn("Authentication failed: user not found: {}", username);
            return null;
        }

        if (!user.isActive()) {
            logger.warn("Authentication failed: user inactive: {}", username);
            return null;
        }

        if (!user.verifyPassword(password)) {
            logger.warn("Authentication failed: invalid password for user: {}", username);
            return null;
        }

        logger.info("User authenticated successfully: {}", username);
        return user;
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
