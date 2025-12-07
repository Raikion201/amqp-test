package com.amqp.security;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class User {
    private final String username;
    private final String passwordHash;
    private final Set<String> tags;
    private final ConcurrentMap<String, VirtualHostPermissions> vhostPermissions;
    private final long createdAt;
    private volatile boolean active;

    public User(String username, String passwordHash) {
        this(username, passwordHash, new HashSet<>());
    }

    public User(String username, String passwordHash, Set<String> tags) {
        this.username = username;
        this.passwordHash = passwordHash;
        this.tags = new HashSet<>(tags);
        this.vhostPermissions = new ConcurrentHashMap<>();
        this.createdAt = System.currentTimeMillis();
        this.active = true;
    }

    public String getUsername() {
        return username;
    }

    public String getPasswordHash() {
        return passwordHash;
    }

    public Set<String> getTags() {
        return new HashSet<>(tags);
    }

    public boolean hasTag(String tag) {
        return tags.contains(tag);
    }

    public void addTag(String tag) {
        tags.add(tag);
    }

    public void removeTag(String tag) {
        tags.remove(tag);
    }

    public boolean isAdministrator() {
        return tags.contains("administrator");
    }

    public boolean isMonitoring() {
        return tags.contains("monitoring");
    }

    public boolean isPolicymaker() {
        return tags.contains("policymaker");
    }

    public boolean isManagement() {
        return tags.contains("management");
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

    public void setPermissions(String vhost, VirtualHostPermissions permissions) {
        vhostPermissions.put(vhost, permissions);
    }

    public VirtualHostPermissions getPermissions(String vhost) {
        return vhostPermissions.get(vhost);
    }

    public boolean hasPermission(String vhost, Permission permission, String resource) {
        if (isAdministrator()) {
            return true;
        }

        VirtualHostPermissions perms = vhostPermissions.get(vhost);
        if (perms == null) {
            return false;
        }

        return perms.hasPermission(permission, resource);
    }

    public Map<String, VirtualHostPermissions> getAllPermissions() {
        return new HashMap<>(vhostPermissions);
    }

    public boolean verifyPassword(String plainPassword) {
        return PasswordHasher.verify(plainPassword, passwordHash);
    }

    @Override
    public String toString() {
        return String.format("User{username='%s', tags=%s, active=%s}",
                           username, tags, active);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        User user = (User) o;
        return Objects.equals(username, user.username);
    }

    @Override
    public int hashCode() {
        return Objects.hash(username);
    }
}
