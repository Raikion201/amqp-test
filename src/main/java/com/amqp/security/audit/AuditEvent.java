package com.amqp.security.audit;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents an audit event.
 */
public class AuditEvent {

    public enum EventType {
        // Connection events
        CONNECTION_OPENED,
        CONNECTION_CLOSED,
        CONNECTION_REJECTED,

        // Authentication events
        AUTH_SUCCESS,
        AUTH_FAILURE,
        AUTH_LOCKOUT,

        // Authorization events
        ACCESS_GRANTED,
        ACCESS_DENIED,

        // Resource events
        QUEUE_CREATED,
        QUEUE_DELETED,
        EXCHANGE_CREATED,
        EXCHANGE_DELETED,
        BINDING_CREATED,
        BINDING_DELETED,

        // Message events
        MESSAGE_PUBLISHED,
        MESSAGE_CONSUMED,
        MESSAGE_REJECTED,

        // Admin events
        USER_CREATED,
        USER_DELETED,
        USER_UPDATED,
        PERMISSION_CHANGED
    }

    private final EventType type;
    private final Instant timestamp;
    private final String username;
    private final String ipAddress;
    private final String resource;
    private final String action;
    private final boolean success;
    private final String message;
    private final Map<String, Object> details;

    private AuditEvent(Builder builder) {
        this.type = builder.type;
        this.timestamp = builder.timestamp != null ? builder.timestamp : Instant.now();
        this.username = builder.username;
        this.ipAddress = builder.ipAddress;
        this.resource = builder.resource;
        this.action = builder.action;
        this.success = builder.success;
        this.message = builder.message;
        this.details = builder.details;
    }

    public static Builder builder(EventType type) {
        return new Builder(type);
    }

    // Getters
    public EventType getType() {
        return type;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public String getUsername() {
        return username;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public String getResource() {
        return resource;
    }

    public String getAction() {
        return action;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getMessage() {
        return message;
    }

    public Map<String, Object> getDetails() {
        return details;
    }

    public Object getDetail(String key) {
        return details.get(key);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(timestamp).append(" ");
        sb.append("[").append(type).append("] ");

        if (username != null) {
            sb.append("user=").append(username).append(" ");
        }
        if (ipAddress != null) {
            sb.append("ip=").append(ipAddress).append(" ");
        }
        if (resource != null) {
            sb.append("resource=").append(resource).append(" ");
        }
        if (action != null) {
            sb.append("action=").append(action).append(" ");
        }

        sb.append("success=").append(success);

        if (message != null) {
            sb.append(" message=\"").append(message).append("\"");
        }

        return sb.toString();
    }

    /**
     * Convert to a map for JSON serialization.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("type", type.name());
        map.put("timestamp", timestamp.toString());
        map.put("success", success);

        if (username != null) map.put("username", username);
        if (ipAddress != null) map.put("ipAddress", ipAddress);
        if (resource != null) map.put("resource", resource);
        if (action != null) map.put("action", action);
        if (message != null) map.put("message", message);
        if (!details.isEmpty()) map.put("details", details);

        return map;
    }

    public static class Builder {
        private final EventType type;
        private Instant timestamp;
        private String username;
        private String ipAddress;
        private String resource;
        private String action;
        private boolean success = true;
        private String message;
        private final Map<String, Object> details = new HashMap<>();

        public Builder(EventType type) {
            this.type = type;
        }

        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder ipAddress(String ipAddress) {
            this.ipAddress = ipAddress;
            return this;
        }

        public Builder resource(String resource) {
            this.resource = resource;
            return this;
        }

        public Builder action(String action) {
            this.action = action;
            return this;
        }

        public Builder success(boolean success) {
            this.success = success;
            return this;
        }

        public Builder message(String message) {
            this.message = message;
            return this;
        }

        public Builder detail(String key, Object value) {
            this.details.put(key, value);
            return this;
        }

        public Builder details(Map<String, Object> details) {
            this.details.putAll(details);
            return this;
        }

        public AuditEvent build() {
            return new AuditEvent(this);
        }
    }

    // Factory methods for common events
    public static AuditEvent authSuccess(String username, String ipAddress) {
        return builder(EventType.AUTH_SUCCESS)
                .username(username)
                .ipAddress(ipAddress)
                .success(true)
                .build();
    }

    public static AuditEvent authFailure(String username, String ipAddress, String reason) {
        return builder(EventType.AUTH_FAILURE)
                .username(username)
                .ipAddress(ipAddress)
                .success(false)
                .message(reason)
                .build();
    }

    public static AuditEvent connectionOpened(String username, String ipAddress) {
        return builder(EventType.CONNECTION_OPENED)
                .username(username)
                .ipAddress(ipAddress)
                .success(true)
                .build();
    }

    public static AuditEvent connectionRejected(String ipAddress, String reason) {
        return builder(EventType.CONNECTION_REJECTED)
                .ipAddress(ipAddress)
                .success(false)
                .message(reason)
                .build();
    }

    public static AuditEvent accessDenied(String username, String resource, String action) {
        return builder(EventType.ACCESS_DENIED)
                .username(username)
                .resource(resource)
                .action(action)
                .success(false)
                .build();
    }
}
