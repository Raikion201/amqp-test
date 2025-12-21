package com.amqp.security.audit;

import java.util.EnumSet;
import java.util.Set;

/**
 * Configuration for audit logging.
 */
public class AuditConfig {

    public enum Format {
        TEXT,
        JSON,
        CSV
    }

    private boolean enabled = true;
    private boolean async = true;
    private int queueSize = 10000;
    private boolean logToFile = true;
    private boolean logToConsole = false;
    private Format format = Format.JSON;

    // Event type filtering
    private Set<AuditEvent.EventType> enabledEventTypes = EnumSet.allOf(AuditEvent.EventType.class);

    public AuditConfig() {
    }

    public static Builder builder() {
        return new Builder();
    }

    // Getters
    public boolean isEnabled() {
        return enabled;
    }

    public boolean isAsync() {
        return async;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public boolean isLogToFile() {
        return logToFile;
    }

    public boolean isLogToConsole() {
        return logToConsole;
    }

    public Format getFormat() {
        return format;
    }

    public Set<AuditEvent.EventType> getEnabledEventTypes() {
        return enabledEventTypes;
    }

    public boolean isEventTypeEnabled(AuditEvent.EventType type) {
        return enabledEventTypes.contains(type);
    }

    // Setters
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setAsync(boolean async) {
        this.async = async;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public void setLogToFile(boolean logToFile) {
        this.logToFile = logToFile;
    }

    public void setLogToConsole(boolean logToConsole) {
        this.logToConsole = logToConsole;
    }

    public void setFormat(Format format) {
        this.format = format;
    }

    public void setEnabledEventTypes(Set<AuditEvent.EventType> types) {
        this.enabledEventTypes = types;
    }

    public void enableEventType(AuditEvent.EventType type) {
        this.enabledEventTypes.add(type);
    }

    public void disableEventType(AuditEvent.EventType type) {
        this.enabledEventTypes.remove(type);
    }

    public static class Builder {
        private final AuditConfig config = new AuditConfig();

        public Builder enabled(boolean enabled) {
            config.enabled = enabled;
            return this;
        }

        public Builder async(boolean async) {
            config.async = async;
            return this;
        }

        public Builder queueSize(int size) {
            config.queueSize = size;
            return this;
        }

        public Builder logToFile(boolean log) {
            config.logToFile = log;
            return this;
        }

        public Builder logToConsole(boolean log) {
            config.logToConsole = log;
            return this;
        }

        public Builder format(Format format) {
            config.format = format;
            return this;
        }

        public Builder enabledEventTypes(AuditEvent.EventType... types) {
            config.enabledEventTypes = EnumSet.of(types[0], types);
            return this;
        }

        public Builder allEventTypes() {
            config.enabledEventTypes = EnumSet.allOf(AuditEvent.EventType.class);
            return this;
        }

        public Builder securityEventsOnly() {
            config.enabledEventTypes = EnumSet.of(
                    AuditEvent.EventType.CONNECTION_OPENED,
                    AuditEvent.EventType.CONNECTION_CLOSED,
                    AuditEvent.EventType.CONNECTION_REJECTED,
                    AuditEvent.EventType.AUTH_SUCCESS,
                    AuditEvent.EventType.AUTH_FAILURE,
                    AuditEvent.EventType.AUTH_LOCKOUT,
                    AuditEvent.EventType.ACCESS_GRANTED,
                    AuditEvent.EventType.ACCESS_DENIED
            );
            return this;
        }

        public AuditConfig build() {
            return config;
        }
    }
}
