package com.amqp.policy;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Represents a RabbitMQ-style policy that can be applied to queues or exchanges.
 * Policies allow dynamic configuration of features like HA, DLX, TTL, etc.
 */
public class Policy {
    private final String name;
    private final String vhost;
    private final Pattern pattern;
    private final ApplyTo applyTo;
    private final Map<String, Object> definition;
    private final int priority;

    public enum ApplyTo {
        QUEUES("queues"),
        EXCHANGES("exchanges"),
        ALL("all");

        private final String value;

        ApplyTo(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static ApplyTo fromString(String value) {
            for (ApplyTo type : ApplyTo.values()) {
                if (type.value.equalsIgnoreCase(value)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Invalid apply-to value: " + value);
        }
    }

    public Policy(String name, String vhost, String pattern, ApplyTo applyTo,
                  Map<String, Object> definition, int priority) {
        this.name = name;
        this.vhost = vhost;
        this.pattern = Pattern.compile(convertToJavaRegex(pattern));
        this.applyTo = applyTo;
        this.definition = new HashMap<>(definition);
        this.priority = priority;
    }

    /**
     * Convert RabbitMQ regex pattern to Java regex.
     * RabbitMQ uses ^$ anchors by default and supports basic wildcards.
     */
    private String convertToJavaRegex(String rabbitPattern) {
        // If pattern doesn't start with ^, add it
        if (!rabbitPattern.startsWith("^")) {
            rabbitPattern = "^" + rabbitPattern;
        }
        // If pattern doesn't end with $, add it
        if (!rabbitPattern.endsWith("$")) {
            rabbitPattern = rabbitPattern + "$";
        }
        return rabbitPattern;
    }

    public String getName() {
        return name;
    }

    public String getVhost() {
        return vhost;
    }

    public Pattern getPattern() {
        return pattern;
    }

    public ApplyTo getApplyTo() {
        return applyTo;
    }

    public Map<String, Object> getDefinition() {
        return new HashMap<>(definition);
    }

    public int getPriority() {
        return priority;
    }

    /**
     * Check if this policy matches a given resource name.
     */
    public boolean matches(String resourceName, boolean isQueue) {
        if (applyTo == ApplyTo.QUEUES && !isQueue) {
            return false;
        }
        if (applyTo == ApplyTo.EXCHANGES && isQueue) {
            return false;
        }
        return pattern.matcher(resourceName).matches();
    }

    /**
     * Get a specific policy value.
     */
    public Object get(String key) {
        return definition.get(key);
    }

    /**
     * Get a policy value as a string.
     */
    public String getString(String key) {
        Object value = definition.get(key);
        return value != null ? value.toString() : null;
    }

    /**
     * Get a policy value as an integer.
     */
    public Integer getInteger(String key) {
        Object value = definition.get(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    /**
     * Get a policy value as a long.
     */
    public Long getLong(String key) {
        Object value = definition.get(key);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return String.format("Policy{name='%s', vhost='%s', pattern='%s', applyTo=%s, priority=%d, definition=%s}",
                           name, vhost, pattern.pattern(), applyTo, priority, definition);
    }
}
