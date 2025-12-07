package com.amqp.model;

import java.util.HashMap;
import java.util.Map;

public class QueueArguments {
    private String deadLetterExchange;
    private String deadLetterRoutingKey;
    private Long messageTtl;  // in milliseconds
    private Long expires;  // queue expiry time in milliseconds
    private Integer maxLength;  // max number of messages
    private Long maxLengthBytes;  // max total message size in bytes
    private Integer maxPriority;  // 0-255, enables priority queue
    private String overflowBehavior = "drop-head";  // "drop-head", "reject-publish", "reject-publish-dlx"
    private boolean lazy = false;  // lazy queue mode (disk-based)
    private String mode = "default";  // "default", "lazy", "quorum"

    // Alternate exchange
    private String alternateExchange;

    // Stream queue specific
    private Long maxAge;  // Max age for stream messages
    private Long maxBytes;  // Max bytes for stream
    private Long maxSegmentSize;  // Max segment size for stream

    // Generic arguments map for extensibility
    private final Map<String, Object> customArguments = new HashMap<>();

    public QueueArguments() {
    }

    public static QueueArguments fromMap(Map<String, Object> args) {
        if (args == null) {
            return new QueueArguments();
        }

        QueueArguments queueArgs = new QueueArguments();

        if (args.containsKey("x-dead-letter-exchange")) {
            queueArgs.setDeadLetterExchange((String) args.get("x-dead-letter-exchange"));
        }
        if (args.containsKey("x-dead-letter-routing-key")) {
            queueArgs.setDeadLetterRoutingKey((String) args.get("x-dead-letter-routing-key"));
        }
        if (args.containsKey("x-message-ttl")) {
            Object ttl = args.get("x-message-ttl");
            queueArgs.setMessageTtl(ttl instanceof Number ? ((Number) ttl).longValue() : null);
        }
        if (args.containsKey("x-expires")) {
            Object expires = args.get("x-expires");
            queueArgs.setExpires(expires instanceof Number ? ((Number) expires).longValue() : null);
        }
        if (args.containsKey("x-max-length")) {
            Object maxLength = args.get("x-max-length");
            queueArgs.setMaxLength(maxLength instanceof Number ? ((Number) maxLength).intValue() : null);
        }
        if (args.containsKey("x-max-length-bytes")) {
            Object maxLengthBytes = args.get("x-max-length-bytes");
            queueArgs.setMaxLengthBytes(maxLengthBytes instanceof Number ? ((Number) maxLengthBytes).longValue() : null);
        }
        if (args.containsKey("x-max-priority")) {
            Object maxPriority = args.get("x-max-priority");
            queueArgs.setMaxPriority(maxPriority instanceof Number ? ((Number) maxPriority).intValue() : null);
        }
        if (args.containsKey("x-overflow")) {
            queueArgs.setOverflowBehavior((String) args.get("x-overflow"));
        }
        if (args.containsKey("x-queue-mode")) {
            String mode = (String) args.get("x-queue-mode");
            queueArgs.setMode(mode);
            queueArgs.setLazy("lazy".equals(mode));
        }

        return queueArgs;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();

        if (deadLetterExchange != null) {
            map.put("x-dead-letter-exchange", deadLetterExchange);
        }
        if (deadLetterRoutingKey != null) {
            map.put("x-dead-letter-routing-key", deadLetterRoutingKey);
        }
        if (messageTtl != null) {
            map.put("x-message-ttl", messageTtl);
        }
        if (expires != null) {
            map.put("x-expires", expires);
        }
        if (maxLength != null) {
            map.put("x-max-length", maxLength);
        }
        if (maxLengthBytes != null) {
            map.put("x-max-length-bytes", maxLengthBytes);
        }
        if (maxPriority != null) {
            map.put("x-max-priority", maxPriority);
        }
        if (overflowBehavior != null) {
            map.put("x-overflow", overflowBehavior);
        }
        if (mode != null && !mode.equals("default")) {
            map.put("x-queue-mode", mode);
        }

        return map;
    }

    // Getters and setters
    public String getDeadLetterExchange() {
        return deadLetterExchange;
    }

    public void setDeadLetterExchange(String deadLetterExchange) {
        this.deadLetterExchange = deadLetterExchange;
    }

    public String getDeadLetterRoutingKey() {
        return deadLetterRoutingKey;
    }

    public void setDeadLetterRoutingKey(String deadLetterRoutingKey) {
        this.deadLetterRoutingKey = deadLetterRoutingKey;
    }

    public Long getMessageTtl() {
        return messageTtl;
    }

    public void setMessageTtl(Long messageTtl) {
        this.messageTtl = messageTtl;
    }

    public Long getExpires() {
        return expires;
    }

    public void setExpires(Long expires) {
        this.expires = expires;
    }

    public Integer getMaxLength() {
        return maxLength;
    }

    public void setMaxLength(Integer maxLength) {
        this.maxLength = maxLength;
    }

    public Long getMaxLengthBytes() {
        return maxLengthBytes;
    }

    public void setMaxLengthBytes(Long maxLengthBytes) {
        this.maxLengthBytes = maxLengthBytes;
    }

    public Integer getMaxPriority() {
        return maxPriority;
    }

    public void setMaxPriority(Integer maxPriority) {
        if (maxPriority != null && (maxPriority < 0 || maxPriority > 255)) {
            throw new IllegalArgumentException("Max priority must be between 0 and 255");
        }
        this.maxPriority = maxPriority;
    }

    public String getOverflowBehavior() {
        return overflowBehavior;
    }

    public void setOverflowBehavior(String overflowBehavior) {
        this.overflowBehavior = overflowBehavior;
    }

    public boolean isLazy() {
        return lazy;
    }

    public void setLazy(boolean lazy) {
        this.lazy = lazy;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getAlternateExchange() {
        return alternateExchange;
    }

    public void setAlternateExchange(String alternateExchange) {
        this.alternateExchange = alternateExchange;
    }

    public boolean hasDLX() {
        return deadLetterExchange != null && !deadLetterExchange.isEmpty();
    }

    public boolean hasTTL() {
        return messageTtl != null && messageTtl > 0;
    }

    public boolean hasExpiry() {
        return expires != null && expires > 0;
    }

    public boolean hasMaxLength() {
        return maxLength != null && maxLength > 0;
    }

    public boolean hasMaxLengthBytes() {
        return maxLengthBytes != null && maxLengthBytes > 0;
    }

    public boolean isPriorityQueue() {
        return maxPriority != null && maxPriority > 0;
    }

    // Stream queue methods
    public boolean hasMaxAge() {
        return maxAge != null && maxAge > 0;
    }

    public Long getMaxAge() {
        return maxAge;
    }

    public void setMaxAge(Long maxAge) {
        this.maxAge = maxAge;
    }

    public boolean hasMaxBytes() {
        return maxBytes != null && maxBytes > 0;
    }

    public Long getMaxBytes() {
        return maxBytes;
    }

    public void setMaxBytes(Long maxBytes) {
        this.maxBytes = maxBytes;
    }

    public boolean hasMaxSegmentSize() {
        return maxSegmentSize != null && maxSegmentSize > 0;
    }

    public Long getMaxSegmentSize() {
        return maxSegmentSize;
    }

    public void setMaxSegmentSize(Long maxSegmentSize) {
        this.maxSegmentSize = maxSegmentSize;
    }

    // Generic put method for custom arguments
    public void put(String key, Object value) {
        // Handle known arguments
        switch (key) {
            case "x-dead-letter-exchange":
                setDeadLetterExchange((String) value);
                break;
            case "x-dead-letter-routing-key":
                setDeadLetterRoutingKey((String) value);
                break;
            case "x-message-ttl":
                setMessageTtl(value instanceof Number ? ((Number) value).longValue() : null);
                break;
            case "x-expires":
                setExpires(value instanceof Number ? ((Number) value).longValue() : null);
                break;
            case "x-max-length":
                setMaxLength(value instanceof Number ? ((Number) value).intValue() : null);
                break;
            case "x-max-length-bytes":
                setMaxLengthBytes(value instanceof Number ? ((Number) value).longValue() : null);
                break;
            case "x-max-priority":
                setMaxPriority(value instanceof Number ? ((Number) value).intValue() : null);
                break;
            case "x-overflow":
                setOverflowBehavior((String) value);
                break;
            case "x-queue-mode":
                String modeValue = (String) value;
                setMode(modeValue);
                setLazy("lazy".equals(modeValue));
                break;
            case "x-max-age":
                setMaxAge(value instanceof Number ? ((Number) value).longValue() : null);
                break;
            case "x-max-bytes":
                setMaxBytes(value instanceof Number ? ((Number) value).longValue() : null);
                break;
            case "x-max-segment-size":
                setMaxSegmentSize(value instanceof Number ? ((Number) value).longValue() : null);
                break;
            default:
                // Store in custom arguments
                customArguments.put(key, value);
        }
    }

    public Object get(String key) {
        switch (key) {
            case "x-dead-letter-exchange": return deadLetterExchange;
            case "x-dead-letter-routing-key": return deadLetterRoutingKey;
            case "x-message-ttl": return messageTtl;
            case "x-expires": return expires;
            case "x-max-length": return maxLength;
            case "x-max-length-bytes": return maxLengthBytes;
            case "x-max-priority": return maxPriority;
            case "x-overflow": return overflowBehavior;
            case "x-queue-mode": return mode;
            case "x-max-age": return maxAge;
            case "x-max-bytes": return maxBytes;
            case "x-max-segment-size": return maxSegmentSize;
            default: return customArguments.get(key);
        }
    }

    @Override
    public String toString() {
        return "QueueArguments{" +
               "DLX='" + deadLetterExchange + '\'' +
               ", DLX_RK='" + deadLetterRoutingKey + '\'' +
               ", TTL=" + messageTtl +
               ", expires=" + expires +
               ", maxLength=" + maxLength +
               ", maxLengthBytes=" + maxLengthBytes +
               ", maxPriority=" + maxPriority +
               ", overflow='" + overflowBehavior + '\'' +
               ", mode='" + mode + '\'' +
               ", maxAge=" + maxAge +
               ", maxBytes=" + maxBytes +
               '}';
    }
}
