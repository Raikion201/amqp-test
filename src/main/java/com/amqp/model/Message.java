package com.amqp.model;

import java.util.Map;

public class Message {
    /**
     * AMQP 1.0 body types.
     */
    public enum BodyType {
        /** Body is raw binary data (Data section) */
        DATA,
        /** Body is a typed value (AmqpValue section) */
        AMQP_VALUE,
        /** Body is a sequence (AmqpSequence section) */
        AMQP_SEQUENCE
    }

    private long id;
    private String routingKey;
    private String contentType;
    private String contentEncoding;
    private Map<String, Object> headers;
    private short deliveryMode = 1;
    private short priority = 0;
    private String correlationId;
    private Object correlationIdTyped; // For AMQP 1.0 type preservation (UUID, UnsignedLong, Binary)
    private String replyTo;
    private String expiration;
    private String messageId;
    private Object messageIdTyped; // For AMQP 1.0 type preservation (UUID, UnsignedLong, Binary)
    private long timestamp;
    private String type;
    private String userId;
    private String appId;
    private String clusterId;
    private byte[] body;
    private BodyType bodyType = BodyType.DATA; // Default to Data for AMQP 0-9-1 compatibility
    private Object amqpValueTyped; // For AMQP 1.0: stores the original typed value (Map, List, etc.)
    private String subject; // AMQP 1.0 subject property
    private Long ttl; // Time-to-live in milliseconds (from AMQP 1.0 Header)
    private Long absoluteExpiryTime; // Absolute expiry time in milliseconds since epoch (from AMQP 1.0 Properties)
    private long creationTime; // When the message was received by the broker
    
    public Message() {
        this.timestamp = System.currentTimeMillis();
        this.creationTime = System.currentTimeMillis();
    }
    
    public Message(byte[] body) {
        this();
        this.body = body;
    }
    
    public long getId() {
        return id;
    }
    
    public void setId(long id) {
        this.id = id;
    }
    
    public String getRoutingKey() {
        return routingKey;
    }
    
    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }
    
    public String getContentType() {
        return contentType;
    }
    
    public void setContentType(String contentType) {
        this.contentType = contentType;
    }
    
    public String getContentEncoding() {
        return contentEncoding;
    }
    
    public void setContentEncoding(String contentEncoding) {
        this.contentEncoding = contentEncoding;
    }
    
    public Map<String, Object> getHeaders() {
        return headers;
    }
    
    public void setHeaders(Map<String, Object> headers) {
        this.headers = headers;
    }
    
    public short getDeliveryMode() {
        return deliveryMode;
    }
    
    public void setDeliveryMode(short deliveryMode) {
        this.deliveryMode = deliveryMode;
    }
    
    public boolean isPersistent() {
        return deliveryMode == 2;
    }
    
    public short getPriority() {
        return priority;
    }
    
    public void setPriority(short priority) {
        this.priority = priority;
    }
    
    public String getCorrelationId() {
        return correlationId;
    }
    
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public Object getCorrelationIdTyped() {
        return correlationIdTyped;
    }

    public void setCorrelationIdTyped(Object correlationIdTyped) {
        this.correlationIdTyped = correlationIdTyped;
        // Also set string version for compatibility
        if (correlationIdTyped != null) {
            this.correlationId = correlationIdTyped.toString();
        }
    }
    
    public String getReplyTo() {
        return replyTo;
    }
    
    public void setReplyTo(String replyTo) {
        this.replyTo = replyTo;
    }
    
    public String getExpiration() {
        return expiration;
    }
    
    public void setExpiration(String expiration) {
        this.expiration = expiration;
    }
    
    public String getMessageId() {
        return messageId;
    }
    
    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public Object getMessageIdTyped() {
        return messageIdTyped;
    }

    public void setMessageIdTyped(Object messageIdTyped) {
        this.messageIdTyped = messageIdTyped;
        // Also set string version for compatibility
        if (messageIdTyped != null) {
            this.messageId = messageIdTyped.toString();
        }
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    
    public String getType() {
        return type;
    }
    
    public void setType(String type) {
        this.type = type;
    }
    
    public String getUserId() {
        return userId;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    public String getAppId() {
        return appId;
    }
    
    public void setAppId(String appId) {
        this.appId = appId;
    }
    
    public String getClusterId() {
        return clusterId;
    }
    
    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }
    
    public byte[] getBody() {
        return body;
    }
    
    public void setBody(byte[] body) {
        this.body = body;
    }

    public BodyType getBodyType() {
        return bodyType;
    }

    public void setBodyType(BodyType bodyType) {
        this.bodyType = bodyType;
    }

    public Object getAmqpValueTyped() {
        return amqpValueTyped;
    }

    public void setAmqpValueTyped(Object amqpValueTyped) {
        this.amqpValueTyped = amqpValueTyped;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public Long getTtl() {
        return ttl;
    }

    public void setTtl(Long ttl) {
        this.ttl = ttl;
    }

    public Long getAbsoluteExpiryTime() {
        return absoluteExpiryTime;
    }

    public void setAbsoluteExpiryTime(Long absoluteExpiryTime) {
        this.absoluteExpiryTime = absoluteExpiryTime;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    /**
     * Check if this message has expired based on TTL or absolute expiry time.
     * @return true if the message has expired, false otherwise
     */
    public boolean isExpired() {
        long now = System.currentTimeMillis();

        // Check absolute expiry time first (takes precedence)
        if (absoluteExpiryTime != null && absoluteExpiryTime > 0) {
            return now >= absoluteExpiryTime;
        }

        // Check TTL (relative to creation time)
        if (ttl != null && ttl > 0) {
            long expiryTime = creationTime + ttl;
            return now >= expiryTime;
        }

        // Check string expiration (AMQP 0-9-1 style, TTL in milliseconds as string)
        if (expiration != null && !expiration.isEmpty()) {
            try {
                long ttlMs = Long.parseLong(expiration);
                long expiryTime = creationTime + ttlMs;
                return now >= expiryTime;
            } catch (NumberFormatException e) {
                // Invalid expiration format, treat as not expired
            }
        }

        return false;
    }

    @Override
    public String toString() {
        return String.format("Message{id=%d, routingKey='%s', contentType='%s', bodySize=%d}",
                id, routingKey, contentType, body != null ? body.length : 0);
    }
}