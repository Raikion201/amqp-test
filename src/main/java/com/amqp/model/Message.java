package com.amqp.model;

import java.util.Map;

public class Message {
    private long id;
    private String routingKey;
    private String contentType;
    private String contentEncoding;
    private Map<String, Object> headers;
    private short deliveryMode = 1;
    private short priority = 0;
    private String correlationId;
    private String replyTo;
    private String expiration;
    private String messageId;
    private long timestamp;
    private String type;
    private String userId;
    private String appId;
    private String clusterId;
    private byte[] body;
    
    public Message() {
        this.timestamp = System.currentTimeMillis();
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
    
    @Override
    public String toString() {
        return String.format("Message{id=%d, routingKey='%s', contentType='%s', bodySize=%d}",
                id, routingKey, contentType, body != null ? body.length : 0);
    }
}