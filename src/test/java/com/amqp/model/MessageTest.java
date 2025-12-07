package com.amqp.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

@DisplayName("Message Tests")
class MessageTest {

    @Test
    @DisplayName("Should create message with body")
    void testMessageCreation() {
        byte[] body = "Hello World".getBytes();
        Message message = new Message(body);

        assertThat(message.getBody()).isEqualTo(body);
        assertThat(message.getTimestamp()).isGreaterThan(0);
    }

    @Test
    @DisplayName("Should set and get message properties")
    void testMessageProperties() {
        Message message = new Message();
        
        message.setRoutingKey("orders.new");
        message.setContentType("application/json");
        message.setContentEncoding("UTF-8");
        message.setDeliveryMode((short) 2);
        message.setPriority((short) 5);
        message.setCorrelationId("corr-123");
        message.setReplyTo("reply-queue");
        message.setExpiration("60000");
        message.setMessageId("msg-456");
        message.setType("OrderCreated");
        message.setUserId("user-789");
        message.setAppId("order-service");

        assertThat(message.getRoutingKey()).isEqualTo("orders.new");
        assertThat(message.getContentType()).isEqualTo("application/json");
        assertThat(message.getContentEncoding()).isEqualTo("UTF-8");
        assertThat(message.getDeliveryMode()).isEqualTo((short) 2);
        assertThat(message.getPriority()).isEqualTo((short) 5);
        assertThat(message.getCorrelationId()).isEqualTo("corr-123");
        assertThat(message.getReplyTo()).isEqualTo("reply-queue");
        assertThat(message.getExpiration()).isEqualTo("60000");
        assertThat(message.getMessageId()).isEqualTo("msg-456");
        assertThat(message.getType()).isEqualTo("OrderCreated");
        assertThat(message.getUserId()).isEqualTo("user-789");
        assertThat(message.getAppId()).isEqualTo("order-service");
    }

    @Test
    @DisplayName("Should identify persistent messages")
    void testPersistentMessage() {
        Message message = new Message();
        
        message.setDeliveryMode((short) 1);
        assertThat(message.isPersistent()).isFalse();
        
        message.setDeliveryMode((short) 2);
        assertThat(message.isPersistent()).isTrue();
    }

    @Test
    @DisplayName("Should set and get headers")
    void testMessageHeaders() {
        Message message = new Message();
        Map<String, Object> headers = new HashMap<>();
        headers.put("x-retry-count", 3);
        headers.put("x-source", "order-service");
        headers.put("x-priority", "high");

        message.setHeaders(headers);

        assertThat(message.getHeaders()).containsEntry("x-retry-count", 3);
        assertThat(message.getHeaders()).containsEntry("x-source", "order-service");
        assertThat(message.getHeaders()).containsEntry("x-priority", "high");
    }

    @Test
    @DisplayName("Should handle empty message body")
    void testEmptyMessageBody() {
        Message message = new Message(new byte[0]);
        
        assertThat(message.getBody()).isEmpty();
    }

    @Test
    @DisplayName("Should default delivery mode to 1 (non-persistent)")
    void testDefaultDeliveryMode() {
        Message message = new Message();
        
        assertThat(message.getDeliveryMode()).isEqualTo((short) 1);
        assertThat(message.isPersistent()).isFalse();
    }

    @Test
    @DisplayName("Should default priority to 0")
    void testDefaultPriority() {
        Message message = new Message();
        
        assertThat(message.getPriority()).isEqualTo((short) 0);
    }
}
