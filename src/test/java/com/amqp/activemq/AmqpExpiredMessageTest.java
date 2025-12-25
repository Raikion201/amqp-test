/*
 * Adapted from Apache ActiveMQ Artemis AmqpExpiredMessageTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Disabled;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 message expiry and TTL handling.
 * Adapted from Apache ActiveMQ Artemis AmqpExpiredMessageTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Expired Message Tests")
public class AmqpExpiredMessageTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Send message with TTL that expires before receive")
    @Timeout(30)
    void testSendMessageThenAllowToExpiredUsingTimeToLive() throws Exception {
        String queueName = generateQueueName("expired-ttl");

        // Create message with very short TTL (100ms)
        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("Short lived message"));
        msg.setMessageId("ttl-msg-1");

        Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(100)); // 100ms TTL
        msg.setHeader(header);

        sendMessage(queueName, msg);

        // Wait for message to expire
        Thread.sleep(500);

        // Should not receive expired message
        Message received = receiveMessage(queueName, 2000);
        assertNull(received, "Should not receive expired message");

        log.info("PASS: Message with short TTL expires and is not delivered");
    }

    @Test
    @Order(2)
    @DisplayName("Test: Send message with TTL that is still valid")
    @Timeout(30)
    void testSendMessageThatIsNotExpiredUsingTimeToLive() throws Exception {
        String queueName = generateQueueName("expired-valid-ttl");

        // Create message with long TTL (60 seconds)
        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("Long lived message"));
        msg.setMessageId("ttl-msg-valid");

        Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(60000)); // 60 second TTL
        msg.setHeader(header);

        sendMessage(queueName, msg);

        // Should receive message since TTL hasn't expired
        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message with valid TTL");
        assertEquals("ttl-msg-valid", received.getMessageId());

        log.info("PASS: Message with valid TTL is delivered");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Send message with absolute expiry time in the past")
    @Timeout(30)
    void testSendMessageThatIsAlreadyExpiredUsingAbsoluteTime() throws Exception {
        String queueName = generateQueueName("expired-absolute-past");

        // Create message with expiry time in the past
        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("Already expired message"));

        Properties props = new Properties();
        props.setMessageId("expired-abs-msg");
        props.setAbsoluteExpiryTime(new Date(System.currentTimeMillis() - 5000)); // 5 seconds ago
        msg.setProperties(props);

        sendMessage(queueName, msg);

        // Should not receive already expired message
        Message received = receiveMessage(queueName, 2000);
        assertNull(received, "Should not receive message with past expiry time");

        log.info("PASS: Message with past absolute expiry time is not delivered");
    }

    @Test
    @Order(4)
    @DisplayName("Test: Send message with absolute expiry time in the future")
    @Timeout(30)
    void testSendMessageThatIsNotExpiredUsingAbsoluteTime() throws Exception {
        String queueName = generateQueueName("expired-absolute-future");

        // Create message with expiry time in the future
        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("Future expiry message"));

        Properties props = new Properties();
        props.setMessageId("future-abs-msg");
        props.setAbsoluteExpiryTime(new Date(System.currentTimeMillis() + 60000)); // 60 seconds from now
        msg.setProperties(props);

        sendMessage(queueName, msg);

        // Should receive message since expiry is in future
        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message with future expiry time");
        assertEquals("future-abs-msg", received.getMessageId());

        log.info("PASS: Message with future absolute expiry time is delivered");
    }

    @Test
    @Order(5)
    @DisplayName("Test: Absolute expiry time overrides TTL")
    @Timeout(30)
    void testSendMessageThatIsExpiredUsingAbsoluteTimeWithLongTTL() throws Exception {
        String queueName = generateQueueName("expired-abs-override");

        // Create message with long TTL but past absolute expiry
        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("Override message"));

        Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(300000)); // 5 minute TTL
        msg.setHeader(header);

        Properties props = new Properties();
        props.setMessageId("override-msg");
        props.setAbsoluteExpiryTime(new Date(System.currentTimeMillis() - 1000)); // Already expired
        msg.setProperties(props);

        sendMessage(queueName, msg);

        // Should not receive - absolute expiry takes precedence
        Message received = receiveMessage(queueName, 2000);
        assertNull(received, "Absolute expiry should override TTL");

        log.info("PASS: Absolute expiry time overrides TTL");
    }

    @Test
    @Order(6)
    @DisplayName("Test: Message with max TTL value does not expire")
    @Timeout(30)
    void testSendMessageThatIsNotExpiredUsingTimeToLiveOfMaxValue() throws Exception {
        String queueName = generateQueueName("expired-max-ttl");

        // Create message with max TTL (essentially no expiry)
        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("Max TTL message"));
        msg.setMessageId("max-ttl-msg");

        Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(0xFFFFFFFFL)); // Max unsigned int
        msg.setHeader(header);

        sendMessage(queueName, msg);

        // Should receive message
        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message with max TTL");
        assertEquals("max-ttl-msg", received.getMessageId());

        log.info("PASS: Message with max TTL value is delivered");
    }

    @Test
    @Order(7)
    @DisplayName("Test: TTL is used when absolute expiry is zero")
    @Timeout(30)
    void testSendMessageThatIsExpiredUsingTTLWhenAbsoluteIsZero() throws Exception {
        String queueName = generateQueueName("expired-ttl-zero-abs");

        // Create message with short TTL and zero absolute expiry
        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("TTL fallback message"));

        Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(100)); // 100ms TTL
        msg.setHeader(header);

        Properties props = new Properties();
        props.setMessageId("ttl-fallback-msg");
        props.setAbsoluteExpiryTime(new Date(0)); // Zero/epoch time
        msg.setProperties(props);

        sendMessage(queueName, msg);

        // Wait for TTL to expire
        Thread.sleep(500);

        // Should not receive - TTL should kick in
        Message received = receiveMessage(queueName, 2000);
        assertNull(received, "TTL should be used when absolute expiry is zero");

        log.info("PASS: TTL is used when absolute expiry is zero/epoch");
    }

    @Test
    @Order(8)
    @DisplayName("Test: Multiple messages with different TTLs")
    @Timeout(30)
    void testMultipleMessagesWithDifferentTTLs() throws Exception {
        String queueName = generateQueueName("expired-multi-ttl");

        // Send message with short TTL (will expire)
        Message msg1 = Message.Factory.create();
        msg1.setBody(new AmqpValue("Short TTL"));
        msg1.setMessageId("short-ttl");
        Header header1 = new Header();
        header1.setTtl(UnsignedInteger.valueOf(100));
        msg1.setHeader(header1);
        sendMessage(queueName, msg1);

        // Send message with long TTL (will not expire)
        Message msg2 = Message.Factory.create();
        msg2.setBody(new AmqpValue("Long TTL"));
        msg2.setMessageId("long-ttl");
        Header header2 = new Header();
        header2.setTtl(UnsignedInteger.valueOf(60000));
        msg2.setHeader(header2);
        sendMessage(queueName, msg2);

        // Send message with no TTL (will not expire)
        Message msg3 = Message.Factory.create();
        msg3.setBody(new AmqpValue("No TTL"));
        msg3.setMessageId("no-ttl");
        sendMessage(queueName, msg3);

        // Wait for short TTL to expire
        Thread.sleep(500);

        // Receive all remaining messages
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger receivedCount = new AtomicInteger(0);
        java.util.List<String> receivedIds = java.util.Collections.synchronizedList(
            new java.util.ArrayList<>());
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            receivedIds.add((String) msg.getMessageId());
                            delivery.disposition(new Accepted(), true);
                            receivedCount.incrementAndGet();
                        });
                        receiver.openHandler(receiverRes -> {
                            if (receiverRes.succeeded()) {
                                receiver.flow(5);
                                // Set timeout
                                vertx.setTimer(3000, id -> {
                                    receiver.close();
                                    conn.close();
                                    done.countDown();
                                });
                            } else {
                                errorRef.set("Receiver attach failed");
                                done.countDown();
                            }
                        });
                        receiver.open();
                    } else {
                        errorRef.set("Connection open failed");
                        done.countDown();
                    }
                });
                conn.open();
            } else {
                errorRef.set("Connect failed");
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Receive timeout");
        assertNull(errorRef.get(), errorRef.get());

        // Should receive 2 messages (long-ttl and no-ttl)
        assertEquals(2, receivedCount.get(), "Should receive 2 non-expired messages");
        assertTrue(receivedIds.contains("long-ttl"), "Should receive long-ttl message");
        assertTrue(receivedIds.contains("no-ttl"), "Should receive no-ttl message");
        assertFalse(receivedIds.contains("short-ttl"), "Should not receive expired short-ttl message");

        log.info("PASS: Multiple messages with different TTLs handled correctly");
    }

    @Test
    @Order(9)
    @DisplayName("Test: Message durability preserved through expiry check")
    @Timeout(30)
    void testDurableMessageWithValidTTL() throws Exception {
        String queueName = generateQueueName("expired-durable");

        // Create durable message with valid TTL
        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("Durable message"));
        msg.setMessageId("durable-ttl-msg");
        msg.setDurable(true);

        Header header = new Header();
        header.setDurable(true);
        header.setTtl(UnsignedInteger.valueOf(60000));
        msg.setHeader(header);

        sendMessage(queueName, msg);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive durable message");
        assertEquals("durable-ttl-msg", received.getMessageId());

        // Check durability is preserved
        if (received.getHeader() != null && received.getHeader().getDurable() != null) {
            assertTrue(received.getHeader().getDurable(), "Durability should be preserved");
        }

        log.info("PASS: Durable message with valid TTL is delivered with durability preserved");
    }

    @Test
    @Order(10)
    @DisplayName("Test: Message priority preserved through expiry check")
    @Timeout(30)
    void testMessagePriorityPreserved() throws Exception {
        String queueName = generateQueueName("expired-priority");

        // Create high priority message with valid TTL
        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("Priority message"));
        msg.setMessageId("priority-msg");

        Header header = new Header();
        header.setPriority(org.apache.qpid.proton.amqp.UnsignedByte.valueOf((byte) 9));
        header.setTtl(UnsignedInteger.valueOf(60000));
        msg.setHeader(header);

        sendMessage(queueName, msg);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive priority message");
        assertEquals("priority-msg", received.getMessageId());

        // Check priority is preserved
        if (received.getHeader() != null && received.getHeader().getPriority() != null) {
            assertEquals(9, received.getHeader().getPriority().intValue(),
                "Priority should be preserved");
        }

        log.info("PASS: Message priority preserved through expiry check");
    }

    @Test
    @Order(11)
    @DisplayName("Test: Application properties preserved through expiry check")
    @Timeout(30)
    void testApplicationPropertiesPreserved() throws Exception {
        String queueName = generateQueueName("expired-appprops");

        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("Properties message"));
        msg.setMessageId("appprops-msg");

        Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(60000));
        msg.setHeader(header);

        java.util.Map<String, Object> appProps = new java.util.HashMap<>();
        appProps.put("custom-header", "custom-value");
        appProps.put("numeric-prop", 12345);
        appProps.put("boolean-prop", true);
        msg.setApplicationProperties(new ApplicationProperties(appProps));

        sendMessage(queueName, msg);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");
        assertNotNull(received.getApplicationProperties(), "Should have application properties");

        java.util.Map<String, Object> receivedProps = received.getApplicationProperties().getValue();
        assertEquals("custom-value", receivedProps.get("custom-header"));
        assertEquals(12345, receivedProps.get("numeric-prop"));
        assertEquals(true, receivedProps.get("boolean-prop"));

        log.info("PASS: Application properties preserved through expiry check");
    }
}
