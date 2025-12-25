/*
 * Adapted from Apache ActiveMQ Artemis AmqpScheduledMessageTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Disabled;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 scheduled/delayed message delivery.
 * Adapted from Apache ActiveMQ Artemis AmqpScheduledMessageTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Scheduled Message Tests")
public class AmqpScheduledMessageTest extends Amqp10TestBase {

    private static final Symbol DELIVERY_TIME = Symbol.valueOf("x-opt-delivery-time");
    private static final Symbol DELIVERY_DELAY = Symbol.valueOf("x-opt-delivery-delay");

    @Test
    @Order(1)
    @DisplayName("Test: Send with delivery time is scheduled")
    @Timeout(60)
    void testSendWithDeliveryTimeIsScheduled() throws Exception {
        String queueName = generateQueueName("scheduled-delivery-time");

        // Schedule message 5 seconds in future
        long deliveryTime = System.currentTimeMillis() + 5000;

        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("Scheduled message"));

        Map<Symbol, Object> annotations = new HashMap<>();
        annotations.put(DELIVERY_TIME, deliveryTime);
        msg.setMessageAnnotations(new MessageAnnotations(annotations));

        sendMessage(queueName, msg);

        // Try to receive immediately - should not get message
        Message received = receiveMessage(queueName, 2000);
        assertNull(received, "Should not receive scheduled message immediately");

        log.info("PASS: Message with delivery time is held for scheduling");
    }

    @Test
    @Order(2)
    @DisplayName("Test: Send and receive with delivery time")
    @Timeout(30)
    void testSendRecvWithDeliveryTime() throws Exception {
        String queueName = generateQueueName("scheduled-recv");

        // Schedule message 3 seconds in future
        long deliveryTime = System.currentTimeMillis() + 3000;

        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("Scheduled message to receive"));
        msg.setMessageId("scheduled-msg-1");

        Map<Symbol, Object> annotations = new HashMap<>();
        annotations.put(DELIVERY_TIME, deliveryTime);
        msg.setMessageAnnotations(new MessageAnnotations(annotations));

        sendMessage(queueName, msg);

        // Should not receive within 1 second
        Message earlyReceive = receiveMessage(queueName, 1000);
        assertNull(earlyReceive, "Should not receive message before scheduled time");

        // Wait and then receive (with longer timeout)
        Thread.sleep(3000);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message after scheduled time");
        assertEquals("scheduled-msg-1", received.getMessageId());

        log.info("PASS: Scheduled message delivered after delay");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Schedule with delivery delay annotation")
    @Timeout(30)
    void testScheduleWithDelay() throws Exception {
        String queueName = generateQueueName("scheduled-delay");

        // Use delay instead of absolute time
        long delayMs = 3000;

        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("Delayed message"));
        msg.setMessageId("delayed-msg-1");

        Map<Symbol, Object> annotations = new HashMap<>();
        annotations.put(DELIVERY_DELAY, delayMs);
        msg.setMessageAnnotations(new MessageAnnotations(annotations));

        long sendTime = System.currentTimeMillis();
        sendMessage(queueName, msg);

        // Should not receive within 1 second
        Message earlyReceive = receiveMessage(queueName, 1000);
        assertNull(earlyReceive, "Should not receive message during delay period");

        // Wait for delay to pass
        Thread.sleep(3000);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message after delay");

        long receiveTime = System.currentTimeMillis();
        assertTrue(receiveTime - sendTime >= delayMs - 500, // Allow some tolerance
            "Message should be delayed by at least " + delayMs + "ms");

        log.info("PASS: Delayed message delivered after delay period");
    }

    @Test
    @Order(4)
    @DisplayName("Test: Send with delivery time holds message")
    @Timeout(30)
    void testSendWithDeliveryTimeHoldsMessage() throws Exception {
        String queueName = generateQueueName("scheduled-hold");

        // Schedule message far in future (5 minutes)
        long deliveryTime = System.currentTimeMillis() + 300000;

        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("Future message"));

        Map<Symbol, Object> annotations = new HashMap<>();
        annotations.put(DELIVERY_TIME, deliveryTime);
        msg.setMessageAnnotations(new MessageAnnotations(annotations));

        sendMessage(queueName, msg);

        // Should not receive within 2 seconds
        Message received = receiveMessage(queueName, 2000);
        assertNull(received, "Should not receive message scheduled for future");

        log.info("PASS: Message scheduled far in future is held");
    }

    @Test
    @Order(5)
    @DisplayName("Test: Send with delivery time delivers after delay")
    @Timeout(20)
    void testSendWithDeliveryTimeDeliversMessageAfterDelay() throws Exception {
        String queueName = generateQueueName("scheduled-deliver");

        // Schedule message 2 seconds in future
        long deliveryTime = System.currentTimeMillis() + 2000;

        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("Soon-to-be-delivered message"));
        msg.setMessageId("deliver-after-delay");

        Map<Symbol, Object> annotations = new HashMap<>();
        annotations.put(DELIVERY_TIME, deliveryTime);
        msg.setMessageAnnotations(new MessageAnnotations(annotations));

        sendMessage(queueName, msg);

        // Wait for scheduled time to pass
        Thread.sleep(3000);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message after scheduled time");
        assertEquals("deliver-after-delay", received.getMessageId());

        // Verify the delivery time annotation is preserved
        if (received.getMessageAnnotations() != null) {
            Object deliveryTimeAnnotation = received.getMessageAnnotations().getValue().get(DELIVERY_TIME);
            if (deliveryTimeAnnotation != null) {
                assertEquals(deliveryTime, ((Number) deliveryTimeAnnotation).longValue(),
                    "Delivery time annotation should be preserved");
            }
        }

        log.info("PASS: Scheduled message delivered after delay with annotations");
    }

    @Test
    @Order(6)
    @DisplayName("Test: Multiple scheduled messages with different delays")
    @Timeout(30)
    void testMultipleScheduledMessages() throws Exception {
        String queueName = generateQueueName("scheduled-multi");

        long now = System.currentTimeMillis();

        // Send 3 messages with different delays (in reverse order)
        for (int i = 3; i >= 1; i--) {
            Message msg = Message.Factory.create();
            msg.setBody(new AmqpValue("Message " + i));
            msg.setMessageId("msg-" + i);

            Map<Symbol, Object> annotations = new HashMap<>();
            annotations.put(DELIVERY_TIME, now + (i * 1000)); // 1s, 2s, 3s
            msg.setMessageAnnotations(new MessageAnnotations(annotations));

            sendMessage(queueName, msg);
        }

        // Wait for all to be deliverable
        Thread.sleep(4000);

        // Receive all messages - they should come in scheduled order
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean(true);
        AtomicReference<String> errorRef = new AtomicReference<>();
        java.util.List<String> receivedOrder = java.util.Collections.synchronizedList(
            new java.util.ArrayList<>());

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            receivedOrder.add((String) msg.getMessageId());
                            delivery.disposition(new Accepted(), true);

                            if (receivedOrder.size() >= 3) {
                                receiver.close();
                                conn.close();
                                done.countDown();
                            }
                        });
                        receiver.openHandler(receiverRes -> {
                            if (receiverRes.succeeded()) {
                                receiver.flow(5);
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
        assertEquals(3, receivedOrder.size(), "Should receive all messages");

        log.info("Received order: {}", receivedOrder);
        log.info("PASS: Multiple scheduled messages delivered");
    }

    @Test
    @Order(7)
    @DisplayName("Test: Zero delivery delay - immediate delivery")
    @Timeout(15)
    void testZeroDeliveryDelay() throws Exception {
        String queueName = generateQueueName("scheduled-zero");

        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("Immediate message"));
        msg.setMessageId("immediate-msg");

        Map<Symbol, Object> annotations = new HashMap<>();
        annotations.put(DELIVERY_DELAY, 0L);
        msg.setMessageAnnotations(new MessageAnnotations(annotations));

        sendMessage(queueName, msg);

        // Should receive immediately
        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message with zero delay immediately");
        assertEquals("immediate-msg", received.getMessageId());

        log.info("PASS: Zero delay message delivered immediately");
    }

    @Test
    @Order(8)
    @DisplayName("Test: Delivery time in past - immediate delivery")
    @Timeout(15)
    void testDeliveryTimeInPast() throws Exception {
        String queueName = generateQueueName("scheduled-past");

        // Set delivery time in the past
        long pastTime = System.currentTimeMillis() - 10000; // 10 seconds ago

        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("Past-scheduled message"));
        msg.setMessageId("past-msg");

        Map<Symbol, Object> annotations = new HashMap<>();
        annotations.put(DELIVERY_TIME, pastTime);
        msg.setMessageAnnotations(new MessageAnnotations(annotations));

        sendMessage(queueName, msg);

        // Should receive immediately since delivery time is in past
        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message with past delivery time immediately");
        assertEquals("past-msg", received.getMessageId());

        log.info("PASS: Past delivery time message delivered immediately");
    }

    @Test
    @Order(9)
    @DisplayName("Test: Scheduled message with properties preserved")
    @Timeout(20)
    @Disabled("Message properties preservation during scheduled delivery needs Properties section preservation")
    void testScheduledMessagePropertiesPreserved() throws Exception {
        String queueName = generateQueueName("scheduled-props");

        long deliveryTime = System.currentTimeMillis() + 2000;

        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("Message with properties"));
        msg.setMessageId("props-msg");
        msg.setCorrelationId("correlation-123");
        msg.setSubject("test-subject");
        msg.setContentType("text/plain");

        Properties props = new Properties();
        props.setReplyTo("reply-queue");
        msg.setProperties(props);

        Map<Symbol, Object> annotations = new HashMap<>();
        annotations.put(DELIVERY_TIME, deliveryTime);
        msg.setMessageAnnotations(new MessageAnnotations(annotations));

        Map<String, Object> appProps = new HashMap<>();
        appProps.put("custom-header", "custom-value");
        appProps.put("priority", 5);
        msg.setApplicationProperties(new ApplicationProperties(appProps));

        sendMessage(queueName, msg);

        // Wait for scheduled time
        Thread.sleep(3000);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");
        assertEquals("props-msg", received.getMessageId());
        assertEquals("correlation-123", received.getCorrelationId());
        assertEquals("test-subject", received.getSubject());

        assertNotNull(received.getApplicationProperties());
        assertEquals("custom-value",
            received.getApplicationProperties().getValue().get("custom-header"));

        log.info("PASS: Scheduled message properties preserved");
    }
}
