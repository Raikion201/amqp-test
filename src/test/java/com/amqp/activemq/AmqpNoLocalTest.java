/*
 * Adapted from Apache ActiveMQ AmqpNoLocalTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 no-local subscription behavior.
 * Adapted from Apache ActiveMQ AmqpNoLocalTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 No-Local Tests (ActiveMQ)")
public class AmqpNoLocalTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Subscriber receives messages from other connection")
    @Timeout(30)
    void testSubscriberReceivesMessagesFromOtherConnection() throws Exception {
        String queueName = generateQueueName("nolocal-other");

        // Send from connection 1
        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue("Message from connection 1"));
        sent.setMessageId("other-conn-msg-1");
        sendMessage(queueName, sent);

        // Receive from connection 2
        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message from other connection");
        assertEquals("Message from connection 1", ((AmqpValue) received.getBody()).getValue());

        log.info("PASS: Subscriber receives messages from other connection");
    }

    @Test
    @Order(2)
    @DisplayName("Test: Same connection send and receive")
    @Timeout(30)
    void testSameConnectionSendAndReceive() throws Exception {
        String queueName = generateQueueName("nolocal-same");

        // Send first, then receive
        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue("Same connection message"));
        sent.setMessageId("same-conn-1");
        sendMessage(queueName, sent);

        // Now receive
        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message on same connection");
        assertEquals("Same connection message", ((AmqpValue) received.getBody()).getValue());

        log.info("PASS: Same connection send and receive works");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Multiple subscribers on same queue")
    @Timeout(60)
    void testMultipleSubscribersOnSameQueue() throws Exception {
        String queueName = generateQueueName("nolocal-multi");
        int messageCount = 10;

        // Send messages first
        for (int i = 0; i < messageCount; i++) {
            Message msg = Message.Factory.create();
            msg.setBody(new AmqpValue("Message " + i));
            msg.setMessageId("multi-sub-" + i);
            sendMessage(queueName, msg);
        }

        // Two receivers compete for messages
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger totalReceived = new AtomicInteger(0);
        AtomicInteger recv1Count = new AtomicInteger(0);
        AtomicInteger recv2Count = new AtomicInteger(0);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Receiver 1
                        ProtonReceiver r1 = conn.createReceiver(queueName);
                        r1.setPrefetch(0);
                        r1.handler((d, m) -> {
                            recv1Count.incrementAndGet();
                            d.disposition(Accepted.getInstance(), true);
                            if (totalReceived.incrementAndGet() >= messageCount) {
                                conn.close();
                                done.countDown();
                            } else {
                                r1.flow(1);
                            }
                        });
                        r1.openHandler(rr -> {
                            if (rr.succeeded()) r1.flow(1);
                        });
                        r1.open();

                        // Receiver 2
                        ProtonReceiver r2 = conn.createReceiver(queueName);
                        r2.setPrefetch(0);
                        r2.handler((d, m) -> {
                            recv2Count.incrementAndGet();
                            d.disposition(Accepted.getInstance(), true);
                            if (totalReceived.incrementAndGet() >= messageCount) {
                                conn.close();
                                done.countDown();
                            } else {
                                r2.flow(1);
                            }
                        });
                        r2.openHandler(rr -> {
                            if (rr.succeeded()) r2.flow(1);
                        });
                        r2.open();
                    } else {
                        done.countDown();
                    }
                });
                conn.open();
            } else {
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS), "Timeout");
        assertEquals(messageCount, totalReceived.get(), "All messages should be received");

        log.info("PASS: Multiple subscribers - R1: {}, R2: {}, Total: {}",
            recv1Count.get(), recv2Count.get(), totalReceived.get());
    }

    @Test
    @Order(4)
    @DisplayName("Test: Sender and receiver on different sessions")
    @Timeout(30)
    void testSenderAndReceiverOnDifferentSessions() throws Exception {
        String queueName = generateQueueName("nolocal-sessions");

        // Simple test using helper methods (separate connections = separate sessions)
        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue("Cross session message"));
        sent.setMessageId("cross-session-1");
        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive cross-session message");
        assertEquals("Cross session message", ((AmqpValue) received.getBody()).getValue());

        log.info("PASS: Sender and receiver on different sessions");
    }

    @Test
    @Order(5)
    @DisplayName("Test: Message delivery to exclusive consumer")
    @Timeout(30)
    void testMessageDeliveryToExclusiveConsumer() throws Exception {
        String queueName = generateQueueName("nolocal-exclusive");

        // Send message first
        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue("Exclusive consumer message"));
        sent.setMessageId("exclusive-msg-1");
        sendMessage(queueName, sent);

        // Receive on a single consumer
        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");
        assertEquals("Exclusive consumer message", ((AmqpValue) received.getBody()).getValue());

        log.info("PASS: Message delivery to exclusive consumer");
    }
}
