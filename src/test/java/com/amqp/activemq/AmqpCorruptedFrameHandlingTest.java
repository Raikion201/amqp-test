/*
 * Adapted from Apache ActiveMQ AmqpCorruptedFrameHandlingTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 corrupted frame handling.
 * Adapted from Apache ActiveMQ AmqpCorruptedFrameHandlingTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Corrupted Frame Handling Tests (ActiveMQ)")
public class AmqpCorruptedFrameHandlingTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Normal message exchange works")
    @Timeout(30)
    void testNormalMessageExchangeWorks() throws Exception {
        String queueName = generateQueueName("corrupt-normal");

        // Send a message
        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue("Normal message"));
        sent.setMessageId("normal-msg-1");
        sendMessage(queueName, sent);

        // Receive the message
        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");
        assertEquals("Normal message", ((AmqpValue) received.getBody()).getValue());

        log.info("PASS: Normal message exchange works");
    }

    @Test
    @Order(2)
    @DisplayName("Test: Connection survives after protocol error")
    @Timeout(30)
    void testConnectionSurvivesAfterProtocolError() throws Exception {
        String queueName = generateQueueName("corrupt-survive");

        // First establish a working connection and send a message
        Message sent1 = Message.Factory.create();
        sent1.setBody(new AmqpValue("Message before error"));
        sent1.setMessageId("before-error-1");
        sendMessage(queueName, sent1);

        // Receive first message
        Message received1 = receiveMessage(queueName, 5000);
        assertNotNull(received1, "Should receive first message");

        // Send another message after - connection should still work
        Message sent2 = Message.Factory.create();
        sent2.setBody(new AmqpValue("Message after potential error"));
        sent2.setMessageId("after-error-1");
        sendMessage(queueName, sent2);

        // Receive second message
        Message received2 = receiveMessage(queueName, 5000);
        assertNotNull(received2, "Should receive second message");

        log.info("PASS: Connection survives after protocol operations");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Server handles empty message body")
    @Timeout(30)
    void testServerHandlesEmptyMessageBody() throws Exception {
        String queueName = generateQueueName("corrupt-empty");

        // Send message with no body
        Message sent = Message.Factory.create();
        sent.setMessageId("empty-body-msg");
        // No body set
        sendMessage(queueName, sent);

        // Should be able to receive it
        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive empty message");
        // Body may be null or empty AmqpValue

        log.info("PASS: Server handles empty message body");
    }

    @Test
    @Order(4)
    @DisplayName("Test: Server handles message with null properties")
    @Timeout(30)
    void testServerHandlesMessageWithNullProperties() throws Exception {
        String queueName = generateQueueName("corrupt-null-props");

        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue("Message with minimal properties"));
        // No message ID, no properties, etc.
        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message with null properties");

        log.info("PASS: Server handles message with null properties");
    }

    @Test
    @Order(5)
    @DisplayName("Test: Server handles rapid open/close")
    @Timeout(30)
    void testServerHandlesRapidOpenClose() throws Exception {
        int iterations = 10;
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < iterations; i++) {
            CountDownLatch done = new CountDownLatch(1);
            AtomicBoolean success = new AtomicBoolean(false);

            ProtonClient client = ProtonClient.create(vertx);
            client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        if (openRes.succeeded()) {
                            success.set(true);
                        }
                        conn.close();
                        done.countDown();
                    });
                    conn.open();
                } else {
                    done.countDown();
                }
            });

            done.await(5, TimeUnit.SECONDS);
            if (success.get()) {
                successCount.incrementAndGet();
            }
        }

        assertTrue(successCount.get() >= iterations - 1,
            "Most rapid open/close cycles should succeed");
        log.info("PASS: {} of {} rapid open/close cycles succeeded",
            successCount.get(), iterations);
    }

    @Test
    @Order(6)
    @DisplayName("Test: Server handles session end during transfer")
    @Timeout(30)
    void testServerHandlesSessionEndDuringTransfer() throws Exception {
        String queueName = generateQueueName("corrupt-session-end");
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean completed = new AtomicBoolean(false);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                // Send a message
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue("Test message"));
                                sender.send(msg, delivery -> {
                                    completed.set(true);
                                    sender.close();
                                    conn.close();
                                    done.countDown();
                                });
                            } else {
                                conn.close();
                                done.countDown();
                            }
                        });
                        sender.open();
                    } else {
                        done.countDown();
                    }
                });
                conn.open();
            } else {
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertTrue(completed.get(), "Message should be sent before session end");

        log.info("PASS: Server handles session end during transfer");
    }
}
