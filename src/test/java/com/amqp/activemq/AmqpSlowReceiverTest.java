/*
 * Adapted from Apache ActiveMQ AmqpSlowReceiverTest.
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 slow receiver handling.
 * Adapted from Apache ActiveMQ AmqpSlowReceiverTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Slow Receiver Tests (ActiveMQ)")
public class AmqpSlowReceiverTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Slow receiver with flow control")
    @Timeout(60)
    void testSlowReceiverWithFlowControl() throws Exception {
        String queueName = generateQueueName("slow-flow");
        int messageCount = 20;

        // Send messages
        for (int i = 0; i < messageCount; i++) {
            Message msg = Message.Factory.create();
            msg.setBody(new AmqpValue("Message " + i));
            msg.setMessageId("msg-" + i);
            sendMessage(queueName, msg);
        }

        // Slow receiver - only request one message at a time
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger receivedCount = new AtomicInteger(0);
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
                            // Simulate slow processing
                            try {
                                Thread.sleep(50);
                            } catch (InterruptedException e) {
                                // ignore
                            }

                            int count = receivedCount.incrementAndGet();
                            delivery.disposition(Accepted.getInstance(), true);

                            if (count >= messageCount) {
                                receiver.close();
                                conn.close();
                                done.countDown();
                            } else {
                                // Request next message
                                receiver.flow(1);
                            }
                        });
                        receiver.openHandler(recvRes -> {
                            if (recvRes.succeeded()) {
                                receiver.flow(1);
                            } else {
                                errorRef.set("Receiver open failed");
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

        assertTrue(done.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertEquals(messageCount, receivedCount.get(), "Should receive all messages");

        log.info("PASS: Slow receiver with flow control received {} messages", receivedCount.get());
    }

    @Test
    @Order(2)
    @DisplayName("Test: Receiver with limited credit")
    @Timeout(60)
    void testReceiverWithLimitedCredit() throws Exception {
        String queueName = generateQueueName("slow-limited");
        int messageCount = 10;
        int creditLimit = 2;

        // Send messages
        for (int i = 0; i < messageCount; i++) {
            Message msg = Message.Factory.create();
            msg.setBody(new AmqpValue("Message " + i));
            msg.setMessageId("msg-" + i);
            sendMessage(queueName, msg);
        }

        // Receiver with limited credit
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger receivedCount = new AtomicInteger(0);
        AtomicInteger creditIssued = new AtomicInteger(0);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            int count = receivedCount.incrementAndGet();
                            delivery.disposition(Accepted.getInstance(), true);

                            if (count >= messageCount) {
                                receiver.close();
                                conn.close();
                                done.countDown();
                            } else if (count % creditLimit == 0) {
                                // Issue more credit in batches
                                receiver.flow(creditLimit);
                                creditIssued.addAndGet(creditLimit);
                            }
                        });
                        receiver.openHandler(recvRes -> {
                            if (recvRes.succeeded()) {
                                receiver.flow(creditLimit);
                                creditIssued.addAndGet(creditLimit);
                            } else {
                                done.countDown();
                            }
                        });
                        receiver.open();
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
        assertEquals(messageCount, receivedCount.get(), "Should receive all messages");

        log.info("PASS: Receiver with limited credit received {} messages", receivedCount.get());
    }

    @Test
    @Order(3)
    @DisplayName("Test: Multiple slow receivers sharing queue")
    @Timeout(90)
    void testMultipleSlowReceiversSharingQueue() throws Exception {
        String queueName = generateQueueName("slow-shared");
        int messageCount = 20;

        // Send messages
        for (int i = 0; i < messageCount; i++) {
            Message msg = Message.Factory.create();
            msg.setBody(new AmqpValue("Message " + i));
            msg.setMessageId("msg-" + i);
            sendMessage(queueName, msg);
        }

        // Two slow receivers
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger totalReceived = new AtomicInteger(0);
        AtomicInteger receiver1Count = new AtomicInteger(0);
        AtomicInteger receiver2Count = new AtomicInteger(0);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Receiver 1
                        ProtonReceiver r1 = conn.createReceiver(queueName);
                        r1.setPrefetch(0);
                        r1.handler((delivery, msg) -> {
                            try {
                                Thread.sleep(30);
                            } catch (InterruptedException e) {
                                // ignore
                            }
                            receiver1Count.incrementAndGet();
                            delivery.disposition(Accepted.getInstance(), true);
                            if (totalReceived.incrementAndGet() >= messageCount) {
                                r1.close();
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
                        r2.handler((delivery, msg) -> {
                            try {
                                Thread.sleep(30);
                            } catch (InterruptedException e) {
                                // ignore
                            }
                            receiver2Count.incrementAndGet();
                            delivery.disposition(Accepted.getInstance(), true);
                            if (totalReceived.incrementAndGet() >= messageCount) {
                                r2.close();
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

        assertTrue(done.await(TIMEOUT_SECONDS * 3, TimeUnit.SECONDS), "Timeout");
        assertEquals(messageCount, totalReceived.get(), "Should receive all messages");

        log.info("PASS: Multiple slow receivers - R1: {}, R2: {}, Total: {}",
            receiver1Count.get(), receiver2Count.get(), totalReceived.get());
    }

    @Test
    @Order(4)
    @DisplayName("Test: Receiver pauses and resumes")
    @Timeout(60)
    void testReceiverPausesAndResumes() throws Exception {
        String queueName = generateQueueName("slow-pause");
        int messageCount = 10;

        // Send messages
        for (int i = 0; i < messageCount; i++) {
            Message msg = Message.Factory.create();
            msg.setBody(new AmqpValue("Message " + i));
            msg.setMessageId("msg-" + i);
            sendMessage(queueName, msg);
        }

        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger receivedCount = new AtomicInteger(0);
        AtomicInteger pauseCount = new AtomicInteger(0);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            int count = receivedCount.incrementAndGet();
                            delivery.disposition(Accepted.getInstance(), true);

                            if (count >= messageCount) {
                                receiver.close();
                                conn.close();
                                done.countDown();
                            } else if (count == 3 || count == 6) {
                                // Pause - don't issue credit
                                pauseCount.incrementAndGet();
                                // Resume after delay
                                vertx.setTimer(200, id -> receiver.flow(1));
                            } else {
                                receiver.flow(1);
                            }
                        });
                        receiver.openHandler(rr -> {
                            if (rr.succeeded()) receiver.flow(1);
                            else done.countDown();
                        });
                        receiver.open();
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
        assertEquals(messageCount, receivedCount.get(), "Should receive all messages");
        assertEquals(2, pauseCount.get(), "Should have paused twice");

        log.info("PASS: Receiver paused {} times and received all {} messages",
            pauseCount.get(), receivedCount.get());
    }
}
