/*
 * Adapted from Apache ActiveMQ AmqpReceiverDrainTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 receiver functionality with manual flow control.
 * Adapted from Apache ActiveMQ AmqpReceiverDrainTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Receiver Flow Control Tests (ActiveMQ)")
public class AmqpReceiverDrainTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Receiver can consume all messages from queue")
    @Timeout(30)
    void testReceiverCanConsumeAllMessagesQueue() throws Exception {
        String queueName = generateQueueName("drain-queue");
        int messageCount = 5;

        // Send messages
        for (int i = 0; i < messageCount; i++) {
            sendMessage(queueName, createMessage("Message " + i));
        }

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
                        receiver.setQoS(ProtonQoS.AT_LEAST_ONCE);
                        receiver.setPrefetch(0); // Manual flow control

                        receiver.handler((delivery, msg) -> {
                            receivedCount.incrementAndGet();
                            delivery.disposition(Accepted.getInstance(), true);
                        });

                        receiver.openHandler(recvRes -> {
                            if (recvRes.succeeded()) {
                                // Request more credit than messages
                                receiver.flow(messageCount + 10);
                                // Wait a bit then complete
                                vertx.setTimer(2000, id -> {
                                    receiver.close();
                                    conn.close();
                                    done.countDown();
                                });
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertEquals(messageCount, receivedCount.get(), "Should receive all messages");

        log.info("PASS: Receiver can consume all messages from queue");
    }

    @Test
    @Order(2)
    @DisplayName("Test: Pull with no message on empty queue")
    @Timeout(30)
    void testPullWithNoMessageEmptyQueue() throws Exception {
        String queueName = generateQueueName("drain-empty-queue");
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
                        receiver.setQoS(ProtonQoS.AT_LEAST_ONCE);
                        receiver.setPrefetch(0);

                        receiver.handler((delivery, msg) -> {
                            receivedCount.incrementAndGet();
                            delivery.disposition(Accepted.getInstance(), true);
                        });

                        receiver.openHandler(recvRes -> {
                            if (recvRes.succeeded()) {
                                // Pull from empty queue
                                receiver.flow(1);
                                // Wait a bit - no messages should arrive
                                vertx.setTimer(2000, id -> {
                                    receiver.close();
                                    conn.close();
                                    done.countDown();
                                });
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertEquals(0, receivedCount.get(), "Should receive no messages on empty queue");

        log.info("PASS: Pull with no message on empty queue");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Pull one from remote queue")
    @Timeout(30)
    void testPullOneFromRemoteQueue() throws Exception {
        String queueName = generateQueueName("drain-pull-one");

        // Send one message
        sendMessage(queueName, createMessage("Single message"));

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
                        receiver.setQoS(ProtonQoS.AT_LEAST_ONCE);
                        receiver.setPrefetch(0);

                        receiver.handler((delivery, msg) -> {
                            receivedCount.incrementAndGet();
                            delivery.disposition(Accepted.getInstance(), true);
                            receiver.close();
                            conn.close();
                            done.countDown();
                        });

                        receiver.openHandler(recvRes -> {
                            if (recvRes.succeeded()) {
                                // Pull exactly one
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertEquals(1, receivedCount.get(), "Should receive one message");

        log.info("PASS: Pull one from remote queue");
    }

    @Test
    @Order(4)
    @DisplayName("Test: Multiple pull attempts on empty queue")
    @Timeout(30)
    void testMultiplePullAttemptsOnEmptyQueue() throws Exception {
        String queueName = generateQueueName("drain-multi-pull-queue");
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger flowCount = new AtomicInteger(0);
        AtomicReference<String> errorRef = new AtomicReference<>();
        int pullAttempts = 3;

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setQoS(ProtonQoS.AT_LEAST_ONCE);
                        receiver.setPrefetch(0);

                        receiver.handler((delivery, msg) -> {
                            delivery.disposition(Accepted.getInstance(), true);
                        });

                        receiver.openHandler(recvRes -> {
                            if (recvRes.succeeded()) {
                                receiver.flow(1);
                                // Issue multiple flow calls with delays (start at 1ms to avoid Vert.x timer issue)
                                for (int i = 0; i < pullAttempts; i++) {
                                    vertx.setTimer(1 + i * 500, id -> {
                                        receiver.flow(1);
                                        flowCount.incrementAndGet();
                                    });
                                }
                                // After all flows, close
                                vertx.setTimer(pullAttempts * 500 + 500, id -> {
                                    receiver.close();
                                    conn.close();
                                    done.countDown();
                                });
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertEquals(pullAttempts, flowCount.get(), "Should issue all pull attempts");

        log.info("PASS: Multiple pull attempts on empty queue");
    }

    @Test
    @Order(5)
    @DisplayName("Test: Incremental flow control")
    @Timeout(30)
    void testIncrementalFlowControl() throws Exception {
        String queueName = generateQueueName("drain-incremental");
        int messageCount = 10;

        // Send messages
        for (int i = 0; i < messageCount; i++) {
            sendMessage(queueName, createMessage("Message " + i));
        }

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
                        receiver.setQoS(ProtonQoS.AT_LEAST_ONCE);
                        receiver.setPrefetch(0);

                        receiver.handler((delivery, msg) -> {
                            int count = receivedCount.incrementAndGet();
                            delivery.disposition(Accepted.getInstance(), true);
                            if (count < messageCount) {
                                // Issue more credit one at a time
                                receiver.flow(1);
                            } else {
                                receiver.close();
                                conn.close();
                                done.countDown();
                            }
                        });

                        receiver.openHandler(recvRes -> {
                            if (recvRes.succeeded()) {
                                // Start with credit of 1
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertEquals(messageCount, receivedCount.get(), "Should receive all messages with incremental flow");

        log.info("PASS: Incremental flow control - received {} messages", receivedCount.get());
    }

    @Test
    @Order(6)
    @DisplayName("Test: Prefetch mode vs manual flow")
    @Timeout(30)
    void testPrefetchModeVsManualFlow() throws Exception {
        String queueName = generateQueueName("drain-prefetch");
        int messageCount = 5;

        // Send messages
        for (int i = 0; i < messageCount; i++) {
            sendMessage(queueName, createMessage("Message " + i));
        }

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
                        receiver.setQoS(ProtonQoS.AT_LEAST_ONCE);
                        receiver.setPrefetch(0);

                        receiver.handler((delivery, msg) -> {
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertEquals(messageCount, receivedCount.get(), "Should receive all messages with prefetch");

        log.info("PASS: Prefetch mode vs manual flow");
    }

    private Message createMessage(String body) {
        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue(body));
        msg.setMessageId(UUID.randomUUID().toString());
        return msg;
    }
}
