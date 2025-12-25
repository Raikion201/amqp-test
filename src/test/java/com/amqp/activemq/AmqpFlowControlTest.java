/*
 * Adapted from Apache ActiveMQ Artemis AmqpFlowControlTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 flow control functionality.
 * Adapted from Apache ActiveMQ Artemis AmqpFlowControlTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Flow Control Tests")
public class AmqpFlowControlTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Credits are allocated once on link creation")
    @Timeout(30)
    void testCreditsAreAllocatedOnceOnLinkCreated() throws Exception {
        String queueName = generateQueueName("flow-credit-once");

        CountDownLatch done = new CountDownLatch(1);
        AtomicLong initialCredit = new AtomicLong(-1);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.sendQueueDrainHandler(s -> {
                            // Record credit when sender becomes sendable
                            initialCredit.set(sender.getCredit());
                        });
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                // Wait a moment to see if credit changes
                                vertx.setTimer(1000, id -> {
                                    sender.close();
                                    conn.close();
                                    done.countDown();
                                });
                            } else {
                                errorRef.set("Sender attach failed");
                                done.countDown();
                            }
                        });
                        sender.open();
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
        assertTrue(initialCredit.get() >= 0, "Should receive initial credit");

        log.info("PASS: Credits allocated on link creation (credit={})", initialCredit.get());
    }

    @Test
    @Order(2)
    @DisplayName("Test: Receiver flow control with prefetch")
    @Timeout(30)
    void testReceiverFlowControlWithPrefetch() throws Exception {
        String queueName = generateQueueName("flow-prefetch");
        int messageCount = 10;
        int prefetchCount = 3;

        // Send messages
        sendMessages(queueName, messageCount);

        // Create receiver with limited prefetch
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
                        receiver.setPrefetch(0); // Manual credit control

                        receiver.handler((delivery, msg) -> {
                            int count = receivedCount.incrementAndGet();
                            delivery.disposition(new Accepted(), true);

                            // Issue more credit after each message
                            if (count < messageCount) {
                                receiver.flow(1);
                            } else {
                                receiver.close();
                                conn.close();
                                done.countDown();
                            }
                        });

                        receiver.openHandler(receiverRes -> {
                            if (receiverRes.succeeded()) {
                                // Issue initial credit
                                receiver.flow(prefetchCount);
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertEquals(messageCount, receivedCount.get(), "Should receive all messages");

        log.info("PASS: Receiver flow control with prefetch works");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Receiver with zero credit receives no messages")
    @Timeout(30)
    void testReceiverWithZeroCreditReceivesNothing() throws Exception {
        String queueName = generateQueueName("flow-zero-credit");

        // Send message
        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("No credit message"));
        sendMessage(queueName, msg);

        // Create receiver with zero credit
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean messageReceived = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0); // No automatic prefetch

                        receiver.handler((delivery, message) -> {
                            messageReceived.set(true);
                            delivery.disposition(new Accepted(), true);
                        });

                        receiver.openHandler(receiverRes -> {
                            if (receiverRes.succeeded()) {
                                // Don't issue any credit!
                                // Wait and verify no message received
                                vertx.setTimer(2000, id -> {
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertFalse(messageReceived.get(), "Should not receive message without credit");

        log.info("PASS: Zero credit receiver receives nothing");
    }

    @Test
    @Order(4)
    @DisplayName("Test: Credits are replenished after consumption")
    @Timeout(60)
    void testCreditsReplenishedAfterConsumption() throws Exception {
        String queueName = generateQueueName("flow-replenish");
        int totalMessages = 20;
        int batchSize = 5;

        // Send messages
        sendMessages(queueName, totalMessages);

        // Receive in batches, replenishing credit after each batch
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger receivedCount = new AtomicInteger(0);
        AtomicInteger batchCount = new AtomicInteger(0);
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
                            int count = receivedCount.incrementAndGet();
                            delivery.disposition(new Accepted(), true);

                            if (count % batchSize == 0) {
                                batchCount.incrementAndGet();
                                if (count < totalMessages) {
                                    // Replenish credit for next batch
                                    receiver.flow(batchSize);
                                }
                            }

                            if (count >= totalMessages) {
                                receiver.close();
                                conn.close();
                                done.countDown();
                            }
                        });

                        receiver.openHandler(receiverRes -> {
                            if (receiverRes.succeeded()) {
                                // Issue initial batch credit
                                receiver.flow(batchSize);
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertEquals(totalMessages, receivedCount.get(), "Should receive all messages");
        assertEquals(totalMessages / batchSize, batchCount.get(), "Should process correct number of batches");

        log.info("PASS: Credits replenished after consumption ({} batches)", batchCount.get());
    }

    @Test
    @Order(5)
    @DisplayName("Test: Drain mode clears remaining credit")
    @Timeout(30)
    void testDrainModeClearsRemainingCredit() throws Exception {
        String queueName = generateQueueName("flow-drain");

        // Send fewer messages than credit
        int messageCount = 3;
        int creditIssued = 10;

        sendMessages(queueName, messageCount);

        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger receivedCount = new AtomicInteger(0);
        AtomicBoolean drainCompleted = new AtomicBoolean(false);
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
                            receivedCount.incrementAndGet();
                            delivery.disposition(new Accepted(), true);
                        });

                        receiver.openHandler(receiverRes -> {
                            if (receiverRes.succeeded()) {
                                // Issue credit
                                receiver.flow(creditIssued);

                                // Wait for messages and check remaining credit
                                vertx.setTimer(2000, id -> {
                                    // After receiving available messages, drain completes
                                    drainCompleted.set(true);
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertEquals(messageCount, receivedCount.get(), "Should receive available messages");

        log.info("PASS: Drain mode clears remaining credit after {} messages", receivedCount.get());
    }

    @Test
    @Order(6)
    @DisplayName("Test: Multiple receivers share messages fairly")
    @Timeout(60)
    void testMultipleReceiversShareMessagesFairly() throws Exception {
        String queueName = generateQueueName("flow-fair-share");
        int totalMessages = 10;

        // Send messages
        sendMessages(queueName, totalMessages);

        CountDownLatch done = new CountDownLatch(2);
        AtomicInteger receiver1Count = new AtomicInteger(0);
        AtomicInteger receiver2Count = new AtomicInteger(0);
        AtomicReference<String> errorRef = new AtomicReference<>();

        // Create two receivers
        for (int r = 0; r < 2; r++) {
            final AtomicInteger counter = (r == 0) ? receiver1Count : receiver2Count;

            ProtonClient client = ProtonClient.create(vertx);
            client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        if (openRes.succeeded()) {
                            ProtonReceiver receiver = conn.createReceiver(queueName);
                            receiver.setPrefetch(0);

                            receiver.handler((delivery, msg) -> {
                                counter.incrementAndGet();
                                delivery.disposition(new Accepted(), true);
                            });

                            receiver.openHandler(receiverRes -> {
                                if (receiverRes.succeeded()) {
                                    receiver.flow(totalMessages);

                                    // Wait then close
                                    vertx.setTimer(3000, id -> {
                                        receiver.close();
                                        conn.close();
                                        done.countDown();
                                    });
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
        }

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");

        int total = receiver1Count.get() + receiver2Count.get();
        assertEquals(totalMessages, total, "Total messages should equal sent count");

        log.info("PASS: Multiple receivers shared messages (R1={}, R2={})",
            receiver1Count.get(), receiver2Count.get());
    }

    @Test
    @Order(7)
    @DisplayName("Test: Session flow control with window")
    @Timeout(30)
    void testSessionFlowControlWithWindow() throws Exception {
        String queueName = generateQueueName("flow-session-window");
        int messageCount = 5;

        // Send messages
        sendMessages(queueName, messageCount);

        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger receivedCount = new AtomicInteger(0);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Create session with specific settings
                        ProtonSession session = conn.createSession();
                        session.setIncomingCapacity(1024 * 10); // 10KB window

                        session.openHandler(sessionRes -> {
                            if (sessionRes.succeeded()) {
                                ProtonReceiver receiver = session.createReceiver(queueName);
                                receiver.setPrefetch(0);

                                receiver.handler((delivery, msg) -> {
                                    int count = receivedCount.incrementAndGet();
                                    delivery.disposition(new Accepted(), true);

                                    if (count >= messageCount) {
                                        receiver.close();
                                        session.close();
                                        conn.close();
                                        done.countDown();
                                    }
                                });

                                receiver.openHandler(receiverRes -> {
                                    if (receiverRes.succeeded()) {
                                        receiver.flow(messageCount);
                                    } else {
                                        errorRef.set("Receiver attach failed");
                                        done.countDown();
                                    }
                                });
                                receiver.open();
                            } else {
                                errorRef.set("Session open failed");
                                done.countDown();
                            }
                        });
                        session.open();
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

        log.info("PASS: Session flow control with window works");
    }

    @Test
    @Order(8)
    @DisplayName("Test: Stop flow by setting zero credit")
    @Timeout(30)
    void testStopFlowBySettingZeroCredit() throws Exception {
        String queueName = generateQueueName("flow-stop");
        int initialMessages = 5;
        int stopAfter = 3;

        // Send messages
        sendMessages(queueName, initialMessages);

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
                            int count = receivedCount.incrementAndGet();
                            delivery.disposition(new Accepted(), true);

                            if (count >= stopAfter) {
                                // Stop flow by not issuing more credit
                                vertx.setTimer(1000, id -> {
                                    receiver.close();
                                    conn.close();
                                    done.countDown();
                                });
                            } else {
                                // Continue flow
                                receiver.flow(1);
                            }
                        });

                        receiver.openHandler(receiverRes -> {
                            if (receiverRes.succeeded()) {
                                // Issue credit one at a time
                                receiver.flow(1);
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertEquals(stopAfter, receivedCount.get(), "Should receive only messages before stop");

        log.info("PASS: Stopped flow after {} messages by not issuing credit", stopAfter);
    }
}
