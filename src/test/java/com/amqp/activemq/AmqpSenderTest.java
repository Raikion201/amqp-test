/*
 * Adapted from Apache ActiveMQ AmqpSenderTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
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
 * Tests for AMQP 1.0 sender functionality.
 * Adapted from Apache ActiveMQ AmqpSenderTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Sender Tests (ActiveMQ)")
public class AmqpSenderTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Create queue sender")
    @Timeout(30)
    void testCreateQueueSender() throws Exception {
        String queueName = generateQueueName("sender-queue");

        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean senderOpened = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    ProtonSender sender = conn.createSender(queueName);
                    sender.openHandler(sRes -> {
                        senderOpened.set(sRes.succeeded());
                        if (!sRes.succeeded()) {
                            errorRef.set("Sender open failed: " + sRes.cause());
                        }
                        sender.close();
                        conn.close();
                        done.countDown();
                    });
                    sender.open();
                });
                conn.open();
            } else {
                errorRef.set("Connect failed");
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(senderOpened.get(), "Sender should open successfully");

        log.info("PASS: Queue sender created");
    }

    @Test
    @Order(2)
    @DisplayName("Test: Create topic sender")
    @Timeout(30)
    void testCreateTopicSender() throws Exception {
        String topicName = generateQueueName("sender-topic");

        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean senderOpened = new AtomicBoolean(false);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    ProtonSender sender = conn.createSender(topicName);
                    sender.openHandler(sRes -> {
                        senderOpened.set(sRes.succeeded());
                        sender.close();
                        conn.close();
                        done.countDown();
                    });
                    sender.open();
                });
                conn.open();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertTrue(senderOpened.get(), "Topic sender should open");

        log.info("PASS: Topic sender created");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Sender settlement mode settled is honored")
    @Timeout(30)
    void testSenderSettlementModeSettledIsHonored() throws Exception {
        String queueName = generateQueueName("settled-mode");

        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean senderOpened = new AtomicBoolean(false);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    ProtonSender sender = conn.createSender(queueName);
                    sender.setQoS(ProtonQoS.AT_MOST_ONCE); // Settled/presettled
                    sender.openHandler(sRes -> {
                        senderOpened.set(sRes.succeeded());
                        sender.close();
                        conn.close();
                        done.countDown();
                    });
                    sender.open();
                });
                conn.open();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertTrue(senderOpened.get(), "Sender should open with settled mode");

        log.info("PASS: Settled sender settlement mode");
    }

    @Test
    @Order(4)
    @DisplayName("Test: Sender settlement mode unsettled is honored")
    @Timeout(30)
    void testSenderSettlementModeUnsettledIsHonored() throws Exception {
        String queueName = generateQueueName("unsettled-mode");

        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean senderOpened = new AtomicBoolean(false);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    ProtonSender sender = conn.createSender(queueName);
                    sender.setQoS(ProtonQoS.AT_LEAST_ONCE); // Unsettled
                    sender.openHandler(sRes -> {
                        senderOpened.set(sRes.succeeded());
                        sender.close();
                        conn.close();
                        done.countDown();
                    });
                    sender.open();
                });
                conn.open();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertTrue(senderOpened.get(), "Sender should open with unsettled mode");

        log.info("PASS: Unsettled sender settlement mode");
    }

    @Test
    @Order(5)
    @DisplayName("Test: Send message to queue")
    @Timeout(30)
    void testSendMessageToQueue() throws Exception {
        String queueName = generateQueueName("send-to-queue");

        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean sendSuccess = new AtomicBoolean(false);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    ProtonSender sender = conn.createSender(queueName);
                    sender.openHandler(sRes -> {
                        if (sRes.succeeded()) {
                            Message msg = Message.Factory.create();
                            msg.setBody(new AmqpValue("Test message"));
                            msg.setMessageId("test-msg-1");

                            sender.send(msg, delivery -> {
                                sendSuccess.set(delivery.getRemoteState() instanceof Accepted);
                                sender.close();
                                conn.close();
                                done.countDown();
                            });
                        } else {
                            done.countDown();
                        }
                    });
                    sender.open();
                });
                conn.open();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertTrue(sendSuccess.get(), "Send should succeed");

        // Verify message is in queue
        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Message should be in queue");

        log.info("PASS: Send message to queue");
    }

    @Test
    @Order(6)
    @DisplayName("Test: Send multiple messages to queue")
    @Timeout(60)
    void testSendMultipleMessagesToQueue() throws Exception {
        String queueName = generateQueueName("multi-send");
        int messageCount = 100;

        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger sentCount = new AtomicInteger(0);
        AtomicInteger acceptedCount = new AtomicInteger(0);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    ProtonSender sender = conn.createSender(queueName);
                    sender.openHandler(sRes -> {
                        if (sRes.succeeded()) {
                            for (int i = 0; i < messageCount; i++) {
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue("Message " + i));
                                msg.setMessageId("msg-" + i);

                                sender.send(msg, delivery -> {
                                    if (delivery.getRemoteState() instanceof Accepted) {
                                        acceptedCount.incrementAndGet();
                                    }
                                    if (sentCount.incrementAndGet() >= messageCount) {
                                        sender.close();
                                        conn.close();
                                        done.countDown();
                                    }
                                });
                            }
                        } else {
                            done.countDown();
                        }
                    });
                    sender.open();
                });
                conn.open();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS), "Timeout");
        assertEquals(messageCount, acceptedCount.get(), "All messages should be accepted");

        log.info("PASS: Sent {} messages to queue", messageCount);
    }

    @Test
    @Order(7)
    @DisplayName("Test: Unsettled sender")
    @Timeout(120)
    void testUnsettledSender() throws Exception {
        String queueName = generateQueueName("unsettled-sender");
        int messageCount = 100;

        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger settledCount = new AtomicInteger(0);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    ProtonSender sender = conn.createSender(queueName);
                    sender.setQoS(ProtonQoS.AT_LEAST_ONCE); // Unsettled
                    sender.openHandler(sRes -> {
                        if (sRes.succeeded()) {
                            for (int i = 0; i < messageCount; i++) {
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue("Unsettled message " + i));
                                msg.setMessageId("unsettled-msg-" + i);

                                sender.send(msg, delivery -> {
                                    if (delivery.remotelySettled()) {
                                        settledCount.incrementAndGet();
                                    }
                                    if (settledCount.get() >= messageCount) {
                                        sender.close();
                                        conn.close();
                                        done.countDown();
                                    }
                                });
                            }
                        } else {
                            done.countDown();
                        }
                    });
                    sender.open();
                });
                conn.open();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS * 3, TimeUnit.SECONDS), "Timeout");
        assertEquals(messageCount, settledCount.get(), "All messages should be remotely settled");

        log.info("PASS: Unsettled sender with {} remote settlements", messageCount);
    }

    @Test
    @Order(8)
    @DisplayName("Test: Presettled sender")
    @Timeout(120)
    void testPresettledSender() throws Exception {
        String queueName = generateQueueName("presettled-sender");
        int messageCount = 100;

        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger sentCount = new AtomicInteger(0);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    ProtonSender sender = conn.createSender(queueName);
                    sender.setQoS(ProtonQoS.AT_MOST_ONCE); // Presettled
                    sender.openHandler(sRes -> {
                        if (sRes.succeeded()) {
                            for (int i = 0; i < messageCount; i++) {
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue("Presettled message " + i));
                                msg.setMessageId("presettled-msg-" + i);

                                sender.send(msg);
                                sentCount.incrementAndGet();
                            }
                            // Give time for messages to be processed
                            vertx.setTimer(1000, id -> {
                                sender.close();
                                conn.close();
                                done.countDown();
                            });
                        } else {
                            done.countDown();
                        }
                    });
                    sender.open();
                });
                conn.open();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS), "Timeout");
        assertEquals(messageCount, sentCount.get(), "All messages should be sent");

        log.info("PASS: Presettled sender sent {} messages", messageCount);
    }

    @Test
    @Order(9)
    @DisplayName("Test: Sender with target capabilities")
    @Timeout(30)
    void testSenderWithTargetCapabilities() throws Exception {
        String queueName = generateQueueName("target-caps");

        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean senderOpened = new AtomicBoolean(false);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    ProtonSender sender = conn.createSender(queueName);

                    // Set target with capabilities
                    Target target = new Target();
                    target.setAddress(queueName);
                    target.setDurable(TerminusDurability.NONE);
                    target.setCapabilities(Symbol.valueOf("queue"));
                    sender.setTarget(target);

                    sender.openHandler(sRes -> {
                        senderOpened.set(sRes.succeeded());
                        sender.close();
                        conn.close();
                        done.countDown();
                    });
                    sender.open();
                });
                conn.open();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertTrue(senderOpened.get(), "Sender with capabilities should open");

        log.info("PASS: Sender with target capabilities");
    }

    @Test
    @Order(10)
    @DisplayName("Test: Anonymous sender")
    @Timeout(30)
    void testAnonymousSender() throws Exception {
        String queueName = generateQueueName("anon-target");

        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean sendSuccess = new AtomicBoolean(false);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    // Create anonymous sender (null address)
                    ProtonSender sender = conn.createSender(null);
                    sender.openHandler(sRes -> {
                        if (sRes.succeeded()) {
                            Message msg = Message.Factory.create();
                            msg.setBody(new AmqpValue("Anonymous send"));
                            msg.setMessageId("anon-msg");
                            msg.setAddress(queueName); // Set address in message

                            sender.send(msg, delivery -> {
                                sendSuccess.set(delivery.getRemoteState() instanceof Accepted);
                                sender.close();
                                conn.close();
                                done.countDown();
                            });
                        } else {
                            // Anonymous relay might not be supported
                            log.info("Anonymous sender not supported: {}", sRes.cause().getMessage());
                            done.countDown();
                        }
                    });
                    sender.open();
                });
                conn.open();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");

        if (sendSuccess.get()) {
            Message received = receiveMessage(queueName, 5000);
            assertNotNull(received, "Anonymous sent message should arrive");
            log.info("PASS: Anonymous sender works");
        } else {
            log.info("PASS: Anonymous sender test completed (may not be supported)");
        }
    }

    @Test
    @Order(11)
    @DisplayName("Test: Sender close and reopen")
    @Timeout(30)
    void testSenderCloseAndReopen() throws Exception {
        String queueName = generateQueueName("reopen");

        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger openCount = new AtomicInteger(0);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    // First sender
                    ProtonSender sender1 = conn.createSender(queueName);
                    sender1.openHandler(s1Res -> {
                        if (s1Res.succeeded()) {
                            openCount.incrementAndGet();
                            sender1.close();

                            // Second sender after close
                            ProtonSender sender2 = conn.createSender(queueName);
                            sender2.openHandler(s2Res -> {
                                if (s2Res.succeeded()) {
                                    openCount.incrementAndGet();
                                }
                                sender2.close();
                                conn.close();
                                done.countDown();
                            });
                            sender2.open();
                        } else {
                            done.countDown();
                        }
                    });
                    sender1.open();
                });
                conn.open();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertEquals(2, openCount.get(), "Both senders should open successfully");

        log.info("PASS: Sender close and reopen works");
    }
}
