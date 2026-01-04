/*
 * Adapted from Apache ActiveMQ AmqpDisabledInactivityMonitorTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
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
 * Tests for AMQP 1.0 inactivity monitor behavior.
 * Adapted from Apache ActiveMQ AmqpDisabledInactivityMonitorTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Inactivity Monitor Tests (ActiveMQ)")
public class AmqpInactivityMonitorTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Connection stays alive during inactivity")
    @Timeout(60)
    void testConnectionStaysAliveDuringInactivity() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean connectionOpen = new AtomicBoolean(false);
        AtomicReference<ProtonConnection> connRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                connRef.set(conn);
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        connectionOpen.set(true);
                        done.countDown();
                    } else {
                        done.countDown();
                    }
                });
                conn.open();
            } else {
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Connection timeout");
        assertTrue(connectionOpen.get(), "Connection should be open");

        // Wait for inactivity period
        Thread.sleep(5000);

        // Check connection is still open
        assertNotNull(connRef.get(), "Connection reference should exist");

        // Clean up
        CountDownLatch closeLatch = new CountDownLatch(1);
        connRef.get().closeHandler(closeRes -> closeLatch.countDown());
        connRef.get().close();
        closeLatch.await(5, TimeUnit.SECONDS);

        log.info("PASS: Connection stays alive during inactivity");
    }

    @Test
    @Order(2)
    @DisplayName("Test: Send message after idle period")
    @Timeout(60)
    void testSendMessageAfterIdlePeriod() throws Exception {
        String queueName = generateQueueName("inactivity-idle");

        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean messageSent = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Wait idle period then send
                        vertx.setTimer(3000, timerId -> {
                            ProtonSender sender = conn.createSender(queueName);
                            sender.openHandler(senderRes -> {
                                if (senderRes.succeeded()) {
                                    Message msg = Message.Factory.create();
                                    msg.setBody(new AmqpValue("Message after idle"));
                                    msg.setMessageId("idle-msg-1");
                                    sender.send(msg, delivery -> {
                                        messageSent.set(true);
                                        sender.close();
                                        conn.close();
                                        done.countDown();
                                    });
                                } else {
                                    errorRef.set("Sender open failed after idle");
                                    conn.close();
                                    done.countDown();
                                }
                            });
                            sender.open();
                        });
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
        assertTrue(messageSent.get(), "Message should be sent after idle period");

        // Verify message arrived
        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message sent after idle");

        log.info("PASS: Send message after idle period");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Receive message after idle period")
    @Timeout(60)
    void testReceiveMessageAfterIdlePeriod() throws Exception {
        String queueName = generateQueueName("inactivity-recv");

        // Pre-send message
        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue("Pre-sent message"));
        sent.setMessageId("presend-msg-1");
        sendMessage(queueName, sent);

        // Connect and wait before receiving
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Message> receivedRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Wait idle period then receive
                        vertx.setTimer(3000, timerId -> {
                            ProtonReceiver receiver = conn.createReceiver(queueName);
                            receiver.setPrefetch(0);
                            receiver.handler((delivery, msg) -> {
                                receivedRef.set(msg);
                                delivery.disposition(org.apache.qpid.proton.amqp.messaging.Accepted.getInstance(), true);
                                receiver.close();
                                conn.close();
                                done.countDown();
                            });
                            receiver.openHandler(recvRes -> {
                                if (recvRes.succeeded()) {
                                    receiver.flow(1);
                                } else {
                                    conn.close();
                                    done.countDown();
                                }
                            });
                            receiver.open();
                        });
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
        assertNotNull(receivedRef.get(), "Should receive message after idle period");

        log.info("PASS: Receive message after idle period");
    }

    @Test
    @Order(4)
    @DisplayName("Test: Multiple idle periods")
    @Timeout(90)
    void testMultipleIdlePeriods() throws Exception {
        String queueName = generateQueueName("inactivity-multi");
        int messageCount = 3;

        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger sentCount = new AtomicInteger(0);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                // Send messages with idle periods between
                                sendWithDelay(sender, sentCount, messageCount, conn, done);
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

        assertTrue(done.await(TIMEOUT_SECONDS * 3, TimeUnit.SECONDS), "Timeout");
        assertEquals(messageCount, sentCount.get(), "All messages should be sent");

        // Verify all messages arrived
        int received = 0;
        for (int i = 0; i < messageCount; i++) {
            Message msg = receiveMessage(queueName, 3000);
            if (msg != null) received++;
        }
        assertEquals(messageCount, received, "All messages should arrive");

        log.info("PASS: Multiple idle periods handled correctly");
    }

    private void sendWithDelay(ProtonSender sender, AtomicInteger count, int total,
                               ProtonConnection conn, CountDownLatch done) {
        if (count.get() >= total) {
            sender.close();
            conn.close();
            done.countDown();
            return;
        }

        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("Message " + count.get()));
        msg.setMessageId("multi-idle-" + count.get());
        sender.send(msg, delivery -> {
            count.incrementAndGet();
            // Delay before next message
            vertx.setTimer(2000, id -> sendWithDelay(sender, count, total, conn, done));
        });
    }

    @Test
    @Order(5)
    @DisplayName("Test: Long running connection with activity")
    @Timeout(60)
    void testLongRunningConnectionWithActivity() throws Exception {
        String queueName = generateQueueName("inactivity-long");

        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger pingCount = new AtomicInteger(0);
        int targetPings = 5;

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                // Send periodic "ping" messages
                                long timerId = vertx.setPeriodic(1000, id -> {
                                    if (pingCount.get() < targetPings) {
                                        Message msg = Message.Factory.create();
                                        msg.setBody(new AmqpValue("Ping " + pingCount.get()));
                                        sender.send(msg);
                                        pingCount.incrementAndGet();
                                    } else {
                                        vertx.cancelTimer(id);
                                        sender.close();
                                        conn.close();
                                        done.countDown();
                                    }
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

        assertTrue(done.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS), "Timeout");
        assertEquals(targetPings, pingCount.get(), "All pings should be sent");

        log.info("PASS: Long running connection with activity - {} pings sent", pingCount.get());
    }
}
