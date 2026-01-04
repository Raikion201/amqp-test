/*
 * Adapted from Apache ActiveMQ AmqpPresettledTest.
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
 * Tests for AMQP 1.0 presettled (at-most-once) delivery.
 * Adapted from Apache ActiveMQ presettled message tests.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Presettled Tests (ActiveMQ)")
public class AmqpPresettledTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Presettled sender delivery")
    @Timeout(30)
    void testPresettledSenderDelivery() throws Exception {
        String queueName = generateQueueName("presettled-send");
        int messageCount = 5;

        CountDownLatch sendDone = new CountDownLatch(1);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.setQoS(ProtonQoS.AT_MOST_ONCE); // Presettled
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                for (int i = 0; i < messageCount; i++) {
                                    Message msg = Message.Factory.create();
                                    msg.setBody(new AmqpValue("Presettled message " + i));
                                    msg.setMessageId("presettled-" + i);
                                    sender.send(msg);
                                }
                                sender.close();
                                conn.close();
                                sendDone.countDown();
                            } else {
                                errorRef.set("Sender open failed");
                                sendDone.countDown();
                            }
                        });
                        sender.open();
                    } else {
                        errorRef.set("Connection open failed");
                        sendDone.countDown();
                    }
                });
                conn.open();
            } else {
                errorRef.set("Connect failed");
                sendDone.countDown();
            }
        });

        assertTrue(sendDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Send timeout");
        assertNull(errorRef.get(), errorRef.get());

        // Verify messages arrived
        int received = 0;
        for (int i = 0; i < messageCount; i++) {
            Message msg = receiveMessage(queueName, 2000);
            if (msg != null) received++;
        }

        assertEquals(messageCount, received, "All presettled messages should arrive");
        log.info("PASS: Presettled sender delivered {} messages", received);
    }

    @Test
    @Order(2)
    @DisplayName("Test: Presettled receiver delivery")
    @Timeout(30)
    void testPresettledReceiverDelivery() throws Exception {
        String queueName = generateQueueName("presettled-recv");
        int messageCount = 5;

        // Send messages first
        for (int i = 0; i < messageCount; i++) {
            Message msg = Message.Factory.create();
            msg.setBody(new AmqpValue("Message " + i));
            msg.setMessageId("msg-" + i);
            sendMessage(queueName, msg);
        }

        // Receive with presettled mode
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger receivedCount = new AtomicInteger(0);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setQoS(ProtonQoS.AT_MOST_ONCE); // Presettled
                        receiver.handler((delivery, msg) -> {
                            // Message is already settled, no need to disposition
                            int count = receivedCount.incrementAndGet();
                            if (count >= messageCount) {
                                receiver.close();
                                conn.close();
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertEquals(messageCount, receivedCount.get(), "Should receive all messages");

        log.info("PASS: Presettled receiver got {} messages", receivedCount.get());
    }

    @Test
    @Order(3)
    @DisplayName("Test: Mixed settled and unsettled")
    @Timeout(30)
    void testMixedSettledAndUnsettled() throws Exception {
        String queueName = generateQueueName("presettled-mixed");

        // Send presettled message
        CountDownLatch sendLatch = new CountDownLatch(1);
        ProtonClient client1 = ProtonClient.create(vertx);
        client1.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    ProtonSender sender = conn.createSender(queueName);
                    sender.setQoS(ProtonQoS.AT_MOST_ONCE);
                    sender.openHandler(sRes -> {
                        Message msg = Message.Factory.create();
                        msg.setBody(new AmqpValue("Presettled"));
                        msg.setMessageId("presettled-1");
                        sender.send(msg);
                        sender.close();
                        conn.close();
                        sendLatch.countDown();
                    });
                    sender.open();
                });
                conn.open();
            } else {
                sendLatch.countDown();
            }
        });
        sendLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // Send unsettled message
        Message unsettledMsg = Message.Factory.create();
        unsettledMsg.setBody(new AmqpValue("Unsettled"));
        unsettledMsg.setMessageId("unsettled-1");
        sendMessage(queueName, unsettledMsg);

        // Receive both
        int count = 0;
        for (int i = 0; i < 2; i++) {
            Message msg = receiveMessage(queueName, 3000);
            if (msg != null) count++;
        }

        assertEquals(2, count, "Should receive both messages");
        log.info("PASS: Mixed settled and unsettled messages handled");
    }

    @Test
    @Order(4)
    @DisplayName("Test: Presettled with multiple receivers")
    @Timeout(60)
    void testPresettledWithMultipleReceivers() throws Exception {
        String queueName = generateQueueName("presettled-multi");
        int messageCount = 10;

        // Send messages
        for (int i = 0; i < messageCount; i++) {
            Message msg = Message.Factory.create();
            msg.setBody(new AmqpValue("Message " + i));
            msg.setMessageId("msg-" + i);
            sendMessage(queueName, msg);
        }

        // Two presettled receivers
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger totalReceived = new AtomicInteger(0);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Receiver 1
                        ProtonReceiver r1 = conn.createReceiver(queueName);
                        r1.setQoS(ProtonQoS.AT_MOST_ONCE);
                        r1.handler((d, m) -> {
                            if (totalReceived.incrementAndGet() >= messageCount) {
                                conn.close();
                                done.countDown();
                            }
                        });
                        r1.open();

                        // Receiver 2
                        ProtonReceiver r2 = conn.createReceiver(queueName);
                        r2.setQoS(ProtonQoS.AT_MOST_ONCE);
                        r2.handler((d, m) -> {
                            if (totalReceived.incrementAndGet() >= messageCount) {
                                conn.close();
                                done.countDown();
                            }
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
        assertEquals(messageCount, totalReceived.get(), "Should receive all messages");

        log.info("PASS: Presettled with multiple receivers got {} messages", totalReceived.get());
    }

    @Test
    @Order(5)
    @DisplayName("Test: High throughput presettled")
    @Timeout(60)
    void testHighThroughputPresettled() throws Exception {
        String queueName = generateQueueName("presettled-throughput");
        int messageCount = 100;

        // Send many presettled messages quickly
        CountDownLatch sendDone = new CountDownLatch(1);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    ProtonSender sender = conn.createSender(queueName);
                    sender.setQoS(ProtonQoS.AT_MOST_ONCE);
                    sender.openHandler(sRes -> {
                        for (int i = 0; i < messageCount; i++) {
                            Message msg = Message.Factory.create();
                            msg.setBody(new AmqpValue("Msg " + i));
                            msg.setMessageId("msg-" + i);
                            sender.send(msg);
                        }
                        sender.close();
                        conn.close();
                        sendDone.countDown();
                    });
                    sender.open();
                });
                conn.open();
            } else {
                sendDone.countDown();
            }
        });

        assertTrue(sendDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Send timeout");

        // Receive all messages
        CountDownLatch recvDone = new CountDownLatch(1);
        AtomicInteger receivedCount = new AtomicInteger(0);

        ProtonClient client2 = ProtonClient.create(vertx);
        client2.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    ProtonReceiver receiver = conn.createReceiver(queueName);
                    receiver.setQoS(ProtonQoS.AT_MOST_ONCE);
                    receiver.handler((d, m) -> {
                        if (receivedCount.incrementAndGet() >= messageCount) {
                            receiver.close();
                            conn.close();
                            recvDone.countDown();
                        }
                    });
                    receiver.open();
                });
                conn.open();
            } else {
                recvDone.countDown();
            }
        });

        assertTrue(recvDone.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS), "Receive timeout");
        assertEquals(messageCount, receivedCount.get(), "Should receive all messages");

        log.info("PASS: High throughput presettled sent and received {} messages", messageCount);
    }
}
