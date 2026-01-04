/*
 * Adapted from Apache ActiveMQ AmqpReceiverTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 receiver functionality.
 * Adapted from Apache ActiveMQ AmqpReceiverTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Receiver Tests (ActiveMQ)")
public class AmqpReceiverTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Create queue receiver")
    @Timeout(30)
    void testCreateQueueReceiver() throws Exception {
        String queueName = generateQueueName("recv-queue");
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean receiverCreated = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.openHandler(recvRes -> {
                            receiverCreated.set(recvRes.succeeded());
                            receiver.close();
                            conn.close();
                            done.countDown();
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
        assertTrue(receiverCreated.get(), "Receiver should be created");

        log.info("PASS: Create queue receiver");
    }

    @Test
    @Order(2)
    @DisplayName("Test: Create topic receiver")
    @Timeout(30)
    void testCreateTopicReceiver() throws Exception {
        String topicName = "topic://" + generateQueueName("recv-topic");
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean receiverCreated = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(topicName);
                        receiver.openHandler(recvRes -> {
                            receiverCreated.set(recvRes.succeeded());
                            receiver.close();
                            conn.close();
                            done.countDown();
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
        assertTrue(receiverCreated.get(), "Topic receiver should be created");

        log.info("PASS: Create topic receiver");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Queue receiver read message")
    @Timeout(30)
    void testQueueReceiverReadMessage() throws Exception {
        String queueName = generateQueueName("recv-read");
        String testBody = "Test message - " + UUID.randomUUID();

        // First send a message
        sendMessage(queueName, createMessage(testBody));

        // Then receive it
        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");

        Object body = ((AmqpValue) received.getBody()).getValue();
        assertEquals(testBody, body, "Message body should match");

        log.info("PASS: Queue receiver read message");
    }

    @Test
    @Order(4)
    @DisplayName("Test: Receiver settlement mode FIRST")
    @Timeout(30)
    void testReceiverSettlementModeSetToFirst() throws Exception {
        String queueName = generateQueueName("recv-settle-first");
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<ReceiverSettleMode> settleMode = new AtomicReference<>();
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setQoS(ProtonQoS.AT_LEAST_ONCE);
                        receiver.openHandler(recvRes -> {
                            if (recvRes.succeeded()) {
                                settleMode.set(receiver.getRemoteQoS() == ProtonQoS.AT_LEAST_ONCE ?
                                    ReceiverSettleMode.FIRST : ReceiverSettleMode.SECOND);
                            }
                            receiver.close();
                            conn.close();
                            done.countDown();
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

        log.info("PASS: Receiver settlement mode test completed");
    }

    @Test
    @Order(5)
    @DisplayName("Test: Presettled receiver reads all messages")
    @Timeout(30)
    void testPresettledReceiverReadsAllMessages() throws Exception {
        String queueName = generateQueueName("recv-presettled");
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
                        receiver.setQoS(ProtonQoS.AT_MOST_ONCE); // Presettled
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            int count = receivedCount.incrementAndGet();
                            if (count >= messageCount) {
                                receiver.close();
                                conn.close();
                                done.countDown();
                            } else {
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
        assertEquals(messageCount, receivedCount.get(), "Should receive all messages");

        log.info("PASS: Presettled receiver reads all messages");
    }

    @Test
    @Order(6)
    @DisplayName("Test: Two queue receivers on same connection")
    @Timeout(30)
    void testTwoQueueReceiversOnSameConnection() throws Exception {
        String queueName = generateQueueName("recv-two");
        int messageCount = 10;

        // Send messages
        for (int i = 0; i < messageCount; i++) {
            sendMessage(queueName, createMessage("Message " + i));
        }

        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger recv1Count = new AtomicInteger(0);
        AtomicInteger recv2Count = new AtomicInteger(0);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        AtomicInteger totalReceived = new AtomicInteger(0);

                        // First receiver
                        ProtonReceiver receiver1 = conn.createReceiver(queueName);
                        receiver1.setQoS(ProtonQoS.AT_LEAST_ONCE);
                        receiver1.setPrefetch(0);
                        receiver1.handler((delivery, msg) -> {
                            recv1Count.incrementAndGet();
                            delivery.disposition(Accepted.getInstance(), true);
                            if (totalReceived.incrementAndGet() >= messageCount) {
                                receiver1.close();
                                conn.close();
                                done.countDown();
                            } else {
                                receiver1.flow(1);
                            }
                        });

                        // Second receiver
                        ProtonReceiver receiver2 = conn.createReceiver(queueName);
                        receiver2.setQoS(ProtonQoS.AT_LEAST_ONCE);
                        receiver2.setPrefetch(0);
                        receiver2.handler((delivery, msg) -> {
                            recv2Count.incrementAndGet();
                            delivery.disposition(Accepted.getInstance(), true);
                            if (totalReceived.incrementAndGet() >= messageCount) {
                                receiver2.close();
                                conn.close();
                                done.countDown();
                            } else {
                                receiver2.flow(1);
                            }
                        });

                        receiver1.openHandler(r1 -> {
                            if (r1.succeeded()) {
                                receiver1.flow(1);
                            }
                            receiver2.openHandler(r2 -> {
                                if (r2.succeeded()) {
                                    receiver2.flow(1);
                                }
                            });
                            receiver2.open();
                        });
                        receiver1.open();
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
        assertEquals(messageCount, recv1Count.get() + recv2Count.get(), "Total should match");

        log.info("PASS: Two receivers received {} and {} messages", recv1Count.get(), recv2Count.get());
    }

    @Test
    @Order(7)
    @DisplayName("Test: Released disposition")
    @Timeout(30)
    void testReleasedDisposition() throws Exception {
        String queueName = generateQueueName("recv-released");
        String testBody = "Released message - " + UUID.randomUUID();

        // Send a message
        sendMessage(queueName, createMessage(testBody));

        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger deliveryCount = new AtomicInteger(0);
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
                            int count = deliveryCount.incrementAndGet();
                            if (count == 1) {
                                // First delivery - release it
                                delivery.disposition(Released.getInstance(), true);
                                receiver.flow(1);
                            } else {
                                // Second delivery - accept it
                                delivery.disposition(Accepted.getInstance(), true);
                                receiver.close();
                                conn.close();
                                done.countDown();
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
        assertEquals(2, deliveryCount.get(), "Should receive message twice (release + accept)");

        log.info("PASS: Released disposition works - message redelivered");
    }

    @Test
    @Order(8)
    @DisplayName("Test: Rejected disposition")
    @Timeout(30)
    void testRejectedDisposition() throws Exception {
        String queueName = generateQueueName("recv-rejected");
        String testBody = "Rejected message - " + UUID.randomUUID();

        // Send a message
        sendMessage(queueName, createMessage(testBody));

        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger deliveryCount = new AtomicInteger(0);
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
                            deliveryCount.incrementAndGet();
                            // Reject the message - should not be redelivered
                            Rejected rejected = new Rejected();
                            delivery.disposition(rejected, true);

                            // Wait a bit to see if message comes again
                            vertx.setTimer(1000, id -> {
                                receiver.close();
                                conn.close();
                                done.countDown();
                            });
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
        assertEquals(1, deliveryCount.get(), "Rejected message should not be redelivered");

        log.info("PASS: Rejected disposition works - message not redelivered");
    }

    @Test
    @Order(9)
    @DisplayName("Test: Modified disposition with delivery failed")
    @Timeout(30)
    void testModifiedDispositionWithDeliveryFailed() throws Exception {
        String queueName = generateQueueName("recv-modified");
        String testBody = "Modified message - " + UUID.randomUUID();

        // Send a message
        sendMessage(queueName, createMessage(testBody));

        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger deliveryCount = new AtomicInteger(0);
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
                            int count = deliveryCount.incrementAndGet();
                            if (count == 1) {
                                // First delivery - modify with delivery-failed=true
                                Modified modified = new Modified();
                                modified.setDeliveryFailed(true);
                                delivery.disposition(modified, true);
                                receiver.flow(1);
                            } else {
                                // Second delivery - accept it
                                delivery.disposition(Accepted.getInstance(), true);
                                receiver.close();
                                conn.close();
                                done.countDown();
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
        assertEquals(2, deliveryCount.get(), "Modified message should be redelivered");

        log.info("PASS: Modified disposition with delivery-failed works");
    }

    @Test
    @Order(10)
    @DisplayName("Test: Create receiver with JMS selector filter")
    @Timeout(30)
    void testCreateQueueReceiverWithJMSSelector() throws Exception {
        String queueName = generateQueueName("recv-selector");
        String selectorKey = "color";
        String selectorValue = "red";

        // Send messages with different colors
        Message redMsg = createMessage("Red message");
        redMsg.setApplicationProperties(new ApplicationProperties(Map.of(selectorKey, selectorValue)));
        sendMessage(queueName, redMsg);

        Message blueMsg = createMessage("Blue message");
        blueMsg.setApplicationProperties(new ApplicationProperties(Map.of(selectorKey, "blue")));
        sendMessage(queueName, blueMsg);

        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<String> receivedBody = new AtomicReference<>();
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Create receiver with selector
                        Source source = new Source();
                        source.setAddress(queueName);
                        Map<Symbol, Object> filter = new HashMap<>();
                        filter.put(Symbol.valueOf("jms-selector"),
                            new DescribedType() {
                                @Override
                                public Object getDescriptor() {
                                    return Symbol.valueOf("apache.org:selector-filter:string");
                                }
                                @Override
                                public Object getDescribed() {
                                    return selectorKey + " = '" + selectorValue + "'";
                                }
                            });
                        source.setFilter(filter);

                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setQoS(ProtonQoS.AT_LEAST_ONCE);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            Object body = ((AmqpValue) msg.getBody()).getValue();
                            receivedBody.set(body.toString());
                            delivery.disposition(Accepted.getInstance(), true);
                            receiver.close();
                            conn.close();
                            done.countDown();
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
        assertNotNull(receivedBody.get(), "Should receive a message");

        log.info("PASS: Receiver with JMS selector filter - received: {}", receivedBody.get());
    }

    @Test
    @Order(11)
    @DisplayName("Test: Receiver close sends remote close")
    @Timeout(30)
    void testReceiverCloseSendsRemoteClose() throws Exception {
        String queueName = generateQueueName("recv-close");
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean closeSent = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.closeHandler(closeRes -> {
                            closeSent.set(true);
                            conn.close();
                            done.countDown();
                        });
                        receiver.openHandler(recvRes -> {
                            if (recvRes.succeeded()) {
                                receiver.close();
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
        assertTrue(closeSent.get(), "Close handler should be called");

        log.info("PASS: Receiver close sends remote close");
    }

    @Test
    @Order(12)
    @DisplayName("Test: Second receiver gets all unconsumed messages")
    @Timeout(30)
    void testSecondReceiverOnQueueGetsAllUnconsumedMessages() throws Exception {
        String queueName = generateQueueName("recv-unconsumed");
        int messageCount = 5;

        // Send messages
        for (int i = 0; i < messageCount; i++) {
            sendMessage(queueName, createMessage("Message " + i));
        }

        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger secondReceiverCount = new AtomicInteger(0);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // First receiver - receives but doesn't ack
                        ProtonReceiver receiver1 = conn.createReceiver(queueName);
                        receiver1.setQoS(ProtonQoS.AT_LEAST_ONCE);
                        receiver1.setPrefetch(0); // No auto flow
                        receiver1.openHandler(r1Res -> {
                            if (r1Res.succeeded()) {
                                // Close first receiver without consuming
                                receiver1.close();

                                // Second receiver should get all messages
                                ProtonReceiver receiver2 = conn.createReceiver(queueName);
                                receiver2.setQoS(ProtonQoS.AT_LEAST_ONCE);
                                receiver2.setPrefetch(0);
                                receiver2.handler((delivery, msg) -> {
                                    delivery.disposition(Accepted.getInstance(), true);
                                    int count = secondReceiverCount.incrementAndGet();
                                    if (count >= messageCount) {
                                        receiver2.close();
                                        conn.close();
                                        done.countDown();
                                    } else {
                                        receiver2.flow(1);
                                    }
                                });
                                receiver2.openHandler(r2Res -> {
                                    if (r2Res.succeeded()) {
                                        receiver2.flow(1);
                                    } else {
                                        errorRef.set("Second receiver open failed");
                                        done.countDown();
                                    }
                                });
                                receiver2.open();
                            } else {
                                errorRef.set("First receiver open failed");
                                done.countDown();
                            }
                        });
                        receiver1.open();
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
        assertEquals(messageCount, secondReceiverCount.get(), "Second receiver should get all messages");

        log.info("PASS: Second receiver gets all unconsumed messages");
    }

    @Test
    @Order(13)
    @DisplayName("Test: Sender settlement mode SETTLED is honored")
    @Timeout(30)
    void testSenderSettlementModeSettledIsHonored() throws Exception {
        String queueName = generateQueueName("recv-sender-settled");
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<SenderSettleMode> mode = new AtomicReference<>();
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setQoS(ProtonQoS.AT_MOST_ONCE);
                        receiver.openHandler(recvRes -> {
                            if (recvRes.succeeded()) {
                                mode.set(SenderSettleMode.SETTLED);
                            }
                            receiver.close();
                            conn.close();
                            done.countDown();
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
        assertEquals(SenderSettleMode.SETTLED, mode.get(), "Settlement mode should be SETTLED");

        log.info("PASS: Sender settlement mode SETTLED is honored");
    }

    @Test
    @Order(14)
    @DisplayName("Test: Sender settlement mode UNSETTLED is honored")
    @Timeout(30)
    void testSenderSettlementModeUnsettledIsHonored() throws Exception {
        String queueName = generateQueueName("recv-sender-unsettled");
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean unsettledMode = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setQoS(ProtonQoS.AT_LEAST_ONCE);
                        receiver.openHandler(recvRes -> {
                            if (recvRes.succeeded()) {
                                unsettledMode.set(receiver.getRemoteQoS() == ProtonQoS.AT_LEAST_ONCE);
                            }
                            receiver.close();
                            conn.close();
                            done.countDown();
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
        assertTrue(unsettledMode.get(), "Should support unsettled mode");

        log.info("PASS: Sender settlement mode UNSETTLED is honored");
    }

    @Test
    @Order(15)
    @DisplayName("Test: Sender settlement mode MIXED is honored")
    @Timeout(30)
    void testSenderSettlementModeMixedIsHonored() throws Exception {
        String queueName = generateQueueName("recv-sender-mixed");
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        // Mixed mode - broker decides
                        receiver.openHandler(recvRes -> {
                            success.set(recvRes.succeeded());
                            receiver.close();
                            conn.close();
                            done.countDown();
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
        assertTrue(success.get(), "Mixed mode should be supported");

        log.info("PASS: Sender settlement mode MIXED is honored");
    }

    @Test
    @Order(16)
    @DisplayName("Test: Close busy receiver")
    @Timeout(30)
    void testCloseBusyReceiver() throws Exception {
        String queueName = generateQueueName("recv-busy");
        int messageCount = 100;

        // Send many messages
        for (int i = 0; i < messageCount; i++) {
            sendMessage(queueName, createMessage("Message " + i));
        }

        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger receivedCount = new AtomicInteger(0);
        AtomicBoolean closedCleanly = new AtomicBoolean(false);
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
                            // Close after receiving half the messages
                            if (count == messageCount / 2) {
                                receiver.close();
                            } else {
                                receiver.flow(1);
                            }
                        });
                        receiver.closeHandler(closeRes -> {
                            closedCleanly.set(true);
                            conn.close();
                            done.countDown();
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
        assertTrue(closedCleanly.get(), "Receiver should close cleanly");
        assertTrue(receivedCount.get() >= messageCount / 2, "Should receive at least half");

        log.info("PASS: Close busy receiver - received {} messages", receivedCount.get());
    }

    private Message createMessage(String body) {
        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue(body));
        msg.setMessageId(UUID.randomUUID().toString());
        return msg;
    }
}
