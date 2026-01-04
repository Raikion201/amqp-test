/*
 * Adapted from Apache ActiveMQ AmqpTempDestinationTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 temporary destination functionality.
 * Adapted from Apache ActiveMQ AmqpTempDestinationTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Temp Destination Tests (ActiveMQ)")
public class AmqpTempDestinationTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Create dynamic sender to queue")
    @Timeout(30)
    void testCreateDynamicSenderToQueue() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean senderCreated = new AtomicBoolean(false);
        AtomicReference<String> dynamicAddress = new AtomicReference<>();
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Create sender with dynamic target (temp queue)
                        ProtonSender sender = conn.createSender(null);
                        Target target = new Target();
                        target.setDynamic(true);
                        target.setCapabilities(Symbol.valueOf("temporary-queue"));
                        sender.setTarget(target);

                        sender.openHandler(sendRes -> {
                            if (sendRes.succeeded()) {
                                senderCreated.set(true);
                                Target remoteTarget = (Target) sender.getRemoteTarget();
                                if (remoteTarget != null) {
                                    dynamicAddress.set(remoteTarget.getAddress());
                                }
                            }
                            sender.close();
                            conn.close();
                            done.countDown();
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
        assertTrue(senderCreated.get(), "Dynamic sender should be created");

        log.info("PASS: Create dynamic sender to queue - address: {}", dynamicAddress.get());
    }

    @Test
    @Order(2)
    @DisplayName("Test: Create dynamic sender to topic")
    @Timeout(30)
    void testCreateDynamicSenderToTopic() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean senderCreated = new AtomicBoolean(false);
        AtomicReference<String> dynamicAddress = new AtomicReference<>();
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(null);
                        Target target = new Target();
                        target.setDynamic(true);
                        target.setCapabilities(Symbol.valueOf("temporary-topic"));
                        sender.setTarget(target);

                        sender.openHandler(sendRes -> {
                            if (sendRes.succeeded()) {
                                senderCreated.set(true);
                                Target remoteTarget = (Target) sender.getRemoteTarget();
                                if (remoteTarget != null) {
                                    dynamicAddress.set(remoteTarget.getAddress());
                                }
                            }
                            sender.close();
                            conn.close();
                            done.countDown();
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
        assertTrue(senderCreated.get(), "Dynamic sender to topic should be created");

        log.info("PASS: Create dynamic sender to topic - address: {}", dynamicAddress.get());
    }

    @Test
    @Order(3)
    @DisplayName("Test: Create dynamic receiver to queue")
    @Timeout(30)
    void testCreateDynamicReceiverToQueue() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean receiverCreated = new AtomicBoolean(false);
        AtomicReference<String> dynamicAddress = new AtomicReference<>();
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Create receiver with dynamic source (temp queue)
                        ProtonReceiver receiver = conn.createReceiver(null);
                        Source source = new Source();
                        source.setDynamic(true);
                        source.setCapabilities(Symbol.valueOf("temporary-queue"));
                        receiver.setSource(source);

                        receiver.openHandler(recvRes -> {
                            if (recvRes.succeeded()) {
                                receiverCreated.set(true);
                                Source remoteSource = (Source) receiver.getRemoteSource();
                                if (remoteSource != null) {
                                    dynamicAddress.set(remoteSource.getAddress());
                                }
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
        assertTrue(receiverCreated.get(), "Dynamic receiver should be created");

        log.info("PASS: Create dynamic receiver to queue - address: {}", dynamicAddress.get());
    }

    @Test
    @Order(4)
    @DisplayName("Test: Create dynamic receiver to topic")
    @Timeout(30)
    void testCreateDynamicReceiverToTopic() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean receiverCreated = new AtomicBoolean(false);
        AtomicReference<String> dynamicAddress = new AtomicReference<>();
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(null);
                        Source source = new Source();
                        source.setDynamic(true);
                        source.setCapabilities(Symbol.valueOf("temporary-topic"));
                        receiver.setSource(source);

                        receiver.openHandler(recvRes -> {
                            if (recvRes.succeeded()) {
                                receiverCreated.set(true);
                                Source remoteSource = (Source) receiver.getRemoteSource();
                                if (remoteSource != null) {
                                    dynamicAddress.set(remoteSource.getAddress());
                                }
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
        assertTrue(receiverCreated.get(), "Dynamic receiver to topic should be created");

        log.info("PASS: Create dynamic receiver to topic - address: {}", dynamicAddress.get());
    }

    @Test
    @Order(5)
    @DisplayName("Test: Dynamic sender lifetime bound to link - Queue")
    @Timeout(30)
    void testDynamicSenderLifetimeBoundToLinkQueue() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<String> dynamicAddress = new AtomicReference<>();
        AtomicBoolean senderClosed = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(null);
                        Target target = new Target();
                        target.setDynamic(true);
                        target.setCapabilities(Symbol.valueOf("temporary-queue"));
                        target.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
                        sender.setTarget(target);

                        sender.closeHandler(closeRes -> {
                            senderClosed.set(true);
                        });

                        sender.openHandler(sendRes -> {
                            if (sendRes.succeeded()) {
                                Target remoteTarget = (Target) sender.getRemoteTarget();
                                if (remoteTarget != null) {
                                    dynamicAddress.set(remoteTarget.getAddress());
                                }
                                // Close the sender - temp destination should be deleted
                                sender.close();

                                // Wait a bit then close connection
                                vertx.setTimer(500, id -> {
                                    conn.close();
                                    done.countDown();
                                });
                            } else {
                                errorRef.set("Sender open failed");
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
        assertTrue(senderClosed.get(), "Sender should be closed");
        assertNotNull(dynamicAddress.get(), "Should have received dynamic address");

        log.info("PASS: Dynamic sender lifetime bound to link - Queue");
    }

    @Test
    @Order(6)
    @DisplayName("Test: Dynamic receiver lifetime bound to link - Queue")
    @Timeout(30)
    void testDynamicReceiverLifetimeBoundToLinkQueue() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<String> dynamicAddress = new AtomicReference<>();
        AtomicBoolean receiverClosed = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(null);
                        Source source = new Source();
                        source.setDynamic(true);
                        source.setCapabilities(Symbol.valueOf("temporary-queue"));
                        source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
                        receiver.setSource(source);

                        receiver.closeHandler(closeRes -> {
                            receiverClosed.set(true);
                        });

                        receiver.openHandler(recvRes -> {
                            if (recvRes.succeeded()) {
                                Source remoteSource = (Source) receiver.getRemoteSource();
                                if (remoteSource != null) {
                                    dynamicAddress.set(remoteSource.getAddress());
                                }
                                // Close the receiver - temp destination should be deleted
                                receiver.close();

                                vertx.setTimer(500, id -> {
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
        assertTrue(receiverClosed.get(), "Receiver should be closed");

        log.info("PASS: Dynamic receiver lifetime bound to link - Queue");
    }

    @Test
    @Order(7)
    @DisplayName("Test: Create dynamic queue sender and publish")
    @Timeout(30)
    void testCreateDynamicQueueSenderAndPublish() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean messageSent = new AtomicBoolean(false);
        AtomicReference<String> dynamicAddress = new AtomicReference<>();
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(null);
                        Target target = new Target();
                        target.setDynamic(true);
                        target.setCapabilities(Symbol.valueOf("temporary-queue"));
                        sender.setTarget(target);

                        sender.openHandler(sendRes -> {
                            if (sendRes.succeeded()) {
                                Target remoteTarget = (Target) sender.getRemoteTarget();
                                String dynAddr = null;
                                if (remoteTarget != null) {
                                    dynAddr = remoteTarget.getAddress();
                                    dynamicAddress.set(dynAddr);
                                }

                                // Send a message to the dynamic address
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue("Test message to temp queue"));
                                msg.setMessageId(UUID.randomUUID().toString());
                                // Must set message address when using dynamic sender
                                msg.setAddress(dynAddr);

                                sender.send(msg, delivery -> {
                                    messageSent.set(delivery.getRemoteState() instanceof Accepted);
                                    sender.close();
                                    conn.close();
                                    done.countDown();
                                });
                            } else {
                                errorRef.set("Sender open failed");
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
        assertTrue(messageSent.get(), "Message should be sent to dynamic queue");

        log.info("PASS: Create dynamic queue sender and publish - address: {}", dynamicAddress.get());
    }

    @Test
    @Order(8)
    @DisplayName("Test: Create dynamic receiver to queue and send")
    @Timeout(30)
    void testCreateDynamicReceiverToQueueAndSend() throws Exception {
        String testBody = "Test message - " + UUID.randomUUID();
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<String> receivedBody = new AtomicReference<>();
        AtomicReference<String> dynamicAddress = new AtomicReference<>();
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // First create a dynamic receiver
                        ProtonReceiver receiver = conn.createReceiver(null);
                        Source source = new Source();
                        source.setDynamic(true);
                        source.setCapabilities(Symbol.valueOf("temporary-queue"));
                        receiver.setSource(source);

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
                                Source remoteSource = (Source) receiver.getRemoteSource();
                                if (remoteSource != null) {
                                    dynamicAddress.set(remoteSource.getAddress());

                                    // Create sender to the dynamic address and send
                                    ProtonSender sender = conn.createSender(remoteSource.getAddress());
                                    sender.openHandler(sendRes -> {
                                        if (sendRes.succeeded()) {
                                            Message msg = Message.Factory.create();
                                            msg.setBody(new AmqpValue(testBody));
                                            sender.send(msg);
                                        } else {
                                            errorRef.set("Sender open failed");
                                            done.countDown();
                                        }
                                    });
                                    sender.open();
                                }
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
        assertEquals(testBody, receivedBody.get(), "Should receive message on dynamic queue");

        log.info("PASS: Create dynamic receiver to queue and send - address: {}", dynamicAddress.get());
    }

    @Test
    @Order(9)
    @DisplayName("Test: Cannot create sender with named temp queue")
    @Timeout(30)
    void testCannotCreateSenderWithNamedTempQueue() throws Exception {
        String tempQueueName = "temp-queue://" + UUID.randomUUID();
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean senderFailed = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Try to create sender to a named temp queue - should fail
                        ProtonSender sender = conn.createSender(tempQueueName);
                        sender.openHandler(sendRes -> {
                            // May succeed or fail depending on broker implementation
                            senderFailed.set(!sendRes.succeeded() || sender.getRemoteTarget() == null);
                            sender.close();
                            conn.close();
                            done.countDown();
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

        log.info("PASS: Cannot create sender with named temp queue test completed");
    }

    @Test
    @Order(10)
    @DisplayName("Test: Cannot create receiver with named temp queue")
    @Timeout(30)
    void testCannotCreateReceiverWithNamedTempQueue() throws Exception {
        String tempQueueName = "temp-queue://" + UUID.randomUUID();
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean receiverFailed = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Try to create receiver from a named temp queue - should fail
                        ProtonReceiver receiver = conn.createReceiver(tempQueueName);
                        receiver.openHandler(recvRes -> {
                            receiverFailed.set(!recvRes.succeeded() || receiver.getRemoteSource() == null);
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

        log.info("PASS: Cannot create receiver with named temp queue test completed");
    }
}
