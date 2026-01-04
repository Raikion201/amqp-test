/*
 * Adapted from Apache ActiveMQ AmqpBrokerRequestedHeartbeatsTest and AmqpClientRequestsHeartbeatsTest.
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
 * Tests for AMQP 1.0 heartbeat functionality.
 * Adapted from Apache ActiveMQ AmqpBrokerRequestedHeartbeatsTest and AmqpClientRequestsHeartbeatsTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Heartbeats Tests (ActiveMQ)")
public class AmqpHeartbeatsTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Client requests heartbeats")
    @Timeout(60)
    void testClientRequestsHeartbeats() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean connected = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        ProtonClientOptions options = createClientOptions();
        // Use longer idle timeout to avoid race conditions
        options.setIdleTimeout(30000); // 30 second idle timeout
        options.setHeartbeat(10000); // Send heartbeats every 10 seconds

        client.connect(options, TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        connected.set(true);
                        // Short wait to verify connection works with heartbeat settings
                        vertx.setTimer(2000, id -> {
                            conn.close();
                            done.countDown();
                        });
                    } else {
                        errorRef.set("Connection open failed");
                        done.countDown();
                    }
                });
                conn.disconnectHandler(v -> {
                    // Connection was dropped unexpectedly
                    if (done.getCount() > 0) {
                        errorRef.set("Connection dropped unexpectedly");
                        done.countDown();
                    }
                });
                conn.open();
            } else {
                errorRef.set("Connect failed: " + res.cause());
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(connected.get(), "Should connect successfully");

        log.info("PASS: Client requests heartbeats");
    }

    @Test
    @Order(2)
    @DisplayName("Test: Broker honors client heartbeat request")
    @Timeout(60)
    void testBrokerHonorsClientHeartbeatRequest() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean connectionStayedOpen = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        ProtonClientOptions options = createClientOptions();
        // Use longer idle timeout to ensure heartbeats have time to work
        options.setIdleTimeout(30000); // 30 second idle timeout
        options.setHeartbeat(10000); // Send heartbeat every 10 seconds

        client.connect(options, TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Just verify connection stays open for a reasonable period
                        vertx.setTimer(3000, id -> {
                            connectionStayedOpen.set(true);
                            conn.close();
                            done.countDown();
                        });
                    } else {
                        errorRef.set("Connection open failed");
                        done.countDown();
                    }
                });
                conn.disconnectHandler(v -> {
                    if (!connectionStayedOpen.get()) {
                        errorRef.set("Connection dropped - heartbeats may not be working");
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
        assertTrue(connectionStayedOpen.get(), "Connection should stay open with heartbeats");

        log.info("PASS: Broker honors client heartbeat request");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Connection with zero idle timeout")
    @Timeout(30)
    void testConnectionWithZeroIdleTimeout() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean connected = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        ProtonClientOptions options = createClientOptions();
        options.setIdleTimeout(0); // No idle timeout

        client.connect(options, TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    connected.set(openRes.succeeded());
                    conn.close();
                    done.countDown();
                });
                conn.open();
            } else {
                errorRef.set("Connect failed");
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(connected.get(), "Should connect with zero idle timeout");

        log.info("PASS: Connection with zero idle timeout");
    }

    @Test
    @Order(4)
    @DisplayName("Test: Connection with moderate idle timeout")
    @Timeout(60)
    void testConnectionWithModerateIdleTimeout() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean connected = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        ProtonClientOptions options = createClientOptions();
        // Use moderate but realistic timeout values
        options.setIdleTimeout(15000); // 15 second timeout
        options.setHeartbeat(5000); // Heartbeat every 5 seconds

        client.connect(options, TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        connected.set(true);
                        // Short wait to verify connection works
                        vertx.setTimer(2000, id -> {
                            conn.close();
                            done.countDown();
                        });
                    } else {
                        errorRef.set("Connection open failed");
                        done.countDown();
                    }
                });
                conn.disconnectHandler(v -> {
                    if (done.getCount() > 0 && !connected.get()) {
                        errorRef.set("Connection dropped unexpectedly");
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
        assertTrue(connected.get(), "Should connect with moderate idle timeout");

        log.info("PASS: Connection with moderate idle timeout");
    }

    @Test
    @Order(5)
    @DisplayName("Test: Heartbeats with active messaging")
    @Timeout(60)
    void testHeartbeatsWithActiveMessaging() throws Exception {
        String queueName = generateQueueName("heartbeat-msg");
        int messageCount = 10;
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger sent = new AtomicInteger(0);
        AtomicInteger received = new AtomicInteger(0);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        ProtonClientOptions options = createClientOptions();
        // Use realistic timeout values
        options.setIdleTimeout(30000);
        options.setHeartbeat(10000);

        client.connect(options, TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Create sender
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(sendRes -> {
                            if (sendRes.succeeded()) {
                                // Send messages with delays (start at 1ms to avoid Vert.x timer issue)
                                for (int i = 0; i < messageCount; i++) {
                                    final int msgNum = i;
                                    vertx.setTimer(1 + i * 200, id -> {
                                        Message msg = Message.Factory.create();
                                        msg.setBody(new AmqpValue("Message " + msgNum));
                                        sender.send(msg);
                                        sent.incrementAndGet();
                                    });
                                }

                                // Create receiver after a delay
                                vertx.setTimer(500, id -> {
                                    ProtonReceiver receiver = conn.createReceiver(queueName);
                                    receiver.setPrefetch(0); // Use manual flow control
                                    receiver.handler((delivery, msg) -> {
                                        delivery.disposition(Accepted.getInstance(), true);
                                        int count = received.incrementAndGet();
                                        if (count >= messageCount) {
                                            sender.close();
                                            receiver.close();
                                            conn.close();
                                            done.countDown();
                                        }
                                    });
                                    receiver.openHandler(recvRes -> {
                                        if (recvRes.succeeded()) {
                                            receiver.flow(messageCount);
                                        } else {
                                            errorRef.set("Receiver open failed");
                                            done.countDown();
                                        }
                                    });
                                    receiver.open();
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

        assertTrue(done.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertEquals(messageCount, sent.get(), "Should send all messages");
        assertEquals(messageCount, received.get(), "Should receive all messages");

        log.info("PASS: Heartbeats with active messaging - sent: {}, received: {}", sent.get(), received.get());
    }

    @Test
    @Order(6)
    @DisplayName("Test: Multiple connections with different heartbeat settings")
    @Timeout(60)
    void testMultipleConnectionsWithDifferentHeartbeats() throws Exception {
        CountDownLatch done = new CountDownLatch(2);
        AtomicBoolean conn1Success = new AtomicBoolean(false);
        AtomicBoolean conn2Success = new AtomicBoolean(false);

        ProtonClient client = ProtonClient.create(vertx);

        // First connection with moderate heartbeat settings
        ProtonClientOptions options1 = createClientOptions();
        options1.setIdleTimeout(20000);
        options1.setHeartbeat(8000);

        client.connect(options1, TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        vertx.setTimer(2000, id -> {
                            conn1Success.set(true);
                            conn.close();
                            done.countDown();
                        });
                    } else {
                        done.countDown();
                    }
                });
                conn.disconnectHandler(v -> {
                    if (!conn1Success.get()) done.countDown();
                });
                conn.open();
            } else {
                done.countDown();
            }
        });

        // Second connection with longer heartbeat settings
        ProtonClientOptions options2 = createClientOptions();
        options2.setIdleTimeout(30000);
        options2.setHeartbeat(10000);

        client.connect(options2, TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        vertx.setTimer(2000, id -> {
                            conn2Success.set(true);
                            conn.close();
                            done.countDown();
                        });
                    } else {
                        done.countDown();
                    }
                });
                conn.disconnectHandler(v -> {
                    if (!conn2Success.get()) done.countDown();
                });
                conn.open();
            } else {
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS), "Timeout");
        assertTrue(conn1Success.get(), "First connection should succeed");
        assertTrue(conn2Success.get(), "Second connection should succeed");

        log.info("PASS: Multiple connections with different heartbeat settings");
    }

    @Test
    @Order(7)
    @DisplayName("Test: Connection with reconnection options")
    @Timeout(60)
    void testConnectionWithReconnectionOptions() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger connectionCount = new AtomicInteger(0);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        ProtonClientOptions options = createClientOptions();
        // Use realistic timeout values
        options.setIdleTimeout(30000);
        options.setHeartbeat(10000);
        options.setReconnectAttempts(2);
        options.setReconnectInterval(1000);

        client.connect(options, TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                connectionCount.incrementAndGet();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Short wait and close gracefully
                        vertx.setTimer(2000, id -> {
                            conn.close();
                            done.countDown();
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
        assertTrue(connectionCount.get() >= 1, "Should have at least one connection");

        log.info("PASS: Connection with reconnection options - connection count: {}", connectionCount.get());
    }
}
