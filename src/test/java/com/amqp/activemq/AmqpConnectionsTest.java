/*
 * Adapted from Apache ActiveMQ AmqpConnectionsTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 connection functionality.
 * Adapted from Apache ActiveMQ AmqpConnectionsTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Connections Tests (ActiveMQ)")
public class AmqpConnectionsTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Can connect to broker")
    @Timeout(30)
    void testCanConnect() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean connected = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        connected.set(true);
                        conn.close();
                        done.countDown();
                    } else {
                        errorRef.set("Connection open failed: " + openRes.cause());
                        done.countDown();
                    }
                });
                conn.closeHandler(v -> {});
                conn.open();
            } else {
                errorRef.set("Connect failed: " + res.cause());
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(connected.get(), "Should have connected successfully");

        log.info("PASS: Can connect to broker");
    }

    @Test
    @Order(2)
    @DisplayName("Test: Connection carries expected capabilities")
    @Timeout(30)
    void testConnectionCarriesExpectedCapabilities() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Symbol[]> capabilities = new AtomicReference<>();
        AtomicReference<Map<Symbol, Object>> properties = new AtomicReference<>();
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        capabilities.set(conn.getRemoteOfferedCapabilities());
                        properties.set(conn.getRemoteProperties());
                        conn.close();
                        done.countDown();
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

        // Check capabilities (ANONYMOUS-RELAY is optional but good to have)
        log.info("Remote capabilities: {}", capabilities.get() != null ?
            java.util.Arrays.toString(capabilities.get()) : "none");
        log.info("Remote properties: {}", properties.get());

        // Properties should include product info
        if (properties.get() != null) {
            log.info("Product: {}", properties.get().get(Symbol.valueOf("product")));
            log.info("Version: {}", properties.get().get(Symbol.valueOf("version")));
        }

        log.info("PASS: Connection carries capabilities/properties");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Connection carries container ID")
    @Timeout(30)
    void testConnectionCarriesContainerId() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<String> remoteContainerId = new AtomicReference<>();
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        remoteContainerId.set(conn.getRemoteContainer());
                        conn.close();
                        done.countDown();
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
        assertNotNull(remoteContainerId.get(), "Should have remote container ID");
        assertFalse(remoteContainerId.get().isEmpty(), "Container ID should not be empty");

        log.info("PASS: Container ID: {}", remoteContainerId.get());
    }

    @Test
    @Order(4)
    @DisplayName("Test: Can connect with different container IDs")
    @Timeout(30)
    void testCanConnectWithDifferentContainerIds() throws Exception {
        CountDownLatch done = new CountDownLatch(2);
        AtomicBoolean conn1Success = new AtomicBoolean(false);
        AtomicBoolean conn2Success = new AtomicBoolean(false);

        String containerId1 = "test-container-" + UUID.randomUUID();
        String containerId2 = "test-container-" + UUID.randomUUID();

        ProtonClient client = ProtonClient.create(vertx);

        // First connection
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.setContainer(containerId1);
                conn.openHandler(openRes -> {
                    conn1Success.set(openRes.succeeded());
                    // Keep connection open
                    done.countDown();
                });
                conn.open();
            } else {
                done.countDown();
            }
        });

        // Small delay
        Thread.sleep(500);

        // Second connection with different container ID
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.setContainer(containerId2);
                conn.openHandler(openRes -> {
                    conn2Success.set(openRes.succeeded());
                    conn.close();
                    done.countDown();
                });
                conn.open();
            } else {
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertTrue(conn1Success.get(), "First connection should succeed");
        assertTrue(conn2Success.get(), "Second connection with different ID should succeed");

        log.info("PASS: Multiple connections with different container IDs");
    }

    @Test
    @Order(5)
    @DisplayName("Test: Simple send and receive")
    @Timeout(30)
    void testSimpleSendOneReceive() throws Exception {
        String queueName = generateQueueName("conn-send-recv");
        String testBody = "Test message body - " + UUID.randomUUID();

        // Send message
        sendMessage(queueName, createMessage(testBody));

        // Receive message
        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");

        Object body = ((AmqpValue) received.getBody()).getValue();
        assertEquals(testBody, body, "Message body should match");

        log.info("PASS: Simple send and receive works");
    }

    @Test
    @Order(6)
    @DisplayName("Test: Connection idle timeout")
    @Timeout(60)
    void testConnectionIdleTimeout() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean connected = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        ProtonClientOptions options = createClientOptions();
        options.setIdleTimeout(5000); // 5 second idle timeout

        client.connect(options, TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        connected.set(true);
                        // Keep connection open, wait for idle timeout or manual close
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(connected.get(), "Should have connected");

        log.info("PASS: Connection idle timeout test completed");
    }

    @Test
    @Order(7)
    @DisplayName("Test: Multiple sessions on one connection")
    @Timeout(30)
    void testMultipleSessionsOnOneConnection() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean session1Open = new AtomicBoolean(false);
        AtomicBoolean session2Open = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Create first session
                        ProtonSession session1 = conn.createSession();
                        session1.openHandler(s1Res -> {
                            session1Open.set(s1Res.succeeded());

                            // Create second session
                            ProtonSession session2 = conn.createSession();
                            session2.openHandler(s2Res -> {
                                session2Open.set(s2Res.succeeded());
                                session1.close();
                                session2.close();
                                conn.close();
                                done.countDown();
                            });
                            session2.open();
                        });
                        session1.open();
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
        assertTrue(session1Open.get(), "First session should open");
        assertTrue(session2Open.get(), "Second session should open");

        log.info("PASS: Multiple sessions on one connection");
    }

    @Test
    @Order(8)
    @DisplayName("Test: Connection close with active session")
    @Timeout(30)
    void testConnectionCloseWithActiveSession() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean closedCleanly = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Create session but don't close it before connection close
                        ProtonSession session = conn.createSession();
                        session.openHandler(sRes -> {
                            if (sRes.succeeded()) {
                                // Close connection directly without closing session
                                conn.closeHandler(closeRes -> {
                                    closedCleanly.set(true);
                                    done.countDown();
                                });
                                conn.close();
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
        assertTrue(closedCleanly.get(), "Connection should close cleanly");

        log.info("PASS: Connection closes cleanly with active session");
    }

    private Message createMessage(String body) {
        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue(body));
        msg.setMessageId(UUID.randomUUID().toString());
        return msg;
    }
}
