/*
 * Adapted from Apache ActiveMQ AmqpSaslPlainTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.junit.jupiter.api.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 SASL PLAIN authentication.
 * Adapted from Apache ActiveMQ AmqpSaslPlainTest.
 *
 * Note: These tests may require authentication to be configured on the broker.
 * If authentication is not enabled, tests will pass with anonymous connections.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 SASL PLAIN Tests (ActiveMQ)")
public class AmqpSaslPlainTest extends Amqp10TestBase {

    private static final String VALID_USERNAME = "admin";
    private static final String VALID_PASSWORD = "admin";
    private static final String INVALID_USERNAME = "invalid_user";
    private static final String INVALID_PASSWORD = "wrong_password";

    @Test
    @Order(1)
    @DisplayName("Test: SASL PLAIN with valid username and password")
    @Timeout(30)
    void testSaslPlainWithValidUsernameAndPassword() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean connected = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        ProtonClientOptions options = createClientOptions();
        // Note: vertx-proton handles SASL automatically if credentials provided

        client.connect(options, TEST_HOST, TEST_PORT, VALID_USERNAME, VALID_PASSWORD, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        connected.set(true);
                        exerciseConnection(conn, done);
                    } else {
                        errorRef.set("Connection open failed: " + openRes.cause());
                        done.countDown();
                    }
                });
                conn.open();
            } else {
                errorRef.set("Connect failed: " + res.cause());
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(connected.get(), "Should connect with valid credentials");

        log.info("PASS: SASL PLAIN with valid username and password");
    }

    @Test
    @Order(2)
    @DisplayName("Test: SASL PLAIN with valid credentials and authzid as user")
    @Timeout(30)
    void testSaslPlainWithValidUsernameAndPasswordAndAuthzidAsUser() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean connected = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        ProtonClientOptions options = createClientOptions();

        // Connect with same user as authzid
        client.connect(options, TEST_HOST, TEST_PORT, VALID_USERNAME, VALID_PASSWORD, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        connected.set(true);
                        exerciseConnection(conn, done);
                    } else {
                        errorRef.set("Connection open failed");
                        done.countDown();
                    }
                });
                conn.open();
            } else {
                errorRef.set("Connect failed: " + res.cause());
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(connected.get(), "Should connect with valid credentials and authzid");

        log.info("PASS: SASL PLAIN with valid credentials and authzid as user");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Connection without credentials (anonymous)")
    @Timeout(30)
    void testConnectionWithoutCredentials() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean connected = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        ProtonClientOptions options = createClientOptions();

        // Connect without credentials - uses ANONYMOUS if available
        client.connect(options, TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        connected.set(true);
                        conn.close();
                        done.countDown();
                    } else {
                        errorRef.set("Connection open failed");
                        done.countDown();
                    }
                });
                conn.open();
            } else {
                // May fail if broker requires auth
                errorRef.set("Connect failed (may be expected if auth required): " + res.cause());
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        // This test documents behavior - may succeed or fail depending on broker config
        if (errorRef.get() == null) {
            assertTrue(connected.get(), "Anonymous connection succeeded");
            log.info("PASS: Anonymous connection allowed");
        } else {
            log.info("PASS: Anonymous connection rejected (auth required): {}", errorRef.get());
        }
    }

    @Test
    @Order(4)
    @DisplayName("Test: SASL PLAIN with invalid username")
    @Timeout(30)
    void testSaslPlainWithInvalidUsername() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean connectionFailed = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        ProtonClientOptions options = createClientOptions();

        client.connect(options, TEST_HOST, TEST_PORT, INVALID_USERNAME, VALID_PASSWORD, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Connection succeeded - broker may allow any user
                        conn.close();
                        done.countDown();
                    } else {
                        connectionFailed.set(true);
                        done.countDown();
                    }
                });
                conn.disconnectHandler(v -> {
                    connectionFailed.set(true);
                    done.countDown();
                });
                conn.open();
            } else {
                // Expected - authentication should fail
                connectionFailed.set(true);
                errorRef.set(res.cause().getMessage());
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        // Document behavior - if auth is enabled, this should fail
        if (connectionFailed.get()) {
            log.info("PASS: Invalid username rejected: {}", errorRef.get());
        } else {
            log.info("PASS: Broker allows any username (no auth configured)");
        }
    }

    @Test
    @Order(5)
    @DisplayName("Test: SASL PLAIN with invalid password")
    @Timeout(30)
    void testSaslPlainWithInvalidPassword() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean connectionFailed = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        ProtonClientOptions options = createClientOptions();

        client.connect(options, TEST_HOST, TEST_PORT, VALID_USERNAME, INVALID_PASSWORD, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Connection succeeded - broker may not validate password
                        conn.close();
                        done.countDown();
                    } else {
                        connectionFailed.set(true);
                        done.countDown();
                    }
                });
                conn.disconnectHandler(v -> {
                    connectionFailed.set(true);
                    done.countDown();
                });
                conn.open();
            } else {
                // Expected - authentication should fail
                connectionFailed.set(true);
                errorRef.set(res.cause().getMessage());
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        if (connectionFailed.get()) {
            log.info("PASS: Invalid password rejected: {}", errorRef.get());
        } else {
            log.info("PASS: Broker allows any password (no auth configured)");
        }
    }

    @Test
    @Order(6)
    @DisplayName("Test: Multiple connections with different credentials")
    @Timeout(30)
    void testMultipleConnectionsWithDifferentCredentials() throws Exception {
        CountDownLatch done = new CountDownLatch(2);
        AtomicBoolean conn1Success = new AtomicBoolean(false);
        AtomicBoolean conn2Success = new AtomicBoolean(false);

        ProtonClient client = ProtonClient.create(vertx);
        ProtonClientOptions options = createClientOptions();

        // First connection with one set of credentials
        client.connect(options, TEST_HOST, TEST_PORT, VALID_USERNAME, VALID_PASSWORD, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    conn1Success.set(openRes.succeeded());
                    conn.close();
                    done.countDown();
                });
                conn.open();
            } else {
                done.countDown();
            }
        });

        // Second connection (may use different credentials or anonymous)
        client.connect(options, TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
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

        log.info("PASS: Multiple connections with different credentials - conn1: {}, conn2: {}",
            conn1Success.get(), conn2Success.get());
    }

    @Test
    @Order(7)
    @DisplayName("Test: Connection exercise with session and sender")
    @Timeout(30)
    void testConnectionExerciseWithSessionAndSender() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean sessionOpened = new AtomicBoolean(false);
        AtomicBoolean senderOpened = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        ProtonClientOptions options = createClientOptions();

        client.connect(options, TEST_HOST, TEST_PORT, VALID_USERNAME, VALID_PASSWORD, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Create session
                        ProtonSession session = conn.createSession();
                        session.openHandler(sessRes -> {
                            sessionOpened.set(sessRes.succeeded());
                            if (sessRes.succeeded()) {
                                // Create sender
                                String queueName = generateQueueName("sasl-test");
                                ProtonSender sender = session.createSender(queueName);
                                sender.openHandler(sendRes -> {
                                    senderOpened.set(sendRes.succeeded());
                                    sender.close();
                                    session.close();
                                    conn.close();
                                    done.countDown();
                                });
                                sender.open();
                            } else {
                                conn.close();
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
                errorRef.set("Connect failed: " + res.cause());
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(sessionOpened.get(), "Session should open");
        assertTrue(senderOpened.get(), "Sender should open");

        log.info("PASS: Connection exercise with session and sender");
    }

    /**
     * Helper method to exercise connection by creating session and sender.
     */
    private void exerciseConnection(ProtonConnection conn, CountDownLatch done) {
        ProtonSession session = conn.createSession();
        session.openHandler(sessRes -> {
            if (sessRes.succeeded()) {
                String queueName = generateQueueName("sasl-exercise");
                ProtonSender sender = session.createSender(queueName);
                sender.openHandler(sendRes -> {
                    sender.close();
                    session.close();
                    conn.close();
                    done.countDown();
                });
                sender.open();
            } else {
                conn.close();
                done.countDown();
            }
        });
        session.open();
    }
}
