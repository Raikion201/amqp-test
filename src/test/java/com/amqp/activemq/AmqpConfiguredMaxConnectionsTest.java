/*
 * Adapted from Apache ActiveMQ AmqpConfiguredMaxConnectionsTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 maximum connections configuration.
 * Adapted from Apache ActiveMQ AmqpConfiguredMaxConnectionsTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Max Connections Tests (ActiveMQ)")
public class AmqpConfiguredMaxConnectionsTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Single connection succeeds")
    @Timeout(30)
    void testSingleConnectionSucceeds() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<String> errorRef = new AtomicReference<>();
        AtomicReference<ProtonConnection> connRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                connRef.set(conn);
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        done.countDown();
                    } else {
                        errorRef.set("Connection open failed");
                        done.countDown();
                    }
                });
                conn.open();
            } else {
                errorRef.set("Connect failed: " + res.cause().getMessage());
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());

        if (connRef.get() != null) {
            connRef.get().close();
        }

        log.info("PASS: Single connection succeeds");
    }

    @Test
    @Order(2)
    @DisplayName("Test: Multiple connections within limit")
    @Timeout(60)
    void testMultipleConnectionsWithinLimit() throws Exception {
        int connectionCount = 5;
        CountDownLatch done = new CountDownLatch(connectionCount);
        AtomicInteger successCount = new AtomicInteger(0);
        List<ProtonConnection> connections = new ArrayList<>();

        ProtonClient client = ProtonClient.create(vertx);

        for (int i = 0; i < connectionCount; i++) {
            final int idx = i;
            client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    synchronized (connections) {
                        connections.add(conn);
                    }
                    conn.openHandler(openRes -> {
                        if (openRes.succeeded()) {
                            successCount.incrementAndGet();
                        }
                        done.countDown();
                    });
                    conn.open();
                } else {
                    done.countDown();
                }
            });
        }

        assertTrue(done.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS), "Timeout");
        assertEquals(connectionCount, successCount.get(), "All connections should succeed");

        // Cleanup
        synchronized (connections) {
            for (ProtonConnection conn : connections) {
                try {
                    conn.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }

        log.info("PASS: {} connections within limit succeeded", connectionCount);
    }

    @Test
    @Order(3)
    @DisplayName("Test: Connection closed properly releases slot")
    @Timeout(60)
    void testConnectionClosedReleasesSlot() throws Exception {
        // Create and close connections in sequence
        for (int i = 0; i < 3; i++) {
            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<ProtonConnection> connRef = new AtomicReference<>();

            ProtonClient client = ProtonClient.create(vertx);
            client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    connRef.set(conn);
                    conn.openHandler(openRes -> done.countDown());
                    conn.open();
                } else {
                    done.countDown();
                }
            });

            assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Connection " + i + " timeout");
            assertNotNull(connRef.get(), "Connection " + i + " should succeed");

            // Close the connection
            CountDownLatch closeLatch = new CountDownLatch(1);
            connRef.get().closeHandler(closeRes -> closeLatch.countDown());
            connRef.get().close();
            closeLatch.await(5, TimeUnit.SECONDS);
        }

        log.info("PASS: Connection slots released properly");
    }

    @Test
    @Order(4)
    @DisplayName("Test: Concurrent connection attempts")
    @Timeout(60)
    void testConcurrentConnectionAttempts() throws Exception {
        int threadCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        List<ProtonConnection> connections = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    CountDownLatch connLatch = new CountDownLatch(1);

                    ProtonClient client = ProtonClient.create(vertx);
                    client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
                        if (res.succeeded()) {
                            ProtonConnection conn = res.result();
                            synchronized (connections) {
                                connections.add(conn);
                            }
                            conn.openHandler(openRes -> {
                                if (openRes.succeeded()) {
                                    successCount.incrementAndGet();
                                }
                                connLatch.countDown();
                            });
                            conn.open();
                        } else {
                            connLatch.countDown();
                        }
                    });

                    connLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                } catch (Exception e) {
                    // ignore
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        // Start all threads simultaneously
        startLatch.countDown();
        assertTrue(doneLatch.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS), "Timeout");

        assertTrue(successCount.get() > 0, "At least some connections should succeed");
        log.info("PASS: {} of {} concurrent connections succeeded", successCount.get(), threadCount);

        // Cleanup
        synchronized (connections) {
            for (ProtonConnection conn : connections) {
                try {
                    conn.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }
}
