/*
 * Adapted from Apache ActiveMQ AmqpSecurityTest.
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
 * Tests for AMQP 1.0 security features.
 * Adapted from Apache ActiveMQ AmqpSecurityTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Security Tests (ActiveMQ)")
public class AmqpSecurityTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Anonymous connection allowed")
    @Timeout(30)
    void testAnonymousConnectionAllowed() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean connected = new AtomicBoolean(false);

        ProtonClient client = ProtonClient.create(vertx);
        ProtonClientOptions options = new ProtonClientOptions()
            .addEnabledSaslMechanism("ANONYMOUS");

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
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertTrue(connected.get(), "Anonymous connection should be allowed");

        log.info("PASS: Anonymous connection allowed");
    }

    @Test
    @Order(2)
    @DisplayName("Test: Connection with SASL ANONYMOUS")
    @Timeout(30)
    void testConnectionWithSaslAnonymous() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<String> containerIdRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        ProtonClientOptions options = createClientOptions();

        client.connect(options, TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        containerIdRef.set(conn.getRemoteContainer());
                    }
                    conn.close();
                    done.countDown();
                });
                conn.open();
            } else {
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNotNull(containerIdRef.get(), "Should get remote container ID");

        log.info("PASS: SASL ANONYMOUS connection established");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Send and receive with anonymous user")
    @Timeout(30)
    void testSendReceiveWithAnonymousUser() throws Exception {
        String queueName = generateQueueName("security-anon");

        // Send message
        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue("Anonymous user message"));
        sent.setMessageId("anon-msg-1");
        sendMessage(queueName, sent);

        // Receive message
        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message as anonymous user");
        assertEquals("Anonymous user message", ((AmqpValue) received.getBody()).getValue());

        log.info("PASS: Send/receive with anonymous user");
    }

    @Test
    @Order(4)
    @DisplayName("Test: Multiple anonymous connections")
    @Timeout(60)
    void testMultipleAnonymousConnections() throws Exception {
        int connectionCount = 5;
        CountDownLatch done = new CountDownLatch(connectionCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < connectionCount; i++) {
            ProtonClient client = ProtonClient.create(vertx);
            client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        if (openRes.succeeded()) {
                            successCount.incrementAndGet();
                        }
                        conn.close();
                        done.countDown();
                    });
                    conn.open();
                } else {
                    done.countDown();
                }
            });
        }

        assertTrue(done.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS), "Timeout");
        assertEquals(connectionCount, successCount.get(), "All anonymous connections should succeed");

        log.info("PASS: {} anonymous connections established", connectionCount);
    }

    @Test
    @Order(5)
    @DisplayName("Test: Queue access with anonymous user")
    @Timeout(30)
    void testQueueAccessWithAnonymousUser() throws Exception {
        String queueName = generateQueueName("security-access");

        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean senderOpened = new AtomicBoolean(false);
        AtomicBoolean receiverOpened = new AtomicBoolean(false);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Try to create sender
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(sRes -> {
                            senderOpened.set(sRes.succeeded());

                            // Try to create receiver
                            ProtonReceiver receiver = conn.createReceiver(queueName);
                            receiver.setPrefetch(0);
                            receiver.openHandler(rRes -> {
                                receiverOpened.set(rRes.succeeded());
                                receiver.close();
                                sender.close();
                                conn.close();
                                done.countDown();
                            });
                            receiver.open();
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertTrue(senderOpened.get(), "Sender should open for anonymous user");
        assertTrue(receiverOpened.get(), "Receiver should open for anonymous user");

        log.info("PASS: Queue access allowed for anonymous user");
    }

    @Test
    @Order(6)
    @DisplayName("Test: Connection close and reconnect")
    @Timeout(30)
    void testConnectionCloseAndReconnect() throws Exception {
        // First connection
        CountDownLatch done1 = new CountDownLatch(1);
        AtomicReference<ProtonConnection> conn1Ref = new AtomicReference<>();

        ProtonClient client1 = ProtonClient.create(vertx);
        client1.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn1Ref.set(conn);
                conn.openHandler(openRes -> done1.countDown());
                conn.open();
            } else {
                done1.countDown();
            }
        });

        assertTrue(done1.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "First connection timeout");
        assertNotNull(conn1Ref.get(), "First connection should succeed");

        // Close first connection
        CountDownLatch closeLatch = new CountDownLatch(1);
        conn1Ref.get().closeHandler(closeRes -> closeLatch.countDown());
        conn1Ref.get().close();
        closeLatch.await(5, TimeUnit.SECONDS);

        // Second connection
        CountDownLatch done2 = new CountDownLatch(1);
        AtomicBoolean reconnected = new AtomicBoolean(false);

        ProtonClient client2 = ProtonClient.create(vertx);
        client2.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    reconnected.set(openRes.succeeded());
                    conn.close();
                    done2.countDown();
                });
                conn.open();
            } else {
                done2.countDown();
            }
        });

        assertTrue(done2.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Reconnect timeout");
        assertTrue(reconnected.get(), "Should be able to reconnect");

        log.info("PASS: Connection close and reconnect successful");
    }
}
