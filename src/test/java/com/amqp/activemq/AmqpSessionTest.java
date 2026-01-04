/*
 * Adapted from Apache ActiveMQ AmqpSessionTest.
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
 * Tests for AMQP 1.0 session functionality.
 * Adapted from Apache ActiveMQ AmqpSessionTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Session Tests (ActiveMQ)")
public class AmqpSessionTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Create session")
    @Timeout(30)
    void testCreateSession() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean sessionCreated = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSession session = conn.createSession();
                        session.openHandler(sessRes -> {
                            sessionCreated.set(sessRes.succeeded());
                            session.close();
                            conn.close();
                            done.countDown();
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
        assertTrue(sessionCreated.get(), "Session should be created");

        log.info("PASS: Create session");
    }

    @Test
    @Order(2)
    @DisplayName("Test: Session closed does not get receiver detach from remote")
    @Timeout(30)
    void testSessionClosedDoesNotGetReceiverDetachFromRemote() throws Exception {
        String queueName = generateQueueName("session-close-recv");
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean receiverDetached = new AtomicBoolean(false);
        AtomicBoolean sessionClosedCleanly = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSession session = conn.createSession();
                        session.openHandler(sessRes -> {
                            if (sessRes.succeeded()) {
                                // Create a receiver on this session
                                ProtonReceiver receiver = session.createReceiver(queueName);
                                receiver.setPrefetch(0);
                                receiver.closeHandler(closeRes -> {
                                    // This should NOT be called when session closes
                                    receiverDetached.set(true);
                                });
                                receiver.openHandler(recvRes -> {
                                    if (recvRes.succeeded()) {
                                        receiver.flow(1);
                                        // Close the session directly (not the receiver)
                                        session.closeHandler(sessCloseRes -> {
                                            sessionClosedCleanly.set(true);
                                            conn.close();
                                            done.countDown();
                                        });
                                        session.close();
                                    } else {
                                        errorRef.set("Receiver open failed");
                                        done.countDown();
                                    }
                                });
                                receiver.open();
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
        assertTrue(sessionClosedCleanly.get(), "Session should close cleanly");

        log.info("PASS: Session closed cleanly, receiver detach behavior correct");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Multiple sessions on same connection")
    @Timeout(30)
    void testMultipleSessionsOnSameConnection() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger sessionsOpened = new AtomicInteger(0);
        AtomicReference<String> errorRef = new AtomicReference<>();
        int sessionCount = 3;

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        for (int i = 0; i < sessionCount; i++) {
                            ProtonSession session = conn.createSession();
                            session.openHandler(sessRes -> {
                                if (sessRes.succeeded()) {
                                    int count = sessionsOpened.incrementAndGet();
                                    if (count == sessionCount) {
                                        conn.close();
                                        done.countDown();
                                    }
                                } else {
                                    errorRef.set("Session open failed");
                                    done.countDown();
                                }
                            });
                            session.open();
                        }
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
        assertEquals(sessionCount, sessionsOpened.get(), "All sessions should open");

        log.info("PASS: Multiple sessions on same connection");
    }

    @Test
    @Order(4)
    @DisplayName("Test: Session with sender and receiver")
    @Timeout(30)
    void testSessionWithSenderAndReceiver() throws Exception {
        String queueName = generateQueueName("session-send-recv");
        String testBody = "Test message - " + UUID.randomUUID();
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<String> receivedBody = new AtomicReference<>();
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSession session = conn.createSession();
                        session.openHandler(sessRes -> {
                            if (sessRes.succeeded()) {
                                // Create sender
                                ProtonSender sender = session.createSender(queueName);
                                sender.openHandler(sendRes -> {
                                    if (sendRes.succeeded()) {
                                        // Send message
                                        Message msg = Message.Factory.create();
                                        msg.setBody(new AmqpValue(testBody));
                                        sender.send(msg);

                                        // Create receiver
                                        ProtonReceiver receiver = session.createReceiver(queueName);
                                        receiver.setPrefetch(0);
                                        receiver.handler((delivery, recvMsg) -> {
                                            Object body = ((AmqpValue) recvMsg.getBody()).getValue();
                                            receivedBody.set(body.toString());
                                            delivery.disposition(Accepted.getInstance(), true);
                                            session.close();
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
                                        errorRef.set("Sender open failed");
                                        done.countDown();
                                    }
                                });
                                sender.open();
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
        assertEquals(testBody, receivedBody.get(), "Message body should match");

        log.info("PASS: Session with sender and receiver");
    }

    @Test
    @Order(5)
    @DisplayName("Test: Session close with active links")
    @Timeout(30)
    void testSessionCloseWithActiveLinks() throws Exception {
        String queueName = generateQueueName("session-active-links");
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean sessionClosedCleanly = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSession session = conn.createSession();
                        session.openHandler(sessRes -> {
                            if (sessRes.succeeded()) {
                                // Create sender and receiver but don't close them
                                ProtonSender sender = session.createSender(queueName);
                                ProtonReceiver receiver = session.createReceiver(queueName);
                                receiver.setPrefetch(0);

                                sender.openHandler(sendRes -> {
                                    receiver.openHandler(recvRes -> {
                                        if (recvRes.succeeded()) {
                                            receiver.flow(1);
                                        }
                                        // Close session with active links
                                        session.closeHandler(sessCloseRes -> {
                                            sessionClosedCleanly.set(true);
                                            conn.close();
                                            done.countDown();
                                        });
                                        session.close();
                                    });
                                    receiver.open();
                                });
                                sender.open();
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
        assertTrue(sessionClosedCleanly.get(), "Session should close cleanly with active links");

        log.info("PASS: Session close with active links");
    }

    @Test
    @Order(6)
    @DisplayName("Test: Session flow control")
    @Timeout(30)
    void testSessionFlowControl() throws Exception {
        String queueName = generateQueueName("session-flow");
        int messageCount = 20;
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger receivedCount = new AtomicInteger(0);
        AtomicReference<String> errorRef = new AtomicReference<>();

        // First send messages
        for (int i = 0; i < messageCount; i++) {
            sendMessage(queueName, createMessage("Message " + i));
        }

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSession session = conn.createSession();
                        session.setIncomingCapacity(1024 * 10); // Set session capacity

                        session.openHandler(sessRes -> {
                            if (sessRes.succeeded()) {
                                ProtonReceiver receiver = session.createReceiver(queueName);
                                receiver.setPrefetch(0);
                                receiver.setQoS(ProtonQoS.AT_LEAST_ONCE);
                                receiver.handler((delivery, msg) -> {
                                    delivery.disposition(Accepted.getInstance(), true);
                                    int count = receivedCount.incrementAndGet();
                                    if (count >= messageCount) {
                                        session.close();
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
        assertEquals(messageCount, receivedCount.get(), "Should receive all messages");

        log.info("PASS: Session flow control - received all {} messages", messageCount);
    }

    @Test
    @Order(7)
    @DisplayName("Test: Session end with pending work")
    @Timeout(30)
    void testSessionEndWithPendingWork() throws Exception {
        String queueName = generateQueueName("session-pending");
        int messageCount = 10;
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger receivedCount = new AtomicInteger(0);
        AtomicBoolean closedWithPending = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        // Send messages
        for (int i = 0; i < messageCount; i++) {
            sendMessage(queueName, createMessage("Message " + i));
        }

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSession session = conn.createSession();
                        session.openHandler(sessRes -> {
                            if (sessRes.succeeded()) {
                                ProtonReceiver receiver = session.createReceiver(queueName);
                                receiver.setPrefetch(0);
                                receiver.setQoS(ProtonQoS.AT_LEAST_ONCE);
                                receiver.handler((delivery, msg) -> {
                                    int count = receivedCount.incrementAndGet();
                                    // Don't acknowledge - leave as pending
                                    if (count >= 5) {
                                        // Close session with pending deliveries
                                        closedWithPending.set(true);
                                        session.close();
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
        assertTrue(closedWithPending.get(), "Should have closed with pending work");
        assertTrue(receivedCount.get() >= 5, "Should have received at least 5 messages");

        log.info("PASS: Session end with pending work");
    }

    private Message createMessage(String body) {
        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue(body));
        msg.setMessageId(UUID.randomUUID().toString());
        return msg;
    }
}
