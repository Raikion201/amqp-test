package com.amqp.docker;

import io.vertx.core.Vertx;
import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AMQP 1.0 Docker-based Compliance Tests
 *
 * Tests the AMQP server running in a Docker container using the
 * Vert.x Proton client library.
 *
 * These tests verify compliance with AMQP 1.0 specification (OASIS)
 * by using a real client against the containerized server.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Docker Compliance Tests")
@Tag("docker")
public class Amqp10DockerComplianceTest {

    private static final Logger log = LoggerFactory.getLogger(Amqp10DockerComplianceTest.class);
    private static final int TIMEOUT_SECONDS = 30;

    private static AmqpServerContainer container;
    private static Vertx vertx;

    @BeforeAll
    static void startContainer() {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                AmqpServerContainer.isDockerAvailable(),
                "Docker is not available - skipping Docker compliance tests");

        container = AmqpServerContainer.createContainer();
        container.start();
        vertx = Vertx.vertx();
    }

    @AfterAll
    static void stopContainer() {
        if (vertx != null) {
            vertx.close();
        }
        if (container != null) {
            container.stop();
        }
    }

    private ProtonClientOptions createClientOptions() {
        return new ProtonClientOptions()
                .addEnabledSaslMechanism("ANONYMOUS")
                .setMaxFrameSize(1024 * 1024);
    }

    private String generateQueueName(String prefix) {
        return prefix + "-" + System.currentTimeMillis() + "-" + Math.random();
    }

    // ==================== Connection Tests ====================

    @Nested
    @DisplayName("Connection Compliance")
    class ConnectionCompliance {

        @Test
        @Order(1)
        @DisplayName("Connection opens successfully")
        @Timeout(TIMEOUT_SECONDS)
        void testConnectionOpens() throws Exception {
            CountDownLatch done = new CountDownLatch(1);
            AtomicBoolean connectionOpened = new AtomicBoolean(false);
            AtomicReference<String> remoteContainer = new AtomicReference<>();
            AtomicReference<Throwable> errorRef = new AtomicReference<>();

            log.info("Attempting to connect to AMQP 1.0 server at {}:{}",
                    container.getHost(), container.getAmqp10Port());

            ProtonClient client = ProtonClient.create(vertx);
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.setContainer("test-client-" + UUID.randomUUID());
                    conn.openHandler(openRes -> {
                        if (openRes.succeeded()) {
                            connectionOpened.set(true);
                            remoteContainer.set(conn.getRemoteContainer());
                            conn.close();
                            done.countDown();
                        } else {
                            errorRef.set(openRes.cause());
                            done.countDown();
                        }
                    });
                    conn.open();
                } else {
                    log.error("Connection failed: {}", res.cause().getMessage());
                    errorRef.set(res.cause());
                    done.countDown();
                }
            });

            assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Connection attempt timed out");
            if (errorRef.get() != null) {
                log.error("Connection error", errorRef.get());
            }
            assertTrue(connectionOpened.get(), "Connection should be opened, error: " +
                    (errorRef.get() != null ? errorRef.get().getMessage() : "unknown"));
            assertNotNull(remoteContainer.get());
            log.info("PASS: Connection opened - server container: {}", remoteContainer.get());
        }

        @Test
        @Order(2)
        @DisplayName("Connection closes gracefully")
        @Timeout(60) // Longer timeout for full test suite
        void testConnectionCloses() throws Exception {
            CountDownLatch openDone = new CountDownLatch(1);
            CountDownLatch closeDone = new CountDownLatch(1);
            AtomicBoolean connectionOpened = new AtomicBoolean(false);
            AtomicBoolean disconnected = new AtomicBoolean(false);

            ProtonClient client = ProtonClient.create(vertx);
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.disconnectHandler(v -> {
                        disconnected.set(true);
                        closeDone.countDown();
                    });
                    conn.closeHandler(closeRes -> {
                        closeDone.countDown();
                    });
                    conn.openHandler(openRes -> {
                        connectionOpened.set(true);
                        openDone.countDown();
                        conn.close();
                    });
                    conn.open();
                } else {
                    openDone.countDown();
                    closeDone.countDown();
                }
            });

            // First wait for connection to open
            assertTrue(openDone.await(30, TimeUnit.SECONDS), "Connection should open");
            assertTrue(connectionOpened.get(), "Connection should be opened");

            // Then wait for close (either closeHandler or disconnectHandler)
            assertTrue(closeDone.await(30, TimeUnit.SECONDS), "Connection should close");
            log.info("PASS: Connection closed gracefully (disconnected={})", disconnected.get());
        }

        @Test
        @Order(3)
        @DisplayName("Multiple connections supported")
        @Timeout(TIMEOUT_SECONDS)
        void testMultipleConnections() throws Exception {
            CountDownLatch done = new CountDownLatch(3);
            AtomicInteger openedCount = new AtomicInteger(0);
            AtomicReference<Throwable> errorRef = new AtomicReference<>();

            log.info("Testing multiple connections to {}:{}",
                    container.getHost(), container.getAmqp10Port());

            ProtonClient client = ProtonClient.create(vertx);

            for (int i = 0; i < 3; i++) {
                final int connNum = i;
                client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                    if (res.succeeded()) {
                        ProtonConnection conn = res.result();
                        conn.openHandler(openRes -> {
                            if (openRes.succeeded()) {
                                openedCount.incrementAndGet();
                                log.debug("Connection {} opened", connNum);
                                conn.close();
                                done.countDown();
                            } else {
                                log.error("Connection {} open failed", connNum, openRes.cause());
                                errorRef.compareAndSet(null, openRes.cause());
                                done.countDown();
                            }
                        });
                        conn.open();
                    } else {
                        log.error("Connection {} failed to connect", connNum, res.cause());
                        errorRef.compareAndSet(null, res.cause());
                        done.countDown();
                    }
                });
            }

            assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Connections timed out");
            if (errorRef.get() != null) {
                log.error("Connection error", errorRef.get());
            }
            assertEquals(3, openedCount.get(), "Expected 3 connections, error: " +
                    (errorRef.get() != null ? errorRef.get().getMessage() : "unknown"));
            log.info("PASS: Multiple connections supported");
        }
    }

    // ==================== Session Tests ====================

    @Nested
    @DisplayName("Session Compliance")
    class SessionCompliance {

        @Test
        @Order(1)
        @DisplayName("Session opens successfully")
        @Timeout(TIMEOUT_SECONDS)
        void testSessionOpens() throws Exception {
            CountDownLatch done = new CountDownLatch(1);
            AtomicBoolean sessionOpened = new AtomicBoolean(false);

            ProtonClient client = ProtonClient.create(vertx);
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonSession session = conn.createSession();
                        session.openHandler(sessionRes -> {
                            if (sessionRes.succeeded()) {
                                sessionOpened.set(true);
                            }
                            session.close();
                            conn.close();
                            done.countDown();
                        });
                        session.open();
                    });
                    conn.open();
                }
            });

            assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue(sessionOpened.get());
            log.info("PASS: Session opened");
        }

        @Test
        @Order(2)
        @DisplayName("Multiple sessions per connection")
        @Timeout(TIMEOUT_SECONDS)
        void testMultipleSessions() throws Exception {
            CountDownLatch done = new CountDownLatch(1);
            AtomicInteger sessionCount = new AtomicInteger(0);

            ProtonClient client = ProtonClient.create(vertx);
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        for (int i = 0; i < 3; i++) {
                            ProtonSession session = conn.createSession();
                            session.openHandler(sessionRes -> {
                                if (sessionRes.succeeded()) {
                                    int count = sessionCount.incrementAndGet();
                                    if (count == 3) {
                                        conn.close();
                                        done.countDown();
                                    }
                                }
                            });
                            session.open();
                        }
                    });
                    conn.open();
                }
            });

            assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertEquals(3, sessionCount.get());
            log.info("PASS: Multiple sessions per connection");
        }
    }

    // ==================== Link Tests ====================

    @Nested
    @DisplayName("Link Compliance")
    class LinkCompliance {

        @Test
        @Order(1)
        @DisplayName("Sender link attaches")
        @Timeout(TIMEOUT_SECONDS)
        void testSenderAttaches() throws Exception {
            String queueName = generateQueueName("link-sender");
            CountDownLatch done = new CountDownLatch(1);
            AtomicBoolean senderAttached = new AtomicBoolean(false);

            ProtonClient client = ProtonClient.create(vertx);
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(senderRes -> {
                            senderAttached.set(senderRes.succeeded());
                            sender.close();
                            conn.close();
                            done.countDown();
                        });
                        sender.open();
                    });
                    conn.open();
                }
            });

            assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue(senderAttached.get());
            log.info("PASS: Sender link attached");
        }

        @Test
        @Order(2)
        @DisplayName("Receiver link attaches")
        @Timeout(TIMEOUT_SECONDS)
        void testReceiverAttaches() throws Exception {
            String queueName = generateQueueName("link-receiver");
            CountDownLatch done = new CountDownLatch(1);
            AtomicBoolean receiverAttached = new AtomicBoolean(false);

            ProtonClient client = ProtonClient.create(vertx);
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.openHandler(receiverRes -> {
                            receiverAttached.set(receiverRes.succeeded());
                            receiver.close();
                            conn.close();
                            done.countDown();
                        });
                        receiver.open();
                    });
                    conn.open();
                }
            });

            assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue(receiverAttached.get());
            log.info("PASS: Receiver link attached");
        }

        @Test
        @Order(3)
        @DisplayName("Link detaches gracefully")
        @Timeout(TIMEOUT_SECONDS)
        void testLinkDetaches() throws Exception {
            String queueName = generateQueueName("link-detach");
            CountDownLatch done = new CountDownLatch(1);
            AtomicBoolean linkOpened = new AtomicBoolean(false);

            ProtonClient client = ProtonClient.create(vertx);
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                linkOpened.set(true);
                                sender.close();
                                vertx.setTimer(500, id -> {
                                    conn.close();
                                    done.countDown();
                                });
                            }
                        });
                        sender.open();
                    });
                    conn.open();
                }
            });

            assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue(linkOpened.get());
            log.info("PASS: Link detached gracefully");
        }
    }

    // ==================== Message Transfer Tests ====================

    @Nested
    @DisplayName("Message Transfer Compliance")
    class MessageCompliance {

        @Test
        @Order(1)
        @DisplayName("Simple message transfer")
        @Timeout(TIMEOUT_SECONDS)
        void testSimpleMessageTransfer() throws Exception {
            String queueName = generateQueueName("transfer-simple");
            String messageBody = "Hello AMQP 1.0!";
            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<String> receivedBody = new AtomicReference<>();

            ProtonClient client = ProtonClient.create(vertx);

            // Send message
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(senderRes -> {
                            Message msg = Message.Factory.create();
                            msg.setBody(new AmqpValue(messageBody));
                            sender.send(msg, delivery -> {
                                sender.close();
                                conn.close();

                                // Receive message
                                receiveMessage(queueName, receivedBody, done);
                            });
                        });
                        sender.open();
                    });
                    conn.open();
                }
            });

            assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertEquals(messageBody, receivedBody.get());
            log.info("PASS: Simple message transfer succeeded");
        }

        private void receiveMessage(String queueName, AtomicReference<String> result, CountDownLatch done) {
            ProtonClient client = ProtonClient.create(vertx);
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            if (msg.getBody() instanceof AmqpValue) {
                                result.set(((AmqpValue) msg.getBody()).getValue().toString());
                            }
                            delivery.disposition(new Accepted(), true);
                            receiver.close();
                            conn.close();
                            done.countDown();
                        });
                        receiver.openHandler(r -> receiver.flow(1));
                        receiver.open();
                    });
                    conn.open();
                }
            });
        }

        @Test
        @Order(2)
        @DisplayName("Message with properties")
        @Timeout(TIMEOUT_SECONDS)
        void testMessageWithProperties() throws Exception {
            String queueName = generateQueueName("transfer-props");
            String messageId = "msg-" + UUID.randomUUID();
            String correlationId = "corr-" + UUID.randomUUID();
            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<String> receivedMsgId = new AtomicReference<>();
            AtomicReference<String> receivedCorrId = new AtomicReference<>();

            ProtonClient client = ProtonClient.create(vertx);

            // Send message with properties
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(senderRes -> {
                            Message msg = Message.Factory.create();
                            msg.setBody(new AmqpValue("Test"));
                            msg.setMessageId(messageId);
                            msg.setCorrelationId(correlationId);
                            msg.setSubject("test-subject");
                            msg.setContentType("text/plain");

                            sender.send(msg, delivery -> {
                                sender.close();
                                conn.close();

                                // Receive and verify
                                receiveWithProperties(queueName, receivedMsgId, receivedCorrId, done);
                            });
                        });
                        sender.open();
                    });
                    conn.open();
                }
            });

            assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertEquals(messageId, receivedMsgId.get());
            assertEquals(correlationId, receivedCorrId.get());
            log.info("PASS: Message properties preserved");
        }

        private void receiveWithProperties(String queueName,
                                           AtomicReference<String> msgId,
                                           AtomicReference<String> corrId,
                                           CountDownLatch done) {
            ProtonClient client = ProtonClient.create(vertx);
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            if (msg.getMessageId() != null) {
                                msgId.set(msg.getMessageId().toString());
                            }
                            if (msg.getCorrelationId() != null) {
                                corrId.set(msg.getCorrelationId().toString());
                            }
                            delivery.disposition(new Accepted(), true);
                            receiver.close();
                            conn.close();
                            done.countDown();
                        });
                        receiver.openHandler(r -> receiver.flow(1));
                        receiver.open();
                    });
                    conn.open();
                }
            });
        }

        @Test
        @Order(3)
        @DisplayName("Binary message body")
        @Timeout(TIMEOUT_SECONDS)
        void testBinaryMessageBody() throws Exception {
            String queueName = generateQueueName("transfer-binary");
            byte[] payload = new byte[256];
            for (int i = 0; i < payload.length; i++) {
                payload[i] = (byte) i;
            }
            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<byte[]> receivedPayload = new AtomicReference<>();

            ProtonClient client = ProtonClient.create(vertx);

            // Send binary message
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(senderRes -> {
                            Message msg = Message.Factory.create();
                            msg.setBody(new Data(new Binary(payload)));

                            sender.send(msg, delivery -> {
                                sender.close();
                                conn.close();

                                // Receive binary
                                receiveBinary(queueName, receivedPayload, done);
                            });
                        });
                        sender.open();
                    });
                    conn.open();
                }
            });

            assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertArrayEquals(payload, receivedPayload.get());
            log.info("PASS: Binary message body preserved");
        }

        private void receiveBinary(String queueName, AtomicReference<byte[]> result, CountDownLatch done) {
            ProtonClient client = ProtonClient.create(vertx);
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            if (msg.getBody() instanceof Data) {
                                result.set(((Data) msg.getBody()).getValue().getArray());
                            }
                            delivery.disposition(new Accepted(), true);
                            receiver.close();
                            conn.close();
                            done.countDown();
                        });
                        receiver.openHandler(r -> receiver.flow(1));
                        receiver.open();
                    });
                    conn.open();
                }
            });
        }

        @Test
        @Order(4)
        @DisplayName("Multiple messages in order")
        @Timeout(TIMEOUT_SECONDS)
        void testMultipleMessagesOrder() throws Exception {
            String queueName = generateQueueName("transfer-order");
            int messageCount = 10;
            CountDownLatch sendDone = new CountDownLatch(1);
            CountDownLatch receiveDone = new CountDownLatch(messageCount);
            AtomicInteger counter = new AtomicInteger(0);
            AtomicReference<String> errorRef = new AtomicReference<>();

            ProtonClient client = ProtonClient.create(vertx);

            // Send messages
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(senderRes -> {
                            for (int i = 0; i < messageCount; i++) {
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue("Message-" + i));
                                sender.send(msg);
                            }
                            vertx.setTimer(500, id -> {
                                sender.close();
                                conn.close();
                                sendDone.countDown();
                            });
                        });
                        sender.open();
                    });
                    conn.open();
                }
            });

            assertTrue(sendDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            // Receive messages
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            int expected = counter.getAndIncrement();
                            String body = ((AmqpValue) msg.getBody()).getValue().toString();
                            if (!body.equals("Message-" + expected)) {
                                errorRef.set("Out of order: expected " + expected + ", got " + body);
                            }
                            delivery.disposition(new Accepted(), true);
                            receiveDone.countDown();
                            if (counter.get() < messageCount) {
                                receiver.flow(1);
                            } else {
                                receiver.close();
                                conn.close();
                            }
                        });
                        receiver.openHandler(r -> receiver.flow(1));
                        receiver.open();
                    });
                    conn.open();
                }
            });

            assertTrue(receiveDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertNull(errorRef.get(), errorRef.get());
            log.info("PASS: {} messages delivered in order", messageCount);
        }

        @Test
        @Order(5)
        @DisplayName("Application properties preserved")
        @Timeout(TIMEOUT_SECONDS)
        void testApplicationProperties() throws Exception {
            String queueName = generateQueueName("transfer-app-props");
            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<Map<String, Object>> receivedProps = new AtomicReference<>();

            Map<String, Object> appProps = new HashMap<>();
            appProps.put("string-prop", "value1");
            appProps.put("int-prop", 42);
            appProps.put("boolean-prop", true);

            ProtonClient client = ProtonClient.create(vertx);

            // Send with app properties
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(senderRes -> {
                            Message msg = Message.Factory.create();
                            msg.setBody(new AmqpValue("Test"));
                            msg.setApplicationProperties(new ApplicationProperties(appProps));

                            sender.send(msg, delivery -> {
                                sender.close();
                                conn.close();

                                // Receive
                                receiveAppProps(queueName, receivedProps, done);
                            });
                        });
                        sender.open();
                    });
                    conn.open();
                }
            });

            assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertNotNull(receivedProps.get());
            assertEquals("value1", receivedProps.get().get("string-prop"));
            assertEquals(42, receivedProps.get().get("int-prop"));
            log.info("PASS: Application properties preserved");
        }

        private void receiveAppProps(String queueName, AtomicReference<Map<String, Object>> result, CountDownLatch done) {
            ProtonClient client = ProtonClient.create(vertx);
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            if (msg.getApplicationProperties() != null) {
                                result.set(msg.getApplicationProperties().getValue());
                            }
                            delivery.disposition(new Accepted(), true);
                            receiver.close();
                            conn.close();
                            done.countDown();
                        });
                        receiver.openHandler(r -> receiver.flow(1));
                        receiver.open();
                    });
                    conn.open();
                }
            });
        }
    }

    // ==================== Delivery Outcome Tests ====================

    @Nested
    @DisplayName("Delivery Outcome Compliance")
    class DeliveryOutcomeCompliance {

        @Test
        @Order(1)
        @DisplayName("Accepted outcome")
        @Timeout(TIMEOUT_SECONDS)
        void testAcceptedOutcome() throws Exception {
            String queueName = generateQueueName("outcome-accepted");
            CountDownLatch done = new CountDownLatch(1);
            AtomicBoolean accepted = new AtomicBoolean(false);

            // Send message
            sendMessage(queueName, "Accept me", () -> {
                // Receive and accept
                ProtonClient client = ProtonClient.create(vertx);
                client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                    if (res.succeeded()) {
                        ProtonConnection conn = res.result();
                        conn.openHandler(openRes -> {
                            ProtonReceiver receiver = conn.createReceiver(queueName);
                            receiver.setPrefetch(0);
                            receiver.handler((delivery, msg) -> {
                                delivery.disposition(new Accepted(), true);
                                accepted.set(true);
                                receiver.close();
                                conn.close();
                                done.countDown();
                            });
                            receiver.openHandler(r -> receiver.flow(1));
                            receiver.open();
                        });
                        conn.open();
                    }
                });
            });

            assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue(accepted.get());
            log.info("PASS: Accepted outcome works");
        }

        @Test
        @Order(2)
        @DisplayName("Released outcome causes redelivery")
        @Timeout(TIMEOUT_SECONDS)
        void testReleasedOutcome() throws Exception {
            String queueName = generateQueueName("outcome-released");
            CountDownLatch done = new CountDownLatch(1);
            AtomicInteger deliveryCount = new AtomicInteger(0);

            // Send message
            sendMessage(queueName, "Release me", () -> {
                // Receive and release first, accept second
                ProtonClient client = ProtonClient.create(vertx);
                client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                    if (res.succeeded()) {
                        ProtonConnection conn = res.result();
                        conn.openHandler(openRes -> {
                            ProtonReceiver receiver = conn.createReceiver(queueName);
                            receiver.setPrefetch(0);
                            receiver.handler((delivery, msg) -> {
                                int count = deliveryCount.incrementAndGet();
                                if (count == 1) {
                                    delivery.disposition(new Released(), true);
                                    receiver.flow(1);
                                } else {
                                    delivery.disposition(new Accepted(), true);
                                    receiver.close();
                                    conn.close();
                                    done.countDown();
                                }
                            });
                            receiver.openHandler(r -> receiver.flow(1));
                            receiver.open();
                        });
                        conn.open();
                    }
                });
            });

            assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertEquals(2, deliveryCount.get());
            log.info("PASS: Released outcome causes redelivery");
        }

        private void sendMessage(String queueName, String body, Runnable callback) {
            ProtonClient client = ProtonClient.create(vertx);
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(senderRes -> {
                            Message msg = Message.Factory.create();
                            msg.setBody(new AmqpValue(body));
                            sender.send(msg, delivery -> {
                                sender.close();
                                conn.close();
                                callback.run();
                            });
                        });
                        sender.open();
                    });
                    conn.open();
                }
            });
        }
    }

    // ==================== Flow Control Tests ====================

    @Nested
    @DisplayName("Flow Control Compliance")
    class FlowControlCompliance {

        @Test
        @Order(1)
        @DisplayName("Link credit controls delivery")
        @Timeout(TIMEOUT_SECONDS)
        void testLinkCreditControls() throws Exception {
            String queueName = generateQueueName("flow-credit");
            int messageCount = 10;
            int creditIssued = 5;
            CountDownLatch sendDone = new CountDownLatch(1);
            CountDownLatch receiveDone = new CountDownLatch(1);
            AtomicInteger receivedCount = new AtomicInteger(0);

            // Send messages
            ProtonClient client = ProtonClient.create(vertx);
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(senderRes -> {
                            for (int i = 0; i < messageCount; i++) {
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue("Message-" + i));
                                sender.send(msg);
                            }
                            vertx.setTimer(500, id -> {
                                sender.close();
                                conn.close();
                                sendDone.countDown();
                            });
                        });
                        sender.open();
                    });
                    conn.open();
                }
            });

            assertTrue(sendDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            // Receive with limited credit
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            receivedCount.incrementAndGet();
                            delivery.disposition(new Accepted(), true);
                        });
                        receiver.openHandler(r -> {
                            receiver.flow(creditIssued);
                            // Wait then check count
                            vertx.setTimer(2000, id -> {
                                receiver.close();
                                conn.close();
                                receiveDone.countDown();
                            });
                        });
                        receiver.open();
                    });
                    conn.open();
                }
            });

            assertTrue(receiveDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertEquals(creditIssued, receivedCount.get());
            log.info("PASS: Link credit limits delivery to {} messages", creditIssued);
        }

        @Test
        @Order(2)
        @DisplayName("Zero credit means no delivery")
        @Timeout(TIMEOUT_SECONDS)
        void testZeroCreditNoDelivery() throws Exception {
            String queueName = generateQueueName("flow-zero");
            CountDownLatch sendDone = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(1);
            AtomicBoolean messageReceived = new AtomicBoolean(false);

            // Send message
            ProtonClient client = ProtonClient.create(vertx);
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(senderRes -> {
                            Message msg = Message.Factory.create();
                            msg.setBody(new AmqpValue("No delivery"));
                            sender.send(msg, d -> {
                                sender.close();
                                conn.close();
                                sendDone.countDown();
                            });
                        });
                        sender.open();
                    });
                    conn.open();
                }
            });

            assertTrue(sendDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            // Receive with zero credit
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> messageReceived.set(true));
                        receiver.openHandler(r -> {
                            // Don't issue any credit
                            vertx.setTimer(2000, id -> {
                                receiver.close();
                                conn.close();
                                done.countDown();
                            });
                        });
                        receiver.open();
                    });
                    conn.open();
                }
            });

            assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertFalse(messageReceived.get());
            log.info("PASS: Zero credit prevents delivery");
        }
    }

    // ==================== Large Message Tests ====================

    @Nested
    @DisplayName("Large Message Compliance")
    class LargeMessageCompliance {

        @Test
        @Order(1)
        @DisplayName("Large message transfer")
        @Timeout(60)
        void testLargeMessageTransfer() throws Exception {
            String queueName = generateQueueName("large-msg");
            int size = 100 * 1024; // 100KB
            byte[] payload = new byte[size];
            for (int i = 0; i < size; i++) {
                payload[i] = (byte) (i % 256);
            }
            CountDownLatch sendDone = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<byte[]> receivedPayload = new AtomicReference<>();

            // Send large message
            ProtonClient client = ProtonClient.create(vertx);
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(senderRes -> {
                            Message msg = Message.Factory.create();
                            msg.setBody(new Data(new Binary(payload)));
                            sender.send(msg, d -> {
                                sender.close();
                                conn.close();
                                sendDone.countDown();
                            });
                        });
                        sender.open();
                    });
                    conn.open();
                }
            });

            assertTrue(sendDone.await(60, TimeUnit.SECONDS)); // Longer timeout for large messages

            // Receive large message
            client.connect(createClientOptions(), container.getHost(), container.getAmqp10Port(), res -> {
                if (res.succeeded()) {
                    ProtonConnection conn = res.result();
                    conn.openHandler(openRes -> {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            if (msg.getBody() instanceof Data) {
                                receivedPayload.set(((Data) msg.getBody()).getValue().getArray());
                            }
                            delivery.disposition(new Accepted(), true);
                            receiver.close();
                            conn.close();
                            done.countDown();
                        });
                        receiver.openHandler(r -> receiver.flow(1));
                        receiver.open();
                    });
                    conn.open();
                }
            });

            assertTrue(done.await(60, TimeUnit.SECONDS));
            assertArrayEquals(payload, receivedPayload.get());
            log.info("PASS: Large message ({}KB) transferred correctly", size / 1024);
        }
    }
}
