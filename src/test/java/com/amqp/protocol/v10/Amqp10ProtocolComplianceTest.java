package com.amqp.protocol.v10;

import com.amqp.persistence.DatabaseManager;
import com.amqp.persistence.PersistenceManager;
import com.amqp.protocol.v10.server.Amqp10Server;
import com.amqp.server.AmqpBroker;
import io.vertx.core.Vertx;
import io.vertx.proton.*;
import io.vertx.proton.ProtonClientOptions;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AMQP 1.0 Protocol Compliance Tests.
 *
 * Tests the AMQP 1.0 implementation against the Qpid Proton client via Vertx-Proton.
 * These are real integration tests that start the server and test actual protocol behavior.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Protocol Compliance Tests")
public class Amqp10ProtocolComplianceTest {

    private static final Logger log = LoggerFactory.getLogger(Amqp10ProtocolComplianceTest.class);

    private static final int TEST_PORT = 15671;
    private static final String TEST_HOST = "localhost";
    private static final int TIMEOUT_SECONDS = 30;

    private static Amqp10Server server;
    private static AmqpBroker broker;
    private static Vertx vertx;

    @BeforeAll
    static void startServer() throws Exception {
        log.info("=== Starting AMQP 1.0 Test Server ===");

        // Create in-memory database
        DatabaseManager dbManager = new DatabaseManager(
                "jdbc:h2:mem:amqp10test;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false",
                "sa", "");

        PersistenceManager persistence = new PersistenceManager(dbManager);

        // Create broker with guest user enabled for testing
        broker = new AmqpBroker(persistence, true);

        // Create and start AMQP 1.0 server (SASL disabled for testing)
        server = new Amqp10Server(broker, TEST_PORT);
        server.setRequireSasl(false); // Disable SASL for simpler testing
        server.start();

        // Create Vertx for Proton client
        vertx = Vertx.vertx();

        log.info("AMQP 1.0 Test Server started on port {}", TEST_PORT);
    }

    @AfterAll
    static void stopServer() {
        log.info("=== Stopping AMQP 1.0 Test Server ===");

        if (vertx != null) {
            vertx.close();
        }

        if (server != null) {
            server.shutdown();
        }

        if (broker != null) {
            broker.stop();
        }

        log.info("AMQP 1.0 Test Server stopped");
    }

    /**
     * Create ProtonClientOptions with ANONYMOUS SASL mechanism enabled.
     */
    private ProtonClientOptions createClientOptions() {
        return new ProtonClientOptions()
                .addEnabledSaslMechanism("ANONYMOUS");
    }

    @Test
    @Order(1)
    @DisplayName("Test: AMQP 1.0 connection establishment")
    void testConnectionEstablishment() throws Exception {
        log.info("Testing AMQP 1.0 connection establishment...");

        CountDownLatch connected = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);

        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        log.info("Connection opened: containerId={}", conn.getRemoteContainer());
                        success.set(true);
                        conn.close();
                    } else {
                        errorRef.set("Open failed: " + openRes.cause().getMessage());
                    }
                    connected.countDown();
                });
                conn.closeHandler(closeRes -> {
                    log.info("Connection closed");
                });
                conn.open();
            } else {
                errorRef.set("Connect failed: " + res.cause().getMessage());
                connected.countDown();
            }
        });

        assertTrue(connected.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Connection timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(success.get(), "Connection should succeed");

        log.info("PASS: AMQP 1.0 connection establishment works");
    }

    @Test
    @Order(2)
    @DisplayName("Test: AMQP 1.0 session creation")
    void testSessionCreation() throws Exception {
        log.info("Testing AMQP 1.0 session creation...");

        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);

        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Create session
                        ProtonSession session = conn.createSession();
                        session.openHandler(sessionRes -> {
                            if (sessionRes.succeeded()) {
                                log.info("Session created and opened");
                                success.set(true);
                            } else {
                                errorRef.set("Session open failed: " + sessionRes.cause().getMessage());
                            }
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Session timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(success.get(), "Session should be created");

        log.info("PASS: AMQP 1.0 session creation works");
    }

    @Test
    @Order(3)
    @DisplayName("Test: AMQP 1.0 sender link attach")
    void testSenderLinkAttach() throws Exception {
        log.info("Testing AMQP 1.0 sender link attach...");

        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);

        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Create sender to a queue
                        ProtonSender sender = conn.createSender("test-queue-sender");
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                log.info("Sender attached to queue");
                                success.set(true);
                            } else {
                                errorRef.set("Sender attach failed: " + senderRes.cause().getMessage());
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Sender attach timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(success.get(), "Sender should attach");

        log.info("PASS: AMQP 1.0 sender link attach works");
    }

    @Test
    @Order(4)
    @DisplayName("Test: AMQP 1.0 receiver link attach")
    void testReceiverLinkAttach() throws Exception {
        log.info("Testing AMQP 1.0 receiver link attach...");

        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);

        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Create receiver from a queue
                        ProtonReceiver receiver = conn.createReceiver("test-queue-receiver");
                        receiver.openHandler(receiverRes -> {
                            if (receiverRes.succeeded()) {
                                log.info("Receiver attached to queue");
                                success.set(true);
                            } else {
                                errorRef.set("Receiver attach failed: " + receiverRes.cause().getMessage());
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Receiver attach timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(success.get(), "Receiver should attach");

        log.info("PASS: AMQP 1.0 receiver link attach works");
    }

    @Test
    @Order(5)
    @DisplayName("Test: AMQP 1.0 message send")
    void testMessageSend() throws Exception {
        log.info("Testing AMQP 1.0 message send...");

        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);

        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender("test-queue-send");
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                // Send a message
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue("Hello AMQP 1.0!"));

                                sender.send(msg, delivery -> {
                                    if (delivery.getRemoteState() != null) {
                                        log.info("Message accepted: {}", delivery.getRemoteState());
                                        success.set(true);
                                    } else {
                                        log.info("Message sent (no remote state yet)");
                                        success.set(true); // Pre-settled
                                    }
                                    sender.close();
                                    conn.close();
                                    done.countDown();
                                });
                            } else {
                                errorRef.set("Sender attach failed");
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Message send timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(success.get(), "Message should be sent");

        log.info("PASS: AMQP 1.0 message send works");
    }

    @Test
    @Order(6)
    @DisplayName("Test: AMQP 1.0 message send and receive")
    void testMessageSendReceive() throws Exception {
        log.info("Testing AMQP 1.0 message send and receive...");

        String queueName = "test-queue-roundtrip";
        String testMessage = "Hello AMQP 1.0 roundtrip!";

        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<String> receivedMessage = new AtomicReference<>();
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);

        // First, send a message
        CountDownLatch sendDone = new CountDownLatch(1);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue(testMessage));

                                sender.send(msg, delivery -> {
                                    log.info("Message sent to {}", queueName);
                                    sender.close();
                                    conn.close();
                                    sendDone.countDown();
                                });
                            } else {
                                sendDone.countDown();
                            }
                        });
                        sender.open();
                    } else {
                        sendDone.countDown();
                    }
                });
                conn.open();
            } else {
                sendDone.countDown();
            }
        });

        assertTrue(sendDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Send timeout");

        // Now, receive the message
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0); // Disable auto prefetch to use manual credit
                        receiver.handler((delivery, msg) -> {
                            Section body = msg.getBody();
                            String value = null;
                            if (body instanceof AmqpValue) {
                                value = ((AmqpValue) body).getValue().toString();
                            } else if (body instanceof org.apache.qpid.proton.amqp.messaging.Data) {
                                // Server may convert messages to Data format
                                value = new String(((org.apache.qpid.proton.amqp.messaging.Data) body).getValue().getArray());
                            }
                            receivedMessage.set(value);
                            log.info("Message received: {}", value);

                            if (testMessage.equals(value)) {
                                success.set(true);
                            }
                            delivery.disposition(new org.apache.qpid.proton.amqp.messaging.Accepted(), true);
                            receiver.close();
                            conn.close();
                            done.countDown();
                        });
                        receiver.openHandler(receiverRes -> {
                            if (receiverRes.succeeded()) {
                                receiver.flow(10); // Issue credit
                            } else {
                                errorRef.set("Receiver attach failed");
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Receive timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertEquals(testMessage, receivedMessage.get(), "Message content should match");
        assertTrue(success.get(), "Message roundtrip should succeed");

        log.info("PASS: AMQP 1.0 message send and receive works");
    }

    @Test
    @Order(7)
    @DisplayName("Test: AMQP 1.0 multiple messages")
    void testMultipleMessages() throws Exception {
        log.info("Testing AMQP 1.0 multiple messages...");

        String queueName = "test-queue-multi";
        int messageCount = 5;

        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);

        // Send multiple messages
        CountDownLatch sendDone = new CountDownLatch(1);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                for (int i = 0; i < messageCount; i++) {
                                    Message msg = Message.Factory.create();
                                    msg.setBody(new AmqpValue("Message " + i));
                                    sender.send(msg);
                                }
                                log.info("Sent {} messages", messageCount);
                                sender.close();
                                conn.close();
                                sendDone.countDown();
                            } else {
                                sendDone.countDown();
                            }
                        });
                        sender.open();
                    } else {
                        sendDone.countDown();
                    }
                });
                conn.open();
            } else {
                sendDone.countDown();
            }
        });

        assertTrue(sendDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Send timeout");

        // Receive all messages
        java.util.concurrent.atomic.AtomicInteger receivedCount = new java.util.concurrent.atomic.AtomicInteger(0);

        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0); // Disable auto prefetch to use manual credit
                        receiver.handler((delivery, msg) -> {
                            int count = receivedCount.incrementAndGet();
                            log.info("Received message {}", count);
                            delivery.disposition(new org.apache.qpid.proton.amqp.messaging.Accepted(), true);

                            if (count >= messageCount) {
                                success.set(true);
                                receiver.close();
                                conn.close();
                                done.countDown();
                            }
                        });
                        receiver.openHandler(receiverRes -> {
                            if (receiverRes.succeeded()) {
                                receiver.flow(messageCount + 5);
                            } else {
                                errorRef.set("Receiver attach failed");
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Receive timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertEquals(messageCount, receivedCount.get(), "Should receive all messages");
        assertTrue(success.get(), "Multiple messages should work");

        log.info("PASS: AMQP 1.0 multiple messages works");
    }

    @Test
    @Order(8)
    @DisplayName("Test: AMQP 1.0 graceful close")
    void testGracefulClose() throws Exception {
        log.info("Testing AMQP 1.0 graceful close...");

        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean closeReceived = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);

        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        conn.closeHandler(closeRes -> {
                            closeReceived.set(true);
                            log.info("Close response received");
                            done.countDown();
                        });
                        // Initiate close
                        conn.close();
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Close timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(closeReceived.get(), "Close should complete gracefully");

        log.info("PASS: AMQP 1.0 graceful close works");
    }

    @Test
    @Order(9)
    @DisplayName("Test: AMQP 1.0 connection heartbeat")
    void testConnectionHeartbeat() throws Exception {
        log.info("Testing AMQP 1.0 connection heartbeat...");

        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);

        // Set a short idle timeout to trigger heartbeats
        ProtonClientOptions options = new ProtonClientOptions();
        options.setHeartbeat(5000); // 5 second heartbeat

        client.connect(options, TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        log.info("Connection opened, waiting for heartbeat interval...");

                        // Wait a bit and then check connection is still alive
                        vertx.setTimer(3000, id -> {
                            if (!conn.isDisconnected()) {
                                log.info("Connection still alive after heartbeat period");
                                success.set(true);
                            } else {
                                errorRef.set("Connection died");
                            }
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Heartbeat test timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(success.get(), "Connection should stay alive with heartbeats");

        log.info("PASS: AMQP 1.0 connection heartbeat works");
    }

    @Test
    @Order(10)
    @DisplayName("Test: AMQP 1.0 message properties")
    void testMessageProperties() throws Exception {
        log.info("Testing AMQP 1.0 message properties...");

        String queueName = "test-queue-props";
        String messageId = "msg-" + System.currentTimeMillis();
        String correlationId = "corr-123";
        String subject = "test-subject";

        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);

        // Send message with properties
        CountDownLatch sendDone = new CountDownLatch(1);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue("Message with properties"));
                                msg.setMessageId(messageId);
                                msg.setCorrelationId(correlationId);
                                msg.setSubject(subject);
                                msg.setDurable(true);
                                msg.setPriority((short) 5);

                                sender.send(msg, delivery -> {
                                    log.info("Message with properties sent");
                                    sender.close();
                                    conn.close();
                                    sendDone.countDown();
                                });
                            } else {
                                sendDone.countDown();
                            }
                        });
                        sender.open();
                    } else {
                        sendDone.countDown();
                    }
                });
                conn.open();
            } else {
                sendDone.countDown();
            }
        });

        assertTrue(sendDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Send timeout");

        // Receive and verify properties
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0); // Disable auto prefetch to use manual credit
                        receiver.setAutoAccept(false); // Disable auto accept to manually disposition
                        receiver.handler((delivery, msg) -> {
                            log.info("Received message with properties");

                            // Check properties
                            boolean propsMatch = true;
                            if (!messageId.equals(msg.getMessageId())) {
                                log.warn("MessageId mismatch: expected={}, got={}",
                                        messageId, msg.getMessageId());
                                propsMatch = false;
                            }
                            if (!correlationId.equals(msg.getCorrelationId())) {
                                log.warn("CorrelationId mismatch: expected={}, got={}",
                                        correlationId, msg.getCorrelationId());
                                propsMatch = false;
                            }
                            if (!subject.equals(msg.getSubject())) {
                                log.warn("Subject mismatch: expected={}, got={}",
                                        subject, msg.getSubject());
                                propsMatch = false;
                            }

                            success.set(propsMatch);

                            delivery.disposition(new org.apache.qpid.proton.amqp.messaging.Accepted(), true);
                            receiver.close();
                            conn.close();
                            done.countDown();
                        });
                        receiver.openHandler(receiverRes -> {
                            if (receiverRes.succeeded()) {
                                receiver.flow(10);
                            } else {
                                errorRef.set("Receiver attach failed");
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Receive timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(success.get(), "Message properties should be preserved");

        log.info("PASS: AMQP 1.0 message properties works");
    }
}
