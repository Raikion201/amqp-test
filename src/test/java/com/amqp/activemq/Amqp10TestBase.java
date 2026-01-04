/*
 * Adapted from Apache ActiveMQ Artemis AMQP protocol tests.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import com.amqp.persistence.DatabaseManager;
import com.amqp.persistence.PersistenceManager;
import com.amqp.protocol.v10.server.Amqp10Server;
import com.amqp.server.AmqpBroker;
import io.vertx.core.Vertx;
import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for AMQP 1.0 integration tests.
 * Adapted from Apache ActiveMQ Artemis test patterns.
 */
public abstract class Amqp10TestBase {

    protected static final Logger log = LoggerFactory.getLogger(Amqp10TestBase.class);

    protected static int TEST_PORT;
    protected static final String TEST_HOST = "localhost";
    protected static final int TIMEOUT_SECONDS = 30;

    protected static Amqp10Server server;
    protected static AmqpBroker broker;
    protected static Vertx vertx;

    /**
     * Find an available port dynamically.
     */
    protected static int findAvailablePort() {
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (Exception e) {
            // Fallback to a random port in the dynamic range
            return 49152 + (int) (Math.random() * 16383);
        }
    }

    @BeforeAll
    static void startServer() throws Exception {
        log.info("=== Starting AMQP 1.0 Test Server ===");

        TEST_PORT = findAvailablePort();

        DatabaseManager dbManager = new DatabaseManager(
                "jdbc:h2:mem:amqp10activemqtest" + TEST_PORT + ";DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false",
                "sa", "");

        PersistenceManager persistence = new PersistenceManager(dbManager);
        broker = new AmqpBroker(persistence, true);

        server = new Amqp10Server(broker, TEST_PORT);
        server.setRequireSasl(false);
        server.setMaxFrameSize(1024 * 1024); // 1MB frame size for testing

        // Use high-throughput config for tests
        com.amqp.protocol.v10.security.Amqp10SecurityConfig testConfig =
            com.amqp.protocol.v10.security.Amqp10SecurityConfig.development();
        testConfig.setConnectionRateLimitPerSecond(1000); // High rate for tests
        testConfig.setMaxConnectionsPerHost(1000);
        testConfig.setMaxTotalConnections(10000);
        server.setSecurityConfig(testConfig);

        server.start();

        vertx = Vertx.vertx();

        log.info("AMQP 1.0 Test Server started on port {}", TEST_PORT);
    }

    @AfterAll
    static void stopServer() {
        log.info("=== Stopping AMQP 1.0 Test Server ===");

        if (vertx != null) {
            try {
                vertx.close();
            } catch (Exception e) {
                log.warn("Error closing vertx", e);
            }
            vertx = null;
        }
        if (server != null) {
            try {
                server.shutdown();
            } catch (Exception e) {
                log.warn("Error shutting down server", e);
            }
            server = null;
        }
        if (broker != null) {
            try {
                broker.stop();
            } catch (Exception e) {
                log.warn("Error stopping broker", e);
            }
            broker = null;
        }

        // Wait a bit for port to be released
        try {
            Thread.sleep(100);
        } catch (InterruptedException ignored) {
        }

        log.info("AMQP 1.0 Test Server stopped");
    }

    protected ProtonClientOptions createClientOptions() {
        return new ProtonClientOptions()
                .addEnabledSaslMechanism("ANONYMOUS")
                .setMaxFrameSize(1024 * 1024); // 1MB frame size for large message tests
    }

    protected String generateQueueName() {
        return "test-queue-" + System.currentTimeMillis() + "-" + Math.random();
    }

    protected String generateQueueName(String prefix) {
        return prefix + "-" + System.currentTimeMillis() + "-" + Math.random();
    }

    /**
     * Send a single message and wait for completion.
     */
    protected void sendMessage(String address, Message message) throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(address);
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                sender.send(message, delivery -> {
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

        if (!done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            throw new RuntimeException("Send timeout");
        }
        if (errorRef.get() != null) {
            throw new RuntimeException(errorRef.get());
        }
    }

    /**
     * Send multiple messages to a queue.
     */
    protected void sendMessages(String address, int count) throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(address);
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                for (int i = 0; i < count; i++) {
                                    Message msg = Message.Factory.create();
                                    msg.setBody(new AmqpValue("Message " + i));
                                    msg.setMessageId("msg-" + i);
                                    sender.send(msg);
                                }
                                sender.close();
                                conn.close();
                                done.countDown();
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

        if (!done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            throw new RuntimeException("Send timeout");
        }
        if (errorRef.get() != null) {
            throw new RuntimeException(errorRef.get());
        }
    }

    /**
     * Receive a single message with timeout.
     */
    protected Message receiveMessage(String address, long timeoutMs) throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Message> messageRef = new AtomicReference<>();
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(address);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            messageRef.set(msg);
                            delivery.disposition(new Accepted(), true);
                            receiver.close();
                            conn.close();
                            done.countDown();
                        });
                        receiver.openHandler(receiverRes -> {
                            if (receiverRes.succeeded()) {
                                receiver.flow(1);
                                // Set timeout for no message
                                vertx.setTimer(timeoutMs, id -> {
                                    if (messageRef.get() == null) {
                                        receiver.close();
                                        conn.close();
                                        done.countDown();
                                    }
                                });
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

        done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (errorRef.get() != null) {
            throw new RuntimeException(errorRef.get());
        }
        return messageRef.get();
    }

    /**
     * Create a message with the specified payload size.
     */
    protected Message createLargeMessage(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 256);
        }
        Message message = Message.Factory.create();
        message.setBody(new Data(new Binary(data)));
        return message;
    }

    /**
     * Create a message with TTL.
     */
    protected Message createMessageWithTTL(String content, long ttlMs) {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue(content));

        Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(ttlMs));
        message.setHeader(header);

        return message;
    }

    /**
     * Create a message with absolute expiry time.
     */
    protected Message createMessageWithAbsoluteExpiry(String content, long expiryTime) {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue(content));

        Properties props = new Properties();
        props.setAbsoluteExpiryTime(new java.util.Date(expiryTime));
        message.setProperties(props);

        return message;
    }

    /**
     * Create a message with scheduled delivery time.
     */
    protected Message createScheduledMessage(String content, long deliveryTime) {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue(content));

        Map<Symbol, Object> annotations = new HashMap<>();
        annotations.put(Symbol.valueOf("x-opt-delivery-time"), deliveryTime);
        message.setMessageAnnotations(new MessageAnnotations(annotations));

        return message;
    }

    /**
     * Create a message with delivery delay.
     */
    protected Message createDelayedMessage(String content, long delayMs) {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue(content));

        Map<Symbol, Object> annotations = new HashMap<>();
        annotations.put(Symbol.valueOf("x-opt-delivery-delay"), delayMs);
        message.setMessageAnnotations(new MessageAnnotations(annotations));

        return message;
    }

    /**
     * Verify payload content matches expected pattern.
     */
    protected void verifyLargeMessagePayload(byte[] received, int expectedSize) {
        Assertions.assertEquals(expectedSize, received.length);
        for (int i = 0; i < received.length; i++) {
            Assertions.assertEquals((byte) (i % 256), received[i],
                "Payload mismatch at index " + i);
        }
    }
}
