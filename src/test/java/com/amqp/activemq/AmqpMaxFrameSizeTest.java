/*
 * Adapted from Apache ActiveMQ AmqpMaxFrameSizeTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.Binary;
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
 * Tests for AMQP 1.0 max frame size handling.
 * Adapted from Apache ActiveMQ AmqpMaxFrameSizeTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Max Frame Size Tests (ActiveMQ)")
public class AmqpMaxFrameSizeTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Connection with default max frame size")
    @Timeout(30)
    void testConnectionWithDefaultMaxFrameSize() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean connected = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        ProtonClientOptions options = createClientOptions();

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
                errorRef.set("Connect failed");
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(connected.get(), "Should connect");

        log.info("PASS: Connection with default max frame size");
    }

    @Test
    @Order(2)
    @DisplayName("Test: Send message smaller than max frame size")
    @Timeout(30)
    void testSendMessageSmallerThanMaxFrameSize() throws Exception {
        String queueName = generateQueueName("frame-small");
        String testBody = "Small message body";

        Message sentMsg = Message.Factory.create();
        sentMsg.setBody(new AmqpValue(testBody));

        sendMessage(queueName, sentMsg);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");

        Object body = ((AmqpValue) received.getBody()).getValue();
        assertEquals(testBody, body, "Message body should match");

        log.info("PASS: Send message smaller than max frame size");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Send message that spans multiple frames")
    @Timeout(60)
    void testSendMessageSpanningMultipleFrames() throws Exception {
        String queueName = generateQueueName("frame-multi");
        // Create a large message body that will span multiple frames
        int bodySize = 100 * 1024; // 100 KB
        byte[] largeBody = new byte[bodySize];
        for (int i = 0; i < bodySize; i++) {
            largeBody[i] = (byte) (i % 256);
        }

        Message sentMsg = Message.Factory.create();
        sentMsg.setBody(new Data(new Binary(largeBody)));
        sentMsg.setMessageId(UUID.randomUUID().toString());

        sendMessage(queueName, sentMsg);

        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<byte[]> receivedBody = new AtomicReference<>();
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setQoS(ProtonQoS.AT_LEAST_ONCE);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            if (msg.getBody() instanceof Data) {
                                Binary data = ((Data) msg.getBody()).getValue();
                                receivedBody.set(data.getArray());
                            }
                            delivery.disposition(Accepted.getInstance(), true);
                            receiver.close();
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
        assertNotNull(receivedBody.get(), "Should receive message body");
        assertEquals(bodySize, receivedBody.get().length, "Body size should match");

        // Verify content
        for (int i = 0; i < bodySize; i++) {
            assertEquals((byte) (i % 256), receivedBody.get()[i], "Byte at position " + i + " should match");
        }

        log.info("PASS: Send message spanning multiple frames ({} bytes)", bodySize);
    }

    @Test
    @Order(4)
    @DisplayName("Test: Multiple large messages")
    @Timeout(90)
    void testMultipleLargeMessages() throws Exception {
        String queueName = generateQueueName("frame-multi-large");
        int messageCount = 5;
        int bodySize = 50 * 1024; // 50 KB per message

        // Send multiple large messages
        for (int i = 0; i < messageCount; i++) {
            byte[] body = new byte[bodySize];
            java.util.Arrays.fill(body, (byte) i);

            Message msg = Message.Factory.create();
            msg.setBody(new Data(new Binary(body)));
            msg.setMessageId("large-msg-" + i);
            sendMessage(queueName, msg);
        }

        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger receivedCount = new AtomicInteger(0);
        AtomicBoolean allSizesCorrect = new AtomicBoolean(true);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setQoS(ProtonQoS.AT_LEAST_ONCE);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            if (msg.getBody() instanceof Data) {
                                Binary data = ((Data) msg.getBody()).getValue();
                                if (data.getLength() != bodySize) {
                                    allSizesCorrect.set(false);
                                }
                            }
                            delivery.disposition(Accepted.getInstance(), true);
                            int count = receivedCount.incrementAndGet();
                            if (count >= messageCount) {
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

        assertTrue(done.await(TIMEOUT_SECONDS * 3, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertEquals(messageCount, receivedCount.get(), "Should receive all messages");
        assertTrue(allSizesCorrect.get(), "All message sizes should be correct");

        log.info("PASS: Multiple large messages ({} messages of {} KB each)", messageCount, bodySize / 1024);
    }

    @Test
    @Order(5)
    @DisplayName("Test: Send and receive with exact frame boundary")
    @Timeout(30)
    void testExactFrameBoundary() throws Exception {
        String queueName = generateQueueName("frame-boundary");

        // Create message with body size close to typical frame size
        int bodySize = 8 * 1024; // 8 KB - common frame size boundary
        byte[] body = new byte[bodySize];
        for (int i = 0; i < bodySize; i++) {
            body[i] = (byte) ('A' + (i % 26));
        }

        Message sentMsg = Message.Factory.create();
        sentMsg.setBody(new Data(new Binary(body)));
        sentMsg.setMessageId(UUID.randomUUID().toString());

        sendMessage(queueName, sentMsg);

        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<byte[]> receivedBody = new AtomicReference<>();
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            if (msg.getBody() instanceof Data) {
                                Binary data = ((Data) msg.getBody()).getValue();
                                receivedBody.set(data.getArray());
                            }
                            delivery.disposition(Accepted.getInstance(), true);
                            receiver.close();
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
        assertNotNull(receivedBody.get(), "Should receive message");
        assertArrayEquals(body, receivedBody.get(), "Body should match exactly");

        log.info("PASS: Send and receive with exact frame boundary");
    }
}
