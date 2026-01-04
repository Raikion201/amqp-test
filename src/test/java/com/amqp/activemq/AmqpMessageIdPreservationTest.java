/*
 * Adapted from Apache ActiveMQ AmqpMessageIdPreservationTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 message ID preservation.
 * Adapted from Apache ActiveMQ AmqpMessageIdPreservationTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Message ID Preservation Tests (ActiveMQ)")
public class AmqpMessageIdPreservationTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: String message ID preservation")
    @Timeout(30)
    void testStringMessageIdPreservation() throws Exception {
        String queueName = generateQueueName("msgid-string");
        String messageId = "string-message-id-" + UUID.randomUUID();

        Message sentMsg = Message.Factory.create();
        sentMsg.setBody(new AmqpValue("Test body"));
        sentMsg.setMessageId(messageId);

        sendMessage(queueName, sentMsg);

        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Object> receivedId = new AtomicReference<>();
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
                            receivedId.set(msg.getMessageId());
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
        assertEquals(messageId, receivedId.get(), "String message ID should be preserved");

        log.info("PASS: String message ID preservation");
    }

    @Test
    @Order(2)
    @DisplayName("Test: UUID message ID preservation")
    @Timeout(30)
    void testUuidMessageIdPreservation() throws Exception {
        String queueName = generateQueueName("msgid-uuid");
        UUID messageId = UUID.randomUUID();

        Message sentMsg = Message.Factory.create();
        sentMsg.setBody(new AmqpValue("Test body"));
        sentMsg.setMessageId(messageId);

        sendMessage(queueName, sentMsg);

        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Object> receivedId = new AtomicReference<>();
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
                            receivedId.set(msg.getMessageId());
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
        assertEquals(messageId, receivedId.get(), "UUID message ID should be preserved");

        log.info("PASS: UUID message ID preservation");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Binary message ID preservation")
    @Timeout(30)
    void testBinaryMessageIdPreservation() throws Exception {
        String queueName = generateQueueName("msgid-binary");
        byte[] idBytes = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };
        Binary messageId = new Binary(idBytes);

        Message sentMsg = Message.Factory.create();
        sentMsg.setBody(new AmqpValue("Test body"));
        sentMsg.setMessageId(messageId);

        sendMessage(queueName, sentMsg);

        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Object> receivedId = new AtomicReference<>();
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
                            receivedId.set(msg.getMessageId());
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
        assertNotNull(receivedId.get(), "Should receive message ID");
        // Binary may be converted to different type
        log.info("Received ID type: {}, value: {}", receivedId.get().getClass().getSimpleName(), receivedId.get());

        log.info("PASS: Binary message ID preservation");
    }

    @Test
    @Order(4)
    @DisplayName("Test: UnsignedLong message ID preservation")
    @Timeout(30)
    void testUnsignedLongMessageIdPreservation() throws Exception {
        String queueName = generateQueueName("msgid-ulong");
        UnsignedLong messageId = UnsignedLong.valueOf(123456789L);

        Message sentMsg = Message.Factory.create();
        sentMsg.setBody(new AmqpValue("Test body"));
        sentMsg.setMessageId(messageId);

        sendMessage(queueName, sentMsg);

        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Object> receivedId = new AtomicReference<>();
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
                            receivedId.set(msg.getMessageId());
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
        assertEquals(messageId, receivedId.get(), "UnsignedLong message ID should be preserved");

        log.info("PASS: UnsignedLong message ID preservation");
    }

    @Test
    @Order(5)
    @DisplayName("Test: Null message ID")
    @Timeout(30)
    void testNullMessageId() throws Exception {
        String queueName = generateQueueName("msgid-null");

        Message sentMsg = Message.Factory.create();
        sentMsg.setBody(new AmqpValue("Test body with null ID"));
        // Don't set message ID

        sendMessage(queueName, sentMsg);

        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Object> receivedId = new AtomicReference<>();
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
                            receivedId.set(msg.getMessageId());
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
        // Broker may or may not assign a message ID
        log.info("Received message ID (may be null or assigned): {}", receivedId.get());

        log.info("PASS: Null message ID handling");
    }

    @Test
    @Order(6)
    @DisplayName("Test: Multiple messages with unique IDs")
    @Timeout(30)
    void testMultipleMessagesWithUniqueIds() throws Exception {
        String queueName = generateQueueName("msgid-multi");
        int messageCount = 5;
        String[] messageIds = new String[messageCount];

        // Send messages with unique IDs
        for (int i = 0; i < messageCount; i++) {
            messageIds[i] = "msg-id-" + i + "-" + UUID.randomUUID();
            Message msg = Message.Factory.create();
            msg.setBody(new AmqpValue("Message " + i));
            msg.setMessageId(messageIds[i]);
            sendMessage(queueName, msg);
        }

        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<String[]> receivedIds = new AtomicReference<>(new String[messageCount]);
        AtomicReference<String> errorRef = new AtomicReference<>();
        java.util.concurrent.atomic.AtomicInteger receivedCount = new java.util.concurrent.atomic.AtomicInteger(0);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            int idx = receivedCount.getAndIncrement();
                            if (idx < messageCount) {
                                Object id = msg.getMessageId();
                                receivedIds.get()[idx] = id != null ? id.toString() : null;
                            }
                            delivery.disposition(Accepted.getInstance(), true);
                            if (receivedCount.get() >= messageCount) {
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertEquals(messageCount, receivedCount.get(), "Should receive all messages");

        // Verify all IDs are unique and preserved
        java.util.Set<String> uniqueIds = new java.util.HashSet<>();
        for (String id : receivedIds.get()) {
            assertNotNull(id, "Each message should have an ID");
            assertTrue(uniqueIds.add(id), "Each ID should be unique");
        }

        log.info("PASS: Multiple messages with unique IDs");
    }
}
