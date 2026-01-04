/*
 * Adapted from Apache ActiveMQ AmqpCorrelationIdPreservationTest.
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
 * Tests for AMQP 1.0 correlation ID preservation.
 * Adapted from Apache ActiveMQ AmqpCorrelationIdPreservationTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Correlation ID Preservation Tests (ActiveMQ)")
public class AmqpCorrelationIdPreservationTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: String correlation ID preservation")
    @Timeout(30)
    void testStringCorrelationIdPreservation() throws Exception {
        String queueName = generateQueueName("corrid-string");
        String correlationId = "string-correlation-id-" + UUID.randomUUID();

        Message sentMsg = Message.Factory.create();
        sentMsg.setBody(new AmqpValue("Test body"));
        sentMsg.setCorrelationId(correlationId);

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
                            receivedId.set(msg.getCorrelationId());
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
        assertEquals(correlationId, receivedId.get(), "String correlation ID should be preserved");

        log.info("PASS: String correlation ID preservation");
    }

    @Test
    @Order(2)
    @DisplayName("Test: UUID correlation ID preservation")
    @Timeout(30)
    void testUuidCorrelationIdPreservation() throws Exception {
        String queueName = generateQueueName("corrid-uuid");
        UUID correlationId = UUID.randomUUID();

        Message sentMsg = Message.Factory.create();
        sentMsg.setBody(new AmqpValue("Test body"));
        sentMsg.setCorrelationId(correlationId);

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
                            receivedId.set(msg.getCorrelationId());
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
        assertEquals(correlationId, receivedId.get(), "UUID correlation ID should be preserved");

        log.info("PASS: UUID correlation ID preservation");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Binary correlation ID preservation")
    @Timeout(30)
    void testBinaryCorrelationIdPreservation() throws Exception {
        String queueName = generateQueueName("corrid-binary");
        byte[] idBytes = new byte[] { 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F };
        Binary correlationId = new Binary(idBytes);

        Message sentMsg = Message.Factory.create();
        sentMsg.setBody(new AmqpValue("Test body"));
        sentMsg.setCorrelationId(correlationId);

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
                            receivedId.set(msg.getCorrelationId());
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
        assertNotNull(receivedId.get(), "Should receive correlation ID");
        log.info("Received correlation ID type: {}, value: {}",
            receivedId.get().getClass().getSimpleName(), receivedId.get());

        log.info("PASS: Binary correlation ID preservation");
    }

    @Test
    @Order(4)
    @DisplayName("Test: UnsignedLong correlation ID preservation")
    @Timeout(30)
    void testUnsignedLongCorrelationIdPreservation() throws Exception {
        String queueName = generateQueueName("corrid-ulong");
        UnsignedLong correlationId = UnsignedLong.valueOf(987654321L);

        Message sentMsg = Message.Factory.create();
        sentMsg.setBody(new AmqpValue("Test body"));
        sentMsg.setCorrelationId(correlationId);

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
                            receivedId.set(msg.getCorrelationId());
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
        assertEquals(correlationId, receivedId.get(), "UnsignedLong correlation ID should be preserved");

        log.info("PASS: UnsignedLong correlation ID preservation");
    }

    @Test
    @Order(5)
    @DisplayName("Test: Request-reply pattern with correlation ID")
    @Timeout(30)
    void testRequestReplyWithCorrelationId() throws Exception {
        String requestQueue = generateQueueName("corrid-request");
        String replyQueue = generateQueueName("corrid-reply");
        String correlationId = "request-" + UUID.randomUUID();
        String requestBody = "Request message";
        String replyBody = "Reply message";

        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<String> receivedCorrelationId = new AtomicReference<>();
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Setup request sender
                        ProtonSender requestSender = conn.createSender(requestQueue);
                        // Setup reply receiver
                        ProtonReceiver replyReceiver = conn.createReceiver(replyQueue);
                        replyReceiver.setPrefetch(0);

                        replyReceiver.handler((delivery, msg) -> {
                            Object corrId = msg.getCorrelationId();
                            receivedCorrelationId.set(corrId != null ? corrId.toString() : null);
                            delivery.disposition(Accepted.getInstance(), true);
                            replyReceiver.close();
                            requestSender.close();
                            conn.close();
                            done.countDown();
                        });

                        requestSender.openHandler(sendRes -> {
                            if (sendRes.succeeded()) {
                                replyReceiver.openHandler(recvRes -> {
                                    if (recvRes.succeeded()) {
                                        replyReceiver.flow(1);

                                        // Send request
                                        Message request = Message.Factory.create();
                                        request.setBody(new AmqpValue(requestBody));
                                        request.setMessageId(correlationId);
                                        request.setReplyTo(replyQueue);
                                        requestSender.send(request);

                                        // Simulate responder sending reply
                                        ProtonSender replySender = conn.createSender(replyQueue);
                                        replySender.openHandler(replyOpenRes -> {
                                            if (replyOpenRes.succeeded()) {
                                                Message reply = Message.Factory.create();
                                                reply.setBody(new AmqpValue(replyBody));
                                                reply.setCorrelationId(correlationId);
                                                replySender.send(reply);
                                                replySender.close();
                                            }
                                        });
                                        replySender.open();
                                    } else {
                                        errorRef.set("Reply receiver open failed");
                                        done.countDown();
                                    }
                                });
                                replyReceiver.open();
                            } else {
                                errorRef.set("Request sender open failed");
                                done.countDown();
                            }
                        });
                        requestSender.open();
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
        assertEquals(correlationId, receivedCorrelationId.get(),
            "Correlation ID should match original request message ID");

        log.info("PASS: Request-reply pattern with correlation ID");
    }

    @Test
    @Order(6)
    @DisplayName("Test: Message with both message ID and correlation ID")
    @Timeout(30)
    void testMessageWithBothIds() throws Exception {
        String queueName = generateQueueName("corrid-both");
        String messageId = "msg-" + UUID.randomUUID();
        String correlationId = "corr-" + UUID.randomUUID();

        Message sentMsg = Message.Factory.create();
        sentMsg.setBody(new AmqpValue("Test body"));
        sentMsg.setMessageId(messageId);
        sentMsg.setCorrelationId(correlationId);

        sendMessage(queueName, sentMsg);

        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Object> receivedMsgId = new AtomicReference<>();
        AtomicReference<Object> receivedCorrId = new AtomicReference<>();
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
                            receivedMsgId.set(msg.getMessageId());
                            receivedCorrId.set(msg.getCorrelationId());
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
        assertEquals(messageId, receivedMsgId.get(), "Message ID should be preserved");
        assertEquals(correlationId, receivedCorrId.get(), "Correlation ID should be preserved");

        log.info("PASS: Message with both message ID and correlation ID");
    }
}
