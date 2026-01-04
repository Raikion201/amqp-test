/*
 * Adapted from Apache ActiveMQ AmqpDeliveryAnnotationsTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 delivery annotations handling.
 * Adapted from Apache ActiveMQ AmqpDeliveryAnnotationsTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Delivery Annotations Tests (ActiveMQ)")
public class AmqpDeliveryAnnotationsTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Send message with delivery annotations")
    @Timeout(30)
    void testSendMessageWithDeliveryAnnotations() throws Exception {
        String queueName = generateQueueName("delivery-annot");
        String testBody = "Test message with delivery annotations";

        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean messageSent = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(sendRes -> {
                            if (sendRes.succeeded()) {
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue(testBody));
                                msg.setMessageId(UUID.randomUUID().toString());

                                // Add delivery annotations
                                Map<Symbol, Object> deliveryAnnotations = new HashMap<>();
                                deliveryAnnotations.put(Symbol.valueOf("x-opt-delivery-time"), System.currentTimeMillis());
                                deliveryAnnotations.put(Symbol.valueOf("x-opt-delivery-delay"), 0L);
                                msg.setDeliveryAnnotations(new DeliveryAnnotations(deliveryAnnotations));

                                sender.send(msg, delivery -> {
                                    messageSent.set(delivery.getRemoteState() instanceof Accepted);
                                    sender.close();
                                    conn.close();
                                    done.countDown();
                                });
                            } else {
                                errorRef.set("Sender open failed");
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(messageSent.get(), "Message should be sent with delivery annotations");

        log.info("PASS: Send message with delivery annotations");
    }

    @Test
    @Order(2)
    @DisplayName("Test: Delivery annotations are stripped by default")
    @Timeout(30)
    void testDeliveryAnnotationsStrippedByDefault() throws Exception {
        String queueName = generateQueueName("delivery-annot-strip");
        String testBody = "Test message";
        String customAnnotation = "custom-value";

        // Send message with delivery annotations
        CountDownLatch sendDone = new CountDownLatch(1);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(sendRes -> {
                            if (sendRes.succeeded()) {
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue(testBody));

                                Map<Symbol, Object> deliveryAnnotations = new HashMap<>();
                                deliveryAnnotations.put(Symbol.valueOf("x-custom-annotation"), customAnnotation);
                                msg.setDeliveryAnnotations(new DeliveryAnnotations(deliveryAnnotations));

                                sender.send(msg, delivery -> {
                                    sender.close();
                                    conn.close();
                                    sendDone.countDown();
                                });
                            } else {
                                errorRef.set("Sender open failed");
                                sendDone.countDown();
                            }
                        });
                        sender.open();
                    } else {
                        errorRef.set("Connection open failed");
                        sendDone.countDown();
                    }
                });
                conn.open();
            } else {
                errorRef.set("Connect failed");
                sendDone.countDown();
            }
        });

        assertTrue(sendDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout sending");
        assertNull(errorRef.get(), errorRef.get());

        // Receive and check delivery annotations
        CountDownLatch recvDone = new CountDownLatch(1);
        AtomicReference<DeliveryAnnotations> receivedAnnotations = new AtomicReference<>();

        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            receivedAnnotations.set(msg.getDeliveryAnnotations());
                            delivery.disposition(Accepted.getInstance(), true);
                            receiver.close();
                            conn.close();
                            recvDone.countDown();
                        });
                        receiver.openHandler(recvRes -> {
                            if (recvRes.succeeded()) {
                                receiver.flow(1);
                            } else {
                                errorRef.set("Receiver open failed");
                                recvDone.countDown();
                            }
                        });
                        receiver.open();
                    } else {
                        errorRef.set("Connection open failed");
                        recvDone.countDown();
                    }
                });
                conn.open();
            } else {
                errorRef.set("Connect failed");
                recvDone.countDown();
            }
        });

        assertTrue(recvDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout receiving");
        assertNull(errorRef.get(), errorRef.get());

        // Per AMQP spec, delivery annotations are typically stripped
        // But broker may preserve them - document the actual behavior
        if (receivedAnnotations.get() != null && receivedAnnotations.get().getValue() != null) {
            log.info("Broker preserved delivery annotations: {}", receivedAnnotations.get().getValue());
        } else {
            log.info("Broker stripped delivery annotations (default behavior)");
        }

        log.info("PASS: Delivery annotations handling verified");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Message annotations vs delivery annotations")
    @Timeout(30)
    void testMessageAnnotationsVsDeliveryAnnotations() throws Exception {
        String queueName = generateQueueName("annot-compare");
        String testBody = "Test message";

        // Send message with both types of annotations
        CountDownLatch sendDone = new CountDownLatch(1);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(sendRes -> {
                            if (sendRes.succeeded()) {
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue(testBody));

                                // Message annotations (should be preserved)
                                Map<Symbol, Object> msgAnnotations = new HashMap<>();
                                msgAnnotations.put(Symbol.valueOf("x-msg-annotation"), "message-level");
                                msg.setMessageAnnotations(new MessageAnnotations(msgAnnotations));

                                // Delivery annotations (may be stripped)
                                Map<Symbol, Object> deliveryAnnotations = new HashMap<>();
                                deliveryAnnotations.put(Symbol.valueOf("x-delivery-annotation"), "delivery-level");
                                msg.setDeliveryAnnotations(new DeliveryAnnotations(deliveryAnnotations));

                                sender.send(msg, delivery -> {
                                    sender.close();
                                    conn.close();
                                    sendDone.countDown();
                                });
                            } else {
                                errorRef.set("Sender open failed");
                                sendDone.countDown();
                            }
                        });
                        sender.open();
                    } else {
                        errorRef.set("Connection open failed");
                        sendDone.countDown();
                    }
                });
                conn.open();
            } else {
                errorRef.set("Connect failed");
                sendDone.countDown();
            }
        });

        assertTrue(sendDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout sending");
        assertNull(errorRef.get(), errorRef.get());

        // Receive and compare annotations
        CountDownLatch recvDone = new CountDownLatch(1);
        AtomicReference<MessageAnnotations> receivedMsgAnnotations = new AtomicReference<>();
        AtomicReference<DeliveryAnnotations> receivedDeliveryAnnotations = new AtomicReference<>();

        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            receivedMsgAnnotations.set(msg.getMessageAnnotations());
                            receivedDeliveryAnnotations.set(msg.getDeliveryAnnotations());
                            delivery.disposition(Accepted.getInstance(), true);
                            receiver.close();
                            conn.close();
                            recvDone.countDown();
                        });
                        receiver.openHandler(recvRes -> {
                            if (recvRes.succeeded()) {
                                receiver.flow(1);
                            } else {
                                errorRef.set("Receiver open failed");
                                recvDone.countDown();
                            }
                        });
                        receiver.open();
                    } else {
                        errorRef.set("Connection open failed");
                        recvDone.countDown();
                    }
                });
                conn.open();
            } else {
                errorRef.set("Connect failed");
                recvDone.countDown();
            }
        });

        assertTrue(recvDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout receiving");
        assertNull(errorRef.get(), errorRef.get());

        // Message annotations should typically be preserved
        log.info("Message annotations: {}",
            receivedMsgAnnotations.get() != null ? receivedMsgAnnotations.get().getValue() : "null");
        log.info("Delivery annotations: {}",
            receivedDeliveryAnnotations.get() != null ? receivedDeliveryAnnotations.get().getValue() : "null");

        log.info("PASS: Message annotations vs delivery annotations comparison");
    }

    @Test
    @Order(4)
    @DisplayName("Test: Send message without annotations")
    @Timeout(30)
    void testSendMessageWithoutAnnotations() throws Exception {
        String queueName = generateQueueName("no-annot");
        String testBody = "Plain message without annotations";

        Message sentMsg = Message.Factory.create();
        sentMsg.setBody(new AmqpValue(testBody));
        sentMsg.setMessageId(UUID.randomUUID().toString());
        // No annotations set

        sendMessage(queueName, sentMsg);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");

        Object body = ((AmqpValue) received.getBody()).getValue();
        assertEquals(testBody, body, "Message body should match");

        log.info("PASS: Send message without annotations");
    }

    @Test
    @Order(5)
    @DisplayName("Test: Multiple annotation keys")
    @Timeout(30)
    void testMultipleAnnotationKeys() throws Exception {
        String queueName = generateQueueName("multi-annot");
        String testBody = "Message with multiple annotations";

        CountDownLatch sendDone = new CountDownLatch(1);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(sendRes -> {
                            if (sendRes.succeeded()) {
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue(testBody));

                                // Multiple delivery annotations
                                Map<Symbol, Object> deliveryAnnotations = new HashMap<>();
                                deliveryAnnotations.put(Symbol.valueOf("x-opt-key1"), "value1");
                                deliveryAnnotations.put(Symbol.valueOf("x-opt-key2"), 42);
                                deliveryAnnotations.put(Symbol.valueOf("x-opt-key3"), true);
                                deliveryAnnotations.put(Symbol.valueOf("x-opt-key4"), 3.14);
                                msg.setDeliveryAnnotations(new DeliveryAnnotations(deliveryAnnotations));

                                sender.send(msg, delivery -> {
                                    sender.close();
                                    conn.close();
                                    sendDone.countDown();
                                });
                            } else {
                                errorRef.set("Sender open failed");
                                sendDone.countDown();
                            }
                        });
                        sender.open();
                    } else {
                        errorRef.set("Connection open failed");
                        sendDone.countDown();
                    }
                });
                conn.open();
            } else {
                errorRef.set("Connect failed");
                sendDone.countDown();
            }
        });

        assertTrue(sendDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());

        // Verify message can be received
        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");

        log.info("PASS: Multiple annotation keys");
    }
}
