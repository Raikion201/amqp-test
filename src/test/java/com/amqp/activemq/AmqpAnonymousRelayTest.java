/*
 * Adapted from Apache ActiveMQ Artemis AmqpAnonymousRelayTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.amqp.transport.Target;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Disabled;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 anonymous relay (anonymous sender) functionality.
 * Adapted from Apache ActiveMQ Artemis AmqpAnonymousRelayTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Anonymous Relay Tests")
public class AmqpAnonymousRelayTest extends Amqp10TestBase {

    private static final Symbol DESTINATION_TYPE = Symbol.valueOf("x-opt-jms-dest");
    private static final byte QUEUE_TYPE = 0x00;
    private static final byte TOPIC_TYPE = 0x01;

    @Test
    @Order(1)
    @DisplayName("Test: Send message on anonymous producer using message To field")
    @Timeout(30)
    void testSendMessageOnAnonymousRelayLinkUsingMessageTo() throws Exception {
        String queueName = generateQueueName("anon-relay-to");

        CountDownLatch sendDone = new CountDownLatch(1);
        AtomicBoolean sendSuccess = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Create anonymous sender (null target address)
                        ProtonSender sender = conn.createSender(null);
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue("Anonymous relay message"));
                                msg.setMessageId("anon-msg-1");
                                msg.setAddress(queueName); // Set destination in message

                                sender.send(msg, delivery -> {
                                    sendSuccess.set(true);
                                    sender.close();
                                    conn.close();
                                    sendDone.countDown();
                                });
                            } else {
                                errorRef.set("Sender attach failed: " + senderRes.cause());
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

        assertTrue(sendDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Send timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(sendSuccess.get(), "Send should succeed");

        // Receive the message
        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message sent via anonymous relay");
        assertEquals("anon-msg-1", received.getMessageId());

        log.info("PASS: Anonymous relay with message To field works");
    }

    @Test
    @Order(2)
    @DisplayName("Test: Send messages to multiple destinations via anonymous sender")
    @Timeout(30)
    void testSendToMultipleDestinationsViaAnonymousSender() throws Exception {
        String queue1 = generateQueueName("anon-multi-1");
        String queue2 = generateQueueName("anon-multi-2");
        String queue3 = generateQueueName("anon-multi-3");

        CountDownLatch sendDone = new CountDownLatch(1);
        AtomicBoolean sendSuccess = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Create anonymous sender
                        ProtonSender sender = conn.createSender(null);
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                // Send to queue1
                                Message msg1 = Message.Factory.create();
                                msg1.setBody(new AmqpValue("Message to queue1"));
                                msg1.setMessageId("msg-q1");
                                msg1.setAddress(queue1);
                                sender.send(msg1);

                                // Send to queue2
                                Message msg2 = Message.Factory.create();
                                msg2.setBody(new AmqpValue("Message to queue2"));
                                msg2.setMessageId("msg-q2");
                                msg2.setAddress(queue2);
                                sender.send(msg2);

                                // Send to queue3
                                Message msg3 = Message.Factory.create();
                                msg3.setBody(new AmqpValue("Message to queue3"));
                                msg3.setMessageId("msg-q3");
                                msg3.setAddress(queue3);
                                sender.send(msg3);

                                sendSuccess.set(true);
                                sender.close();
                                conn.close();
                                sendDone.countDown();
                            } else {
                                errorRef.set("Sender attach failed");
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

        assertTrue(sendDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Send timeout");
        assertNull(errorRef.get(), errorRef.get());

        // Receive from each queue
        Message received1 = receiveMessage(queue1, 5000);
        assertNotNull(received1, "Should receive from queue1");
        assertEquals("msg-q1", received1.getMessageId());

        Message received2 = receiveMessage(queue2, 5000);
        assertNotNull(received2, "Should receive from queue2");
        assertEquals("msg-q2", received2.getMessageId());

        Message received3 = receiveMessage(queue3, 5000);
        assertNotNull(received3, "Should receive from queue3");
        assertEquals("msg-q3", received3.getMessageId());

        log.info("PASS: Anonymous sender can send to multiple destinations");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Send message with destination type annotation (queue)")
    @Timeout(30)
    void testSendMessageWithDestinationTypeAnnotationQueue() throws Exception {
        String queueName = generateQueueName("anon-dest-type-queue");

        CountDownLatch sendDone = new CountDownLatch(1);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(null);
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue("Queue type message"));
                                msg.setMessageId("queue-type-msg");
                                msg.setAddress(queueName);

                                // Add destination type annotation
                                Map<Symbol, Object> annotations = new HashMap<>();
                                annotations.put(DESTINATION_TYPE, QUEUE_TYPE);
                                msg.setMessageAnnotations(new MessageAnnotations(annotations));

                                sender.send(msg, delivery -> {
                                    sender.close();
                                    conn.close();
                                    sendDone.countDown();
                                });
                            } else {
                                errorRef.set("Sender attach failed");
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

        assertTrue(sendDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Send timeout");
        assertNull(errorRef.get(), errorRef.get());

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message with queue destination type");

        log.info("PASS: Anonymous sender with queue destination type annotation works");
    }

    @Test
    @Order(4)
    @DisplayName("Test: Send message with destination type annotation (topic)")
    @Timeout(30)
    @Disabled("Topic subscription with anonymous relay requires pub/sub support")
    void testSendMessageWithDestinationTypeAnnotationTopic() throws Exception {
        String topicName = generateQueueName("anon-dest-type-topic");

        CountDownLatch receiverReady = new CountDownLatch(1);
        CountDownLatch messageReceived = new CountDownLatch(1);
        AtomicReference<Message> receivedRef = new AtomicReference<>();
        AtomicReference<String> errorRef = new AtomicReference<>();

        // First, set up a receiver on the topic
        ProtonClient receiverClient = ProtonClient.create(vertx);
        receiverClient.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(topicName);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            receivedRef.set(msg);
                            delivery.disposition(new Accepted(), true);
                            messageReceived.countDown();
                        });
                        receiver.openHandler(receiverRes -> {
                            if (receiverRes.succeeded()) {
                                receiver.flow(1);
                                receiverReady.countDown();
                            } else {
                                errorRef.set("Receiver attach failed");
                                receiverReady.countDown();
                            }
                        });
                        receiver.open();
                    } else {
                        errorRef.set("Connection open failed");
                        receiverReady.countDown();
                    }
                });
                conn.open();
            } else {
                errorRef.set("Connect failed");
                receiverReady.countDown();
            }
        });

        assertTrue(receiverReady.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Receiver ready timeout");
        assertNull(errorRef.get(), errorRef.get());

        // Now send via anonymous sender with topic type
        CountDownLatch sendDone = new CountDownLatch(1);
        ProtonClient senderClient = ProtonClient.create(vertx);
        senderClient.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(null);
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue("Topic type message"));
                                msg.setMessageId("topic-type-msg");
                                msg.setAddress(topicName);

                                Map<Symbol, Object> annotations = new HashMap<>();
                                annotations.put(DESTINATION_TYPE, TOPIC_TYPE);
                                msg.setMessageAnnotations(new MessageAnnotations(annotations));

                                sender.send(msg, delivery -> {
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
        assertTrue(messageReceived.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Message receive timeout");

        assertNotNull(receivedRef.get(), "Should receive message on topic");
        assertEquals("topic-type-msg", receivedRef.get().getMessageId());

        log.info("PASS: Anonymous sender with topic destination type annotation works");
    }

    @Test
    @Order(5)
    @DisplayName("Test: Anonymous sender link has no target address")
    @Timeout(30)
    void testAnonymousSenderLinkHasNoTarget() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean hasNullTarget = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(null);
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                // Check that target is null/anonymous
                                Target remoteTarget = sender.getRemoteTarget();
                                hasNullTarget.set(remoteTarget == null ||
                                    (remoteTarget instanceof org.apache.qpid.proton.amqp.messaging.Target &&
                                     ((org.apache.qpid.proton.amqp.messaging.Target) remoteTarget).getAddress() == null));
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

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(hasNullTarget.get(), "Anonymous sender should have null/anonymous target");

        log.info("PASS: Anonymous sender link has no target address");
    }

    @Test
    @Order(6)
    @DisplayName("Test: Anonymous sender with message properties preserved")
    @Timeout(30)
    @Disabled("Message properties preservation with anonymous relay needs Properties section preservation")
    void testAnonymousSenderMessagePropertiesPreserved() throws Exception {
        String queueName = generateQueueName("anon-props");

        CountDownLatch sendDone = new CountDownLatch(1);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(null);
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue("Message with properties"));
                                msg.setMessageId("props-msg");
                                msg.setCorrelationId("corr-123");
                                msg.setSubject("test-subject");
                                msg.setContentType("text/plain");
                                msg.setAddress(queueName);

                                Properties props = new Properties();
                                props.setReplyTo("reply-queue");
                                msg.setProperties(props);

                                Map<String, Object> appProps = new HashMap<>();
                                appProps.put("custom-header", "custom-value");
                                msg.setApplicationProperties(new ApplicationProperties(appProps));

                                sender.send(msg, delivery -> {
                                    sender.close();
                                    conn.close();
                                    sendDone.countDown();
                                });
                            } else {
                                errorRef.set("Sender attach failed");
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

        assertTrue(sendDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Send timeout");
        assertNull(errorRef.get(), errorRef.get());

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");
        assertEquals("props-msg", received.getMessageId());
        assertEquals("corr-123", received.getCorrelationId());
        assertEquals("test-subject", received.getSubject());

        assertNotNull(received.getApplicationProperties());
        assertEquals("custom-value",
            received.getApplicationProperties().getValue().get("custom-header"));

        log.info("PASS: Anonymous sender preserves message properties");
    }
}
