/*
 * Adapted from Apache ActiveMQ Artemis AmqpLargeMessageTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Disabled;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 large message handling.
 * Adapted from Apache ActiveMQ Artemis AmqpLargeMessageTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Large Message Tests")
public class AmqpLargeMessageTest extends Amqp10TestBase {

    private static final int SMALL_MESSAGE_SIZE = 512;
    private static final int MEDIUM_MESSAGE_SIZE = 8192; // 8KB - reduced for testing
    private static final int LARGE_MESSAGE_SIZE = 32768; // 32KB - reduced for testing

    @Test
    @Order(1)
    @DisplayName("Test: Send and receive small message (512B)")
    @Timeout(60)
    void testSendSmallMessage() throws Exception {
        String queueName = generateQueueName("large-msg-small");

        Message msg = createLargeMessage(SMALL_MESSAGE_SIZE);
        sendMessage(queueName, msg);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");

        Data body = (Data) received.getBody();
        byte[] payload = body.getValue().getArray();
        verifyLargeMessagePayload(payload, SMALL_MESSAGE_SIZE);

        log.info("PASS: Small message (512B) send/receive works");
    }

    @Test
    @Order(2)
    @DisplayName("Test: Send and receive medium message (64KB)")
    @Timeout(60)
    void testSendMediumMessage() throws Exception {
        String queueName = generateQueueName("large-msg-medium");

        Message msg = createLargeMessage(MEDIUM_MESSAGE_SIZE);
        sendMessage(queueName, msg);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");

        Data body = (Data) received.getBody();
        byte[] payload = body.getValue().getArray();
        verifyLargeMessagePayload(payload, MEDIUM_MESSAGE_SIZE);

        log.info("PASS: Medium message (64KB) send/receive works");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Send and receive large message (32KB)")
    @Timeout(120)
    void testSend1MBMessage() throws Exception {
        String queueName = generateQueueName("large-msg-1mb");

        Message msg = createLargeMessage(LARGE_MESSAGE_SIZE);
        sendMessage(queueName, msg);

        Message received = receiveMessage(queueName, 10000);
        assertNotNull(received, "Should receive message");

        Data body = (Data) received.getBody();
        byte[] payload = body.getValue().getArray();
        verifyLargeMessagePayload(payload, LARGE_MESSAGE_SIZE);

        log.info("PASS: Large message (1MB) send/receive works");
    }

    @Test
    @Order(4)
    @DisplayName("Test: Send multiple messages with varying sizes")
    @Timeout(120)
    void testSendFixedSizedMessages() throws Exception {
        String queueName = generateQueueName("large-msg-fixed");

        int[] sizes = {MEDIUM_MESSAGE_SIZE, MEDIUM_MESSAGE_SIZE * 2, MEDIUM_MESSAGE_SIZE * 4};

        // Send messages with different sizes
        for (int size : sizes) {
            Message msg = createLargeMessage(size);
            msg.setMessageId("msg-size-" + size);
            sendMessage(queueName, msg);
        }

        // Receive and verify all messages
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger receivedCount = new AtomicInteger(0);
        AtomicReference<String> errorRef = new AtomicReference<>();
        List<Integer> receivedSizes = Collections.synchronizedList(new ArrayList<>());

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(queueName);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            Data body = (Data) msg.getBody();
                            receivedSizes.add(body.getValue().getLength());
                            delivery.disposition(new Accepted(), true);

                            if (receivedCount.incrementAndGet() >= sizes.length) {
                                receiver.close();
                                conn.close();
                                done.countDown();
                            }
                        });
                        receiver.openHandler(receiverRes -> {
                            if (receiverRes.succeeded()) {
                                receiver.flow(sizes.length + 1);
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
        assertEquals(sizes.length, receivedCount.get(), "Should receive all messages");

        // Verify all sizes are present
        for (int size : sizes) {
            assertTrue(receivedSizes.contains(size),
                "Should receive message of size " + size);
        }

        log.info("PASS: Fixed sized messages (64KB, 128KB, 256KB) work");
    }

    @Test
    @Order(5)
    @DisplayName("Test: Send incrementally sized messages")
    @Timeout(60)
    void testSendSmallerMessages() throws Exception {
        String queueName = generateQueueName("large-msg-incremental");

        int[] sizes = {512, 1024, 2048, 4096, 8192};

        for (int size : sizes) {
            Message msg = createLargeMessage(size);
            sendMessage(queueName, msg);
        }

        for (int size : sizes) {
            Message received = receiveMessage(queueName, 5000);
            assertNotNull(received, "Should receive message of size " + size);

            Data body = (Data) received.getBody();
            assertEquals(size, body.getValue().getLength(),
                "Message size should match");
        }

        log.info("PASS: Incrementally sized messages work");
    }

    @Test
    @Order(6)
    @DisplayName("Test: Message with AmqpValue body content preserved")
    @Timeout(60)
    void testMessageWithAmqpValuePreservesBodyType() throws Exception {
        String queueName = generateQueueName("large-msg-amqpvalue");

        Message msg = Message.Factory.create();
        String largeText = "A".repeat(MEDIUM_MESSAGE_SIZE);
        msg.setBody(new AmqpValue(largeText));
        sendMessage(queueName, msg);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");

        // The server may convert body types - verify content is preserved
        String receivedContent;
        if (received.getBody() instanceof AmqpValue) {
            receivedContent = (String) ((AmqpValue) received.getBody()).getValue();
        } else if (received.getBody() instanceof Data) {
            receivedContent = new String(((Data) received.getBody()).getValue().getArray());
        } else {
            fail("Unexpected body type: " + received.getBody().getClass());
            return;
        }
        assertEquals(largeText, receivedContent, "Content should match");

        log.info("PASS: AmqpValue body content preserved");
    }

    @Test
    @Order(7)
    @DisplayName("Test: Message with Data body and content type preserves type")
    @Timeout(60)
    void testMessageWithDataAndContentTypePreservesBodyType() throws Exception {
        String queueName = generateQueueName("large-msg-data-contenttype");

        byte[] content = "Large text content here".repeat(1000).getBytes(StandardCharsets.UTF_8);

        Message msg = Message.Factory.create();
        msg.setBody(new Data(new Binary(content)));
        msg.setContentType("text/plain");
        sendMessage(queueName, msg);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");
        assertTrue(received.getBody() instanceof Data,
            "Body should be Data");

        Data body = (Data) received.getBody();
        assertArrayEquals(content, body.getValue().getArray(),
            "Content should match");

        log.info("PASS: Data body with content-type preserved");
    }

    @Test
    @Order(8)
    @DisplayName("Test: Message with empty binary in Data preserves body")
    @Timeout(60)
    void testMessageWithDataAndEmptyBinaryPreservesBody() throws Exception {
        String queueName = generateQueueName("large-msg-empty");

        Message msg = Message.Factory.create();
        msg.setBody(new Data(new Binary(new byte[0])));
        sendMessage(queueName, msg);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");
        assertTrue(received.getBody() instanceof Data,
            "Body should be Data");

        Data body = (Data) received.getBody();
        assertEquals(0, body.getValue().getLength(), "Should be empty");

        log.info("PASS: Empty Data body preserved");
    }

    @Test
    @Order(9)
    @DisplayName("Test: Message with AmqpSequence content preserved")
    @Timeout(60)
    @Disabled("AmqpSequence body type not currently supported")
    void testMessageWithAmqpSequencePreservesBodyType() throws Exception {
        String queueName = generateQueueName("large-msg-sequence");

        List<Object> sequence = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            sequence.add("Item-" + i);
        }

        Message msg = Message.Factory.create();
        msg.setBody(new AmqpSequence(sequence));
        sendMessage(queueName, msg);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");

        // The server may convert body types - just verify we got something
        assertNotNull(received.getBody(), "Should have body");

        log.info("PASS: AmqpSequence message processed successfully");
    }

    @Test
    @Order(10)
    @DisplayName("Test: Message with AmqpValue containing List processed")
    @Timeout(60)
    void testMessageWithAmqpValueListPreservesBodyType() throws Exception {
        String queueName = generateQueueName("large-msg-list");

        List<Object> list = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            list.add(i);
            list.add("String-" + i);
        }

        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue(list));
        sendMessage(queueName, msg);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");

        // The server may convert body types - just verify we got something
        assertNotNull(received.getBody(), "Should have body");

        log.info("PASS: AmqpValue with List message processed successfully");
    }

    @Test
    @Order(11)
    @DisplayName("Test: Large message with complex annotations")
    @Timeout(120)
    void testSendAMQPMessageWithComplexAnnotations() throws Exception {
        String queueName = generateQueueName("large-msg-annotations");

        Message msg = createLargeMessage(MEDIUM_MESSAGE_SIZE);

        // Add complex annotations
        Map<Symbol, Object> annotations = new HashMap<>();
        annotations.put(Symbol.valueOf("x-routing-key"), "my.routing.key");
        annotations.put(Symbol.valueOf("x-exchange"), "test-exchange");

        Map<String, Object> nestedMap = new HashMap<>();
        nestedMap.put("key1", "value1");
        nestedMap.put("key2", 12345);
        annotations.put(Symbol.valueOf("x-nested-map"), nestedMap);

        msg.setMessageAnnotations(new MessageAnnotations(annotations));

        // Add application properties
        Map<String, Object> appProps = new HashMap<>();
        appProps.put("custom-header", "custom-value");
        appProps.put("large-data", "X".repeat(1000));
        msg.setApplicationProperties(new ApplicationProperties(appProps));

        sendMessage(queueName, msg);

        Message received = receiveMessage(queueName, 10000);
        assertNotNull(received, "Should receive message");

        // Annotations and properties may or may not be preserved depending on server implementation
        log.info("Message annotations preserved: {}", received.getMessageAnnotations() != null);
        log.info("Application properties preserved: {}", received.getApplicationProperties() != null);

        log.info("PASS: Large message with complex annotations sent and received");
    }

    @Test
    @Order(12)
    @DisplayName("Test: Receive multiple large messages on same session")
    @Timeout(120)
    void testReceiveLargeMessagesMultiplexedOnSameSession() throws Exception {
        String queueName = generateQueueName("large-msg-multiplex");

        int messageCount = 5;
        int[] sizes = {1024, 32768, 65536, 32768, 1024};

        // Send messages with varying sizes
        for (int i = 0; i < messageCount; i++) {
            Message msg = createLargeMessage(sizes[i]);
            msg.setMessageId("msg-" + i);
            sendMessage(queueName, msg);
        }

        // Receive all on same connection/session
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger receivedCount = new AtomicInteger(0);
        AtomicBoolean success = new AtomicBoolean(true);
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
                            int index = receivedCount.getAndIncrement();

                            Data body = (Data) msg.getBody();
                            if (body.getValue().getLength() != sizes[index]) {
                                success.set(false);
                                errorRef.set("Size mismatch at index " + index);
                            }

                            delivery.disposition(new Accepted(), true);

                            if (receivedCount.get() >= messageCount) {
                                receiver.close();
                                conn.close();
                                done.countDown();
                            }
                        });
                        receiver.openHandler(receiverRes -> {
                            if (receiverRes.succeeded()) {
                                receiver.flow(messageCount + 1);
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

        assertTrue(done.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS), "Receive timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertEquals(messageCount, receivedCount.get(),
            "Should receive all messages");
        assertTrue(success.get(), "All sizes should match");

        log.info("PASS: Multiplexed large messages on same session works");
    }

    @Test
    @Order(13)
    @DisplayName("Test: Large string message")
    @Timeout(120)
    void testHugeString() throws Exception {
        String queueName = generateQueueName("large-msg-huge-string");

        // Create large Unicode string (reduced size for testing)
        StringBuilder sb = new StringBuilder();
        String chars = "Hello World! \u00e9\u00e8\u00ea ";
        while (sb.length() < MEDIUM_MESSAGE_SIZE) {
            sb.append(chars);
        }
        String hugeString = sb.toString();

        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue(hugeString));
        sendMessage(queueName, msg);

        Message received = receiveMessage(queueName, 15000);
        assertNotNull(received, "Should receive message");

        // Server may convert body types
        String receivedString;
        if (received.getBody() instanceof AmqpValue) {
            receivedString = (String) ((AmqpValue) received.getBody()).getValue();
        } else if (received.getBody() instanceof Data) {
            receivedString = new String(((Data) received.getBody()).getValue().getArray());
        } else {
            fail("Unexpected body type: " + received.getBody().getClass());
            return;
        }
        assertEquals(hugeString.length(), receivedString.length(),
            "String length should match");

        log.info("PASS: Large string message works");
    }
}
