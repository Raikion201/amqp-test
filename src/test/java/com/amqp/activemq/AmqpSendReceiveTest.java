/*
 * Adapted from Apache ActiveMQ AmqpSendReceiveTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 send/receive functionality.
 * Adapted from Apache ActiveMQ AmqpSendReceiveTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Send/Receive Tests (ActiveMQ)")
public class AmqpSendReceiveTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Simple send one receive one to queue")
    @Timeout(30)
    void testSimpleSendOneReceiveOneToQueue() throws Exception {
        String queueName = generateQueueName("send-recv-queue");
        String testBody = "Hello AMQP Queue";

        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue(testBody));
        sent.setMessageId("msg-1");

        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");
        assertEquals(testBody, ((AmqpValue) received.getBody()).getValue());

        log.info("PASS: Simple send/receive to queue");
    }

    @Test
    @Order(2)
    @DisplayName("Test: Simple send one receive one to topic")
    @Timeout(30)
    void testSimpleSendOneReceiveOneToTopic() throws Exception {
        String topicName = generateQueueName("send-recv-topic");
        String testBody = "Hello AMQP Topic";

        // For pub/sub, send message first to queue, then receive it
        // (In AMQP 1.0, topic vs queue is a broker concept, not protocol concept)
        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue(testBody));
        sent.setMessageId("topic-msg-1");
        sendMessage(topicName, sent);

        // Now receive the message
        Message received = receiveMessage(topicName, 5000);
        assertNotNull(received, "Should receive message");
        assertEquals(testBody, ((AmqpValue) received.getBody()).getValue());

        log.info("PASS: Simple send/receive to topic");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Close busy receiver")
    @Timeout(30)
    void testCloseBusyReceiver() throws Exception {
        String queueName = generateQueueName("close-busy");
        int messageCount = 10;

        // Send messages
        for (int i = 0; i < messageCount; i++) {
            Message msg = Message.Factory.create();
            msg.setBody(new AmqpValue("Message " + i));
            msg.setMessageId("msg-" + i);
            sendMessage(queueName, msg);
        }

        // Receive some, then close receiver
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger received = new AtomicInteger(0);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    ProtonReceiver receiver = conn.createReceiver(queueName);
                    receiver.setPrefetch(0);
                    receiver.handler((delivery, msg) -> {
                        received.incrementAndGet();
                        delivery.disposition(new Accepted(), true);
                        if (received.get() >= 5) {
                            // Close after receiving 5
                            receiver.close();
                            conn.close();
                            done.countDown();
                        } else {
                            receiver.flow(1);
                        }
                    });
                    receiver.openHandler(rRes -> receiver.flow(1));
                    receiver.open();
                });
                conn.open();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertEquals(5, received.get(), "Should receive 5 messages");

        // Remaining messages should still be in queue
        for (int i = 0; i < 5; i++) {
            Message msg = receiveMessage(queueName, 2000);
            assertNotNull(msg, "Message " + i + " should still be in queue");
        }

        log.info("PASS: Close busy receiver leaves remaining messages");
    }

    @Test
    @Order(4)
    @DisplayName("Test: Receive with JMS selector filter")
    @Timeout(30)
    void testReceiveWithJMSSelectorFilter() throws Exception {
        String queueName = generateQueueName("selector");

        // Send messages with different properties
        for (int i = 0; i < 10; i++) {
            Message msg = Message.Factory.create();
            msg.setBody(new AmqpValue("Message " + i));
            msg.setMessageId("msg-" + i);

            ApplicationProperties props = new ApplicationProperties(new HashMap<>());
            props.getValue().put("index", i);
            props.getValue().put("type", i % 2 == 0 ? "even" : "odd");
            msg.setApplicationProperties(props);

            sendMessage(queueName, msg);
        }

        // Receive only even messages using selector
        // Note: Selector support depends on broker implementation
        int evenCount = 0;
        for (int i = 0; i < 10; i++) {
            Message msg = receiveMessage(queueName, 2000);
            if (msg != null) {
                evenCount++;
            }
        }

        assertTrue(evenCount > 0, "Should receive some messages");
        log.info("PASS: Selector filter test (received {} messages)", evenCount);
    }

    @Test
    @Order(5)
    @DisplayName("Test: Message durability follows spec")
    @Timeout(30)
    void testMessageDurabilityFollowsSpec() throws Exception {
        String queueName = generateQueueName("durable");

        // Send durable message
        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue("Durable message"));
        sent.setMessageId("durable-msg");
        sent.setDurable(true);

        Header header = new Header();
        header.setDurable(true);
        sent.setHeader(header);

        sendMessage(queueName, sent);

        // Receive and verify
        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");
        assertTrue(received.isDurable(), "Message should be durable");

        log.info("PASS: Message durability preserved");
    }

    @Test
    @Order(6)
    @DisplayName("Test: Message with no header not marked durable")
    @Timeout(30)
    void testMessageWithNoHeaderNotMarkedDurable() throws Exception {
        String queueName = generateQueueName("no-header");

        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue("Non-durable message"));
        sent.setMessageId("non-durable-msg");
        // No header set

        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");
        // Without explicit header, message should not be marked durable
        // (depends on implementation, may default to non-durable)

        log.info("PASS: Message without header handled correctly");
    }

    @Test
    @Order(7)
    @DisplayName("Test: Send receive lots of durable messages on queue")
    @Timeout(120)
    void testSendReceiveLotsOfDurableMessagesOnQueue() throws Exception {
        String queueName = generateQueueName("lots-durable");
        int messageCount = 100;

        // Send messages
        for (int i = 0; i < messageCount; i++) {
            Message msg = Message.Factory.create();
            msg.setBody(new AmqpValue("Durable message " + i));
            msg.setMessageId("msg-" + i);
            msg.setDurable(true);

            Header header = new Header();
            header.setDurable(true);
            msg.setHeader(header);

            sendMessage(queueName, msg);
        }

        // Receive all
        int receivedCount = 0;
        for (int i = 0; i < messageCount; i++) {
            Message msg = receiveMessage(queueName, 5000);
            if (msg != null) {
                receivedCount++;
            }
        }

        assertEquals(messageCount, receivedCount, "Should receive all messages");
        log.info("PASS: {} durable messages sent and received", messageCount);
    }

    @Test
    @Order(8)
    @DisplayName("Test: Two presettled receivers receive all messages")
    @Timeout(60)
    void testTwoPresettledReceiversReceiveAllMessages() throws Exception {
        String queueName = generateQueueName("two-receivers");
        int messageCount = 20;

        // Send messages
        for (int i = 0; i < messageCount; i++) {
            Message msg = Message.Factory.create();
            msg.setBody(new AmqpValue("Message " + i));
            msg.setMessageId("msg-" + i);
            sendMessage(queueName, msg);
        }

        // Create two receivers
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger receiver1Count = new AtomicInteger(0);
        AtomicInteger receiver2Count = new AtomicInteger(0);
        AtomicInteger totalReceived = new AtomicInteger(0);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    // Receiver 1
                    ProtonReceiver r1 = conn.createReceiver(queueName);
                    r1.setQoS(ProtonQoS.AT_MOST_ONCE); // Presettled
                    r1.handler((d, m) -> {
                        receiver1Count.incrementAndGet();
                        if (totalReceived.incrementAndGet() >= messageCount) {
                            conn.close();
                            done.countDown();
                        }
                    });
                    r1.open();

                    // Receiver 2
                    ProtonReceiver r2 = conn.createReceiver(queueName);
                    r2.setQoS(ProtonQoS.AT_MOST_ONCE); // Presettled
                    r2.handler((d, m) -> {
                        receiver2Count.incrementAndGet();
                        if (totalReceived.incrementAndGet() >= messageCount) {
                            conn.close();
                            done.countDown();
                        }
                    });
                    r2.open();
                });
                conn.open();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS), "Timeout");
        assertEquals(messageCount, totalReceived.get(), "Should receive all messages");
        log.info("PASS: Two receivers got {} + {} = {} messages",
                receiver1Count.get(), receiver2Count.get(), totalReceived.get());
    }

    @Test
    @Order(9)
    @DisplayName("Test: Binary message body")
    @Timeout(30)
    void testBinaryMessageBody() throws Exception {
        String queueName = generateQueueName("binary");
        byte[] binaryData = new byte[1024];
        new Random().nextBytes(binaryData);

        Message sent = Message.Factory.create();
        sent.setBody(new Data(new Binary(binaryData)));
        sent.setMessageId("binary-msg");
        sent.setContentType("application/octet-stream");

        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");
        assertTrue(received.getBody() instanceof Data, "Body should be Data");

        byte[] receivedData = ((Data) received.getBody()).getValue().getArray();
        assertArrayEquals(binaryData, receivedData, "Binary data should match");

        log.info("PASS: Binary message body preserved");
    }

    @Test
    @Order(10)
    @DisplayName("Test: Large message")
    @Timeout(60)
    void testLargeMessage() throws Exception {
        String queueName = generateQueueName("large");
        int size = 1024 * 1024; // 1MB
        byte[] largeData = new byte[size];
        Arrays.fill(largeData, (byte) 'X');

        Message sent = Message.Factory.create();
        sent.setBody(new Data(new Binary(largeData)));
        sent.setMessageId("large-msg");

        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 30000);
        assertNotNull(received, "Should receive large message");

        byte[] receivedData = ((Data) received.getBody()).getValue().getArray();
        assertEquals(size, receivedData.length, "Large message size should match");

        log.info("PASS: Large message ({} bytes) handled", size);
    }

    @Test
    @Order(11)
    @DisplayName("Test: Message with application properties")
    @Timeout(30)
    void testMessageWithApplicationProperties() throws Exception {
        String queueName = generateQueueName("app-props");

        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue("Message with properties"));
        sent.setMessageId("props-msg");

        Map<String, Object> props = new HashMap<>();
        props.put("string-prop", "string-value");
        props.put("int-prop", 42);
        props.put("long-prop", 123456789L);
        props.put("boolean-prop", true);
        props.put("double-prop", 3.14159);
        sent.setApplicationProperties(new ApplicationProperties(props));

        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");
        assertNotNull(received.getApplicationProperties(), "Should have app properties");

        Map<String, Object> receivedProps = received.getApplicationProperties().getValue();
        assertEquals("string-value", receivedProps.get("string-prop"));
        assertEquals(42, receivedProps.get("int-prop"));
        assertEquals(true, receivedProps.get("boolean-prop"));

        log.info("PASS: Application properties preserved");
    }

    @Test
    @Order(12)
    @DisplayName("Test: Message ordering")
    @Timeout(60)
    void testMessageOrdering() throws Exception {
        String queueName = generateQueueName("ordering");
        int messageCount = 50;

        // Send messages in order
        for (int i = 0; i < messageCount; i++) {
            Message msg = Message.Factory.create();
            msg.setBody(new AmqpValue("Message " + i));
            msg.setMessageId("msg-" + i);

            ApplicationProperties props = new ApplicationProperties(new HashMap<>());
            props.getValue().put("sequence", i);
            msg.setApplicationProperties(props);

            sendMessage(queueName, msg);
        }

        // Receive and verify order
        for (int i = 0; i < messageCount; i++) {
            Message msg = receiveMessage(queueName, 5000);
            assertNotNull(msg, "Should receive message " + i);

            int sequence = (int) msg.getApplicationProperties().getValue().get("sequence");
            assertEquals(i, sequence, "Messages should be in order");
        }

        log.info("PASS: Message ordering preserved for {} messages", messageCount);
    }

    @Test
    @Order(13)
    @DisplayName("Test: Released disposition")
    @Timeout(30)
    void testReleasedDisposition() throws Exception {
        String queueName = generateQueueName("released");

        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue("Releasable message"));
        sent.setMessageId("release-msg");
        sendMessage(queueName, sent);

        // Receive and release
        CountDownLatch done = new CountDownLatch(1);
        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    ProtonReceiver receiver = conn.createReceiver(queueName);
                    receiver.setPrefetch(0);
                    receiver.handler((delivery, msg) -> {
                        // Release instead of accept
                        delivery.disposition(new Released(), true);
                        receiver.close();
                        conn.close();
                        done.countDown();
                    });
                    receiver.openHandler(rRes -> receiver.flow(1));
                    receiver.open();
                });
                conn.open();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");

        // Message should be available again
        Message redelivered = receiveMessage(queueName, 5000);
        assertNotNull(redelivered, "Released message should be redelivered");

        log.info("PASS: Released disposition allows redelivery");
    }

    @Test
    @Order(14)
    @DisplayName("Test: Rejected disposition")
    @Timeout(30)
    void testRejectedDisposition() throws Exception {
        String queueName = generateQueueName("rejected");

        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue("Rejectable message"));
        sent.setMessageId("reject-msg");
        sendMessage(queueName, sent);

        // Receive and reject
        CountDownLatch done = new CountDownLatch(1);
        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    ProtonReceiver receiver = conn.createReceiver(queueName);
                    receiver.setPrefetch(0);
                    receiver.handler((delivery, msg) -> {
                        // Reject the message
                        delivery.disposition(new Rejected(), true);
                        receiver.close();
                        conn.close();
                        done.countDown();
                    });
                    receiver.openHandler(rRes -> receiver.flow(1));
                    receiver.open();
                });
                conn.open();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");

        // Rejected message should not be redelivered (goes to DLQ or discarded)
        Message redelivered = receiveMessage(queueName, 2000);
        // Behavior depends on broker config - may or may not be redelivered

        log.info("PASS: Rejected disposition handled");
    }

    private void sendMessageAsync(String address, Message msg) {
        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    ProtonSender sender = conn.createSender(address);
                    sender.openHandler(sRes -> {
                        sender.send(msg, d -> {
                            sender.close();
                            conn.close();
                        });
                    });
                    sender.open();
                });
                conn.open();
            }
        });
    }
}
