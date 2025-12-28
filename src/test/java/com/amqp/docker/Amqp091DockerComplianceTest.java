package com.amqp.docker;

import com.rabbitmq.client.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AMQP 0.9.1 Docker-based Compliance Tests
 *
 * Tests the AMQP server running in a Docker container using the
 * RabbitMQ Java client library.
 *
 * These tests verify compliance with AMQP 0-9-1 specification by
 * using a real client against the containerized server.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 0.9.1 Docker Compliance Tests")
@Tag("docker")
public class Amqp091DockerComplianceTest {

    private static final Logger log = LoggerFactory.getLogger(Amqp091DockerComplianceTest.class);
    private static final int TIMEOUT_SECONDS = 30;

    private static AmqpServerContainer container;

    @BeforeAll
    static void startContainer() {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                AmqpServerContainer.isDockerAvailable(),
                "Docker is not available - skipping Docker compliance tests");

        container = AmqpServerContainer.createContainer();
        container.start();
    }

    @AfterAll
    static void stopContainer() {
        if (container != null) {
            container.stop();
        }
    }

    private ConnectionFactory factory;

    @BeforeEach
    void setUp() {
        factory = new ConnectionFactory();
        factory.setHost(container.getHost());
        factory.setPort(container.getAmqp091Port());
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setConnectionTimeout(10000);
        factory.setHandshakeTimeout(10000);
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
            try (Connection connection = factory.newConnection()) {
                assertTrue(connection.isOpen());
                assertNotNull(connection.getServerProperties());
                log.info("PASS: Connection opened - server: {}",
                        connection.getServerProperties().get("product"));
            }
        }

        @Test
        @Order(2)
        @DisplayName("Connection closes gracefully")
        @Timeout(TIMEOUT_SECONDS)
        void testConnectionCloseGraceful() throws Exception {
            Connection connection = factory.newConnection();
            assertTrue(connection.isOpen());
            connection.close();
            assertFalse(connection.isOpen());
            log.info("PASS: Connection closed gracefully");
        }

        @Test
        @Order(3)
        @DisplayName("Multiple connections allowed")
        @Timeout(TIMEOUT_SECONDS)
        void testMultipleConnections() throws Exception {
            try (Connection conn1 = factory.newConnection();
                 Connection conn2 = factory.newConnection();
                 Connection conn3 = factory.newConnection()) {
                assertTrue(conn1.isOpen());
                assertTrue(conn2.isOpen());
                assertTrue(conn3.isOpen());
                log.info("PASS: Multiple connections supported");
            }
        }

        @Test
        @Order(4)
        @DisplayName("Connection with virtual host")
        @Timeout(TIMEOUT_SECONDS)
        void testConnectionVirtualHost() throws Exception {
            factory.setVirtualHost("/");
            try (Connection connection = factory.newConnection()) {
                assertTrue(connection.isOpen());
                log.info("PASS: Virtual host '/' supported");
            }
        }
    }

    // ==================== Channel Tests ====================

    @Nested
    @DisplayName("Channel Compliance")
    class ChannelCompliance {

        @Test
        @Order(1)
        @DisplayName("Channel opens successfully")
        @Timeout(TIMEOUT_SECONDS)
        void testChannelOpens() throws Exception {
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                assertTrue(channel.isOpen());
                log.info("PASS: Channel opened");
            }
        }

        @Test
        @Order(2)
        @DisplayName("Multiple channels on connection")
        @Timeout(TIMEOUT_SECONDS)
        void testMultipleChannels() throws Exception {
            try (Connection connection = factory.newConnection()) {
                Channel ch1 = connection.createChannel();
                Channel ch2 = connection.createChannel();
                Channel ch3 = connection.createChannel();

                assertTrue(ch1.isOpen());
                assertTrue(ch2.isOpen());
                assertTrue(ch3.isOpen());

                // Different channel numbers
                assertNotEquals(ch1.getChannelNumber(), ch2.getChannelNumber());
                assertNotEquals(ch2.getChannelNumber(), ch3.getChannelNumber());

                ch1.close();
                ch2.close();
                ch3.close();
                log.info("PASS: Multiple channels supported");
            }
        }

        @Test
        @Order(3)
        @DisplayName("Channel closes gracefully")
        @Timeout(TIMEOUT_SECONDS)
        void testChannelClose() throws Exception {
            try (Connection connection = factory.newConnection()) {
                Channel channel = connection.createChannel();
                assertTrue(channel.isOpen());
                channel.close();
                assertFalse(channel.isOpen());
                log.info("PASS: Channel closed gracefully");
            }
        }
    }

    // ==================== Exchange Tests ====================

    @Nested
    @DisplayName("Exchange Compliance")
    class ExchangeCompliance {

        @Test
        @Order(1)
        @DisplayName("Declare direct exchange")
        @Timeout(TIMEOUT_SECONDS)
        void testDeclareDirectExchange() throws Exception {
            String exchangeName = "test.direct." + UUID.randomUUID();
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, false, true, null);
                log.info("PASS: Direct exchange declared");
            }
        }

        @Test
        @Order(2)
        @DisplayName("Declare fanout exchange")
        @Timeout(TIMEOUT_SECONDS)
        void testDeclareFanoutExchange() throws Exception {
            String exchangeName = "test.fanout." + UUID.randomUUID();
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.exchangeDeclare(exchangeName, BuiltinExchangeType.FANOUT, false, true, null);
                log.info("PASS: Fanout exchange declared");
            }
        }

        @Test
        @Order(3)
        @DisplayName("Declare topic exchange")
        @Timeout(TIMEOUT_SECONDS)
        void testDeclareTopicExchange() throws Exception {
            String exchangeName = "test.topic." + UUID.randomUUID();
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.exchangeDeclare(exchangeName, BuiltinExchangeType.TOPIC, false, true, null);
                log.info("PASS: Topic exchange declared");
            }
        }

        @Test
        @Order(4)
        @DisplayName("Delete exchange")
        @Timeout(TIMEOUT_SECONDS)
        void testDeleteExchange() throws Exception {
            String exchangeName = "test.delete." + UUID.randomUUID();
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, false, false, null);
                channel.exchangeDelete(exchangeName);
                log.info("PASS: Exchange deleted");
            }
        }
    }

    // ==================== Queue Tests ====================

    @Nested
    @DisplayName("Queue Compliance")
    class QueueCompliance {

        @Test
        @Order(1)
        @DisplayName("Declare named queue")
        @Timeout(TIMEOUT_SECONDS)
        void testDeclareNamedQueue() throws Exception {
            String queueName = "test.queue." + UUID.randomUUID();
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                AMQP.Queue.DeclareOk result = channel.queueDeclare(queueName, false, false, true, null);
                assertEquals(queueName, result.getQueue());
                log.info("PASS: Named queue declared");
            }
        }

        @Test
        @Order(2)
        @DisplayName("Declare anonymous queue")
        @Timeout(TIMEOUT_SECONDS)
        void testDeclareAnonymousQueue() throws Exception {
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                AMQP.Queue.DeclareOk result = channel.queueDeclare();
                assertNotNull(result.getQueue());
                assertFalse(result.getQueue().isEmpty());
                log.info("PASS: Anonymous queue declared: {}", result.getQueue());
            }
        }

        @Test
        @Order(3)
        @DisplayName("Declare durable queue")
        @Timeout(TIMEOUT_SECONDS)
        void testDeclareDurableQueue() throws Exception {
            String queueName = "test.durable." + UUID.randomUUID();
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.queueDeclare(queueName, true, false, false, null);
                log.info("PASS: Durable queue declared");
                channel.queueDelete(queueName);
            }
        }

        @Test
        @Order(4)
        @DisplayName("Delete queue")
        @Timeout(TIMEOUT_SECONDS)
        void testDeleteQueue() throws Exception {
            String queueName = "test.delete." + UUID.randomUUID();
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.queueDeclare(queueName, false, false, false, null);
                AMQP.Queue.DeleteOk result = channel.queueDelete(queueName);
                assertNotNull(result);
                log.info("PASS: Queue deleted");
            }
        }

        @Test
        @Order(5)
        @DisplayName("Purge queue")
        @Timeout(TIMEOUT_SECONDS)
        void testPurgeQueue() throws Exception {
            String queueName = "test.purge." + UUID.randomUUID();
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.queueDeclare(queueName, false, false, true, null);

                // Publish some messages
                for (int i = 0; i < 5; i++) {
                    channel.basicPublish("", queueName, null, ("Message " + i).getBytes());
                }

                // Purge
                AMQP.Queue.PurgeOk result = channel.queuePurge(queueName);
                assertEquals(5, result.getMessageCount());
                log.info("PASS: Queue purged ({} messages)", result.getMessageCount());
            }
        }
    }

    // ==================== Binding Tests ====================

    @Nested
    @DisplayName("Binding Compliance")
    class BindingCompliance {

        @Test
        @Order(1)
        @DisplayName("Bind queue to direct exchange")
        @Timeout(TIMEOUT_SECONDS)
        void testBindQueueDirect() throws Exception {
            String exchange = "test.bind.direct." + UUID.randomUUID();
            String queue = "test.bind.queue." + UUID.randomUUID();
            String routingKey = "test.key";

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, false, true, null);
                channel.queueDeclare(queue, false, false, true, null);
                channel.queueBind(queue, exchange, routingKey);
                log.info("PASS: Queue bound to direct exchange");
            }
        }

        @Test
        @Order(2)
        @DisplayName("Bind queue to fanout exchange")
        @Timeout(TIMEOUT_SECONDS)
        void testBindQueueFanout() throws Exception {
            String exchange = "test.bind.fanout." + UUID.randomUUID();
            String queue = "test.bind.queue." + UUID.randomUUID();

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.exchangeDeclare(exchange, BuiltinExchangeType.FANOUT, false, true, null);
                channel.queueDeclare(queue, false, false, true, null);
                channel.queueBind(queue, exchange, "");
                log.info("PASS: Queue bound to fanout exchange");
            }
        }

        @Test
        @Order(3)
        @DisplayName("Unbind queue from exchange")
        @Timeout(TIMEOUT_SECONDS)
        void testUnbindQueue() throws Exception {
            String exchange = "test.unbind.exchange." + UUID.randomUUID();
            String queue = "test.unbind.queue." + UUID.randomUUID();
            String routingKey = "test.key";

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, false, true, null);
                channel.queueDeclare(queue, false, false, true, null);
                channel.queueBind(queue, exchange, routingKey);
                channel.queueUnbind(queue, exchange, routingKey);
                log.info("PASS: Queue unbound from exchange");
            }
        }
    }

    // ==================== Publish/Consume Tests ====================

    @Nested
    @DisplayName("Message Publish/Consume Compliance")
    class MessageCompliance {

        @Test
        @Order(1)
        @DisplayName("Basic publish and consume")
        @Timeout(TIMEOUT_SECONDS)
        void testBasicPublishConsume() throws Exception {
            String queueName = "test.pubsub." + UUID.randomUUID();
            String messageBody = "Hello AMQP!";
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<String> received = new AtomicReference<>();

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.queueDeclare(queueName, false, false, true, null);

                // Consumer
                channel.basicConsume(queueName, true, (tag, delivery) -> {
                    received.set(new String(delivery.getBody(), StandardCharsets.UTF_8));
                    latch.countDown();
                }, tag -> {});

                // Publish
                channel.basicPublish("", queueName, null, messageBody.getBytes());

                assertTrue(latch.await(10, TimeUnit.SECONDS));
                assertEquals(messageBody, received.get());
                log.info("PASS: Basic publish/consume works");
            }
        }

        @Test
        @Order(2)
        @DisplayName("Publish with properties")
        @Timeout(TIMEOUT_SECONDS)
        void testPublishWithProperties() throws Exception {
            String queueName = "test.props." + UUID.randomUUID();
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<AMQP.BasicProperties> receivedProps = new AtomicReference<>();

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.queueDeclare(queueName, false, false, true, null);

                // Consumer
                channel.basicConsume(queueName, true, (tag, delivery) -> {
                    receivedProps.set(delivery.getProperties());
                    latch.countDown();
                }, tag -> {});

                // Publish with properties
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .contentType("text/plain")
                        .correlationId("corr-123")
                        .messageId("msg-456")
                        .priority(5)
                        .build();
                channel.basicPublish("", queueName, props, "test".getBytes());

                assertTrue(latch.await(10, TimeUnit.SECONDS));
                assertEquals("text/plain", receivedProps.get().getContentType());
                assertEquals("corr-123", receivedProps.get().getCorrelationId());
                assertEquals("msg-456", receivedProps.get().getMessageId());
                assertEquals(Integer.valueOf(5), receivedProps.get().getPriority());
                log.info("PASS: Message properties preserved");
            }
        }

        @Test
        @Order(3)
        @DisplayName("Publish with headers")
        @Timeout(TIMEOUT_SECONDS)
        void testPublishWithHeaders() throws Exception {
            String queueName = "test.headers." + UUID.randomUUID();
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Map<String, Object>> receivedHeaders = new AtomicReference<>();

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.queueDeclare(queueName, false, false, true, null);

                // Consumer
                channel.basicConsume(queueName, true, (tag, delivery) -> {
                    receivedHeaders.set(delivery.getProperties().getHeaders());
                    latch.countDown();
                }, tag -> {});

                // Publish with headers
                Map<String, Object> headers = new HashMap<>();
                headers.put("x-custom-header", "custom-value");
                headers.put("x-number-header", 42);

                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .headers(headers)
                        .build();
                channel.basicPublish("", queueName, props, "test".getBytes());

                assertTrue(latch.await(10, TimeUnit.SECONDS));
                assertNotNull(receivedHeaders.get());
                // RabbitMQ client returns LongString objects for 'S' type fields, so compare string value
                assertEquals("custom-value", receivedHeaders.get().get("x-custom-header").toString());
                log.info("PASS: Message headers preserved");
            }
        }

        @Test
        @Order(4)
        @DisplayName("Multiple messages in order")
        @Timeout(TIMEOUT_SECONDS)
        void testMultipleMessagesOrder() throws Exception {
            String queueName = "test.order." + UUID.randomUUID();
            int messageCount = 10;
            CountDownLatch latch = new CountDownLatch(messageCount);
            AtomicInteger counter = new AtomicInteger(0);
            AtomicReference<String> errorRef = new AtomicReference<>();

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.queueDeclare(queueName, false, false, true, null);

                // Consumer
                channel.basicConsume(queueName, true, (tag, delivery) -> {
                    int expected = counter.getAndIncrement();
                    String body = new String(delivery.getBody(), StandardCharsets.UTF_8);
                    if (!body.equals("Message-" + expected)) {
                        errorRef.set("Out of order: expected " + expected + ", got " + body);
                    }
                    latch.countDown();
                }, tag -> {});

                // Publish messages
                for (int i = 0; i < messageCount; i++) {
                    channel.basicPublish("", queueName, null, ("Message-" + i).getBytes());
                }

                assertTrue(latch.await(30, TimeUnit.SECONDS));
                assertNull(errorRef.get(), errorRef.get());
                log.info("PASS: {} messages delivered in order", messageCount);
            }
        }

        @Test
        @Order(5)
        @DisplayName("Basic get (polling)")
        @Timeout(TIMEOUT_SECONDS)
        void testBasicGet() throws Exception {
            String queueName = "test.get." + UUID.randomUUID();

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.queueDeclare(queueName, false, false, true, null);

                // Publish
                channel.basicPublish("", queueName, null, "Get me".getBytes());

                // Poll
                GetResponse response = channel.basicGet(queueName, true);
                assertNotNull(response);
                assertEquals("Get me", new String(response.getBody(), StandardCharsets.UTF_8));
                log.info("PASS: Basic get works");
            }
        }
    }

    // ==================== Acknowledgment Tests ====================

    @Nested
    @DisplayName("Acknowledgment Compliance")
    class AckCompliance {

        @Test
        @Order(1)
        @DisplayName("Manual acknowledgment")
        @Timeout(TIMEOUT_SECONDS)
        void testManualAck() throws Exception {
            String queueName = "test.ack." + UUID.randomUUID();
            CountDownLatch latch = new CountDownLatch(1);

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.queueDeclare(queueName, false, false, true, null);
                channel.basicPublish("", queueName, null, "Ack me".getBytes());

                // Manual ack consumer
                channel.basicConsume(queueName, false, (tag, delivery) -> {
                    try {
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        latch.countDown();
                    } catch (IOException e) {
                        log.error("Ack failed", e);
                    }
                }, tag -> {});

                assertTrue(latch.await(10, TimeUnit.SECONDS));
                log.info("PASS: Manual acknowledgment works");
            }
        }

        @Test
        @Order(2)
        @DisplayName("Negative acknowledgment (nack)")
        @Timeout(TIMEOUT_SECONDS)
        void testNack() throws Exception {
            String queueName = "test.nack." + UUID.randomUUID();
            CountDownLatch latch = new CountDownLatch(2);
            AtomicInteger deliveryCount = new AtomicInteger(0);

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.queueDeclare(queueName, false, false, true, null);
                channel.basicPublish("", queueName, null, "Nack me".getBytes());

                // Nack with requeue on first delivery
                channel.basicConsume(queueName, false, (tag, delivery) -> {
                    try {
                        int count = deliveryCount.incrementAndGet();
                        if (count == 1) {
                            channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                        } else {
                            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        }
                        latch.countDown();
                    } catch (IOException e) {
                        log.error("Nack failed", e);
                    }
                }, tag -> {});

                assertTrue(latch.await(10, TimeUnit.SECONDS));
                assertEquals(2, deliveryCount.get());
                log.info("PASS: Negative acknowledgment with requeue works");
            }
        }

        @Test
        @Order(3)
        @DisplayName("Reject message")
        @Timeout(TIMEOUT_SECONDS)
        void testReject() throws Exception {
            String queueName = "test.reject." + UUID.randomUUID();
            CountDownLatch latch = new CountDownLatch(2);
            AtomicInteger deliveryCount = new AtomicInteger(0);

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.queueDeclare(queueName, false, false, true, null);
                channel.basicPublish("", queueName, null, "Reject me".getBytes());

                // Reject with requeue on first delivery
                channel.basicConsume(queueName, false, (tag, delivery) -> {
                    try {
                        int count = deliveryCount.incrementAndGet();
                        if (count == 1) {
                            channel.basicReject(delivery.getEnvelope().getDeliveryTag(), true);
                        } else {
                            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        }
                        latch.countDown();
                    } catch (IOException e) {
                        log.error("Reject failed", e);
                    }
                }, tag -> {});

                assertTrue(latch.await(10, TimeUnit.SECONDS));
                assertEquals(2, deliveryCount.get());
                log.info("PASS: Reject with requeue works");
            }
        }
    }

    // ==================== QoS Tests ====================

    @Nested
    @DisplayName("QoS Compliance")
    class QosCompliance {

        @Test
        @Order(1)
        @DisplayName("Prefetch count limits delivery")
        @Timeout(TIMEOUT_SECONDS)
        void testPrefetchCount() throws Exception {
            String queueName = "test.qos." + UUID.randomUUID();
            int messageCount = 10;
            int prefetchCount = 3;
            AtomicInteger unackedCount = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(messageCount);

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.queueDeclare(queueName, false, false, true, null);

                // Publish messages
                for (int i = 0; i < messageCount; i++) {
                    channel.basicPublish("", queueName, null, ("Message " + i).getBytes());
                }

                // Set QoS
                channel.basicQos(prefetchCount);

                // Consumer with manual ack
                channel.basicConsume(queueName, false, (tag, delivery) -> {
                    try {
                        int unacked = unackedCount.incrementAndGet();
                        // Verify prefetch is respected
                        assertTrue(unacked <= prefetchCount,
                                "Unacked count " + unacked + " exceeds prefetch " + prefetchCount);

                        // Simulate processing
                        Thread.sleep(50);

                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        unackedCount.decrementAndGet();
                        latch.countDown();
                    } catch (Exception e) {
                        log.error("Error in consumer", e);
                    }
                }, tag -> {});

                assertTrue(latch.await(30, TimeUnit.SECONDS));
                log.info("PASS: Prefetch count limits delivery");
            }
        }
    }

    // ==================== Exchange Routing Tests ====================

    @Nested
    @DisplayName("Exchange Routing Compliance")
    class RoutingCompliance {

        @Test
        @Order(1)
        @DisplayName("Direct exchange routing")
        @Timeout(TIMEOUT_SECONDS)
        void testDirectExchangeRouting() throws Exception {
            String exchange = "test.routing.direct." + UUID.randomUUID();
            String queue1 = "test.routing.q1." + UUID.randomUUID();
            String queue2 = "test.routing.q2." + UUID.randomUUID();
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<String> q1Message = new AtomicReference<>();

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, false, true, null);
                channel.queueDeclare(queue1, false, false, true, null);
                channel.queueDeclare(queue2, false, false, true, null);
                channel.queueBind(queue1, exchange, "key1");
                channel.queueBind(queue2, exchange, "key2");

                // Consumer on queue1
                channel.basicConsume(queue1, true, (tag, delivery) -> {
                    q1Message.set(new String(delivery.getBody(), StandardCharsets.UTF_8));
                    latch.countDown();
                }, tag -> {});

                // Publish to key1
                channel.basicPublish(exchange, "key1", null, "To queue1".getBytes());

                assertTrue(latch.await(10, TimeUnit.SECONDS));
                assertEquals("To queue1", q1Message.get());
                log.info("PASS: Direct exchange routing works");
            }
        }

        @Test
        @Order(2)
        @DisplayName("Fanout exchange routing")
        @Timeout(TIMEOUT_SECONDS)
        void testFanoutExchangeRouting() throws Exception {
            String exchange = "test.routing.fanout." + UUID.randomUUID();
            String queue1 = "test.fanout.q1." + UUID.randomUUID();
            String queue2 = "test.fanout.q2." + UUID.randomUUID();
            CountDownLatch latch = new CountDownLatch(2);

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.exchangeDeclare(exchange, BuiltinExchangeType.FANOUT, false, true, null);
                channel.queueDeclare(queue1, false, false, true, null);
                channel.queueDeclare(queue2, false, false, true, null);
                channel.queueBind(queue1, exchange, "");
                channel.queueBind(queue2, exchange, "");

                // Consumers
                channel.basicConsume(queue1, true, (tag, delivery) -> latch.countDown(), tag -> {});
                channel.basicConsume(queue2, true, (tag, delivery) -> latch.countDown(), tag -> {});

                // Publish (should go to both queues)
                channel.basicPublish(exchange, "", null, "Broadcast".getBytes());

                assertTrue(latch.await(10, TimeUnit.SECONDS));
                log.info("PASS: Fanout exchange broadcasts to all bound queues");
            }
        }

        @Test
        @Order(3)
        @DisplayName("Topic exchange routing")
        @Timeout(TIMEOUT_SECONDS)
        void testTopicExchangeRouting() throws Exception {
            String exchange = "test.routing.topic." + UUID.randomUUID();
            String queue1 = "test.topic.q1." + UUID.randomUUID();
            String queue2 = "test.topic.q2." + UUID.randomUUID();
            CountDownLatch latch = new CountDownLatch(2);
            AtomicInteger q1Count = new AtomicInteger(0);
            AtomicInteger q2Count = new AtomicInteger(0);

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC, false, true, null);
                channel.queueDeclare(queue1, false, false, true, null);
                channel.queueDeclare(queue2, false, false, true, null);
                channel.queueBind(queue1, exchange, "app.*.log");
                channel.queueBind(queue2, exchange, "app.error.*");

                // Consumers
                channel.basicConsume(queue1, true, (tag, delivery) -> {
                    q1Count.incrementAndGet();
                    latch.countDown();
                }, tag -> {});
                channel.basicConsume(queue2, true, (tag, delivery) -> {
                    q2Count.incrementAndGet();
                    latch.countDown();
                }, tag -> {});

                // This matches queue1 only
                channel.basicPublish(exchange, "app.info.log", null, "Info".getBytes());
                // This matches queue2 only
                channel.basicPublish(exchange, "app.error.db", null, "Error".getBytes());

                assertTrue(latch.await(10, TimeUnit.SECONDS));
                assertEquals(1, q1Count.get());
                assertEquals(1, q2Count.get());
                log.info("PASS: Topic exchange routing with wildcards works");
            }
        }
    }

    // ==================== Consumer Cancel Tests ====================

    @Nested
    @DisplayName("Consumer Lifecycle Compliance")
    class ConsumerCompliance {

        @Test
        @Order(1)
        @DisplayName("Cancel consumer")
        @Timeout(TIMEOUT_SECONDS)
        void testCancelConsumer() throws Exception {
            String queueName = "test.cancel." + UUID.randomUUID();
            CountDownLatch cancelLatch = new CountDownLatch(1);

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.queueDeclare(queueName, false, false, true, null);

                String consumerTag = channel.basicConsume(queueName, true,
                        (tag, delivery) -> {},
                        tag -> cancelLatch.countDown());

                // basicCancel() blocks until server sends Cancel-Ok
                // This verifies explicit cancellation works
                channel.basicCancel(consumerTag);
                log.info("Consumer explicitly cancelled via basicCancel");

                // Now test implicit cancellation via queue deletion
                // This triggers the CancelCallback per RabbitMQ spec
                String queueName2 = "test.cancel2." + UUID.randomUUID();
                CountDownLatch implicitCancelLatch = new CountDownLatch(1);
                channel.queueDeclare(queueName2, false, false, true, null);
                channel.basicConsume(queueName2, true,
                        (tag, delivery) -> {},
                        tag -> implicitCancelLatch.countDown());

                // Delete queue to trigger implicit cancellation
                channel.queueDelete(queueName2);
                assertTrue(implicitCancelLatch.await(10, TimeUnit.SECONDS));
                log.info("PASS: Consumer cancelled successfully (both explicit and implicit)");
            }
        }
    }
}
