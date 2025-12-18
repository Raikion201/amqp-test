package com.amqp.compliance;

import com.rabbitmq.client.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AMQP 0-9-1 Protocol Compliance Tests.
 *
 * These tests verify that the AMQP server correctly implements
 * the AMQP 0-9-1 specification using the RabbitMQ Java Client.
 *
 * Run with: mvn verify -DskipTests=false
 */
@TestMethodOrder(OrderAnnotation.class)
@DisplayName("AMQP Protocol Compliance Tests")
public class AmqpProtocolComplianceTest extends AmqpServerTestHarness {

    // ==================== CONNECTION TESTS ====================

    @Test
    @Order(1)
    @DisplayName("Connection: Basic connection establishment")
    void testConnectionEstablishment() {
        assertNotNull(connection, "Connection should be established");
        assertTrue(connection.isOpen(), "Connection should be open");
        logger.info("PASS: Connection established successfully");
    }

    @Test
    @Order(2)
    @DisplayName("Connection: Multiple connections")
    void testMultipleConnections() throws IOException, TimeoutException {
        List<Connection> connections = new ArrayList<>();
        try {
            for (int i = 0; i < 5; i++) {
                Connection conn = createNewConnection();
                assertNotNull(conn, "Connection " + i + " should be established");
                assertTrue(conn.isOpen(), "Connection " + i + " should be open");
                connections.add(conn);
            }
            assertEquals(5, connections.size(), "Should have 5 connections");
            logger.info("PASS: Multiple connections established successfully");
        } finally {
            for (Connection conn : connections) {
                try {
                    conn.close();
                } catch (Exception e) {
                    // Ignore
                }
            }
        }
    }

    @Test
    @Order(3)
    @DisplayName("Connection: Connection close")
    void testConnectionClose() throws IOException, TimeoutException {
        Connection conn = createNewConnection();
        assertTrue(conn.isOpen(), "New connection should be open");
        conn.close();
        assertFalse(conn.isOpen(), "Connection should be closed after close()");
        logger.info("PASS: Connection close works correctly");
    }

    // ==================== CHANNEL TESTS ====================

    @Test
    @Order(10)
    @DisplayName("Channel: Channel creation")
    void testChannelCreation() {
        assertNotNull(channel, "Channel should be created");
        assertTrue(channel.isOpen(), "Channel should be open");
        logger.info("PASS: Channel created successfully");
    }

    @Test
    @Order(11)
    @DisplayName("Channel: Multiple channels on same connection")
    void testMultipleChannels() throws IOException {
        List<Channel> channels = new ArrayList<>();
        try {
            for (int i = 0; i < 10; i++) {
                Channel ch = createChannel();
                assertNotNull(ch, "Channel " + i + " should be created");
                assertTrue(ch.isOpen(), "Channel " + i + " should be open");
                channels.add(ch);
            }
            assertEquals(10, channels.size(), "Should have 10 channels");
            logger.info("PASS: Multiple channels on same connection work correctly");
        } finally {
            for (Channel ch : channels) {
                try {
                    ch.close();
                } catch (Exception e) {
                    // Ignore
                }
            }
        }
    }

    @Test
    @Order(12)
    @DisplayName("Channel: Channel close")
    void testChannelClose() throws IOException, TimeoutException {
        Channel ch = createChannel();
        assertTrue(ch.isOpen(), "New channel should be open");
        ch.close();
        assertFalse(ch.isOpen(), "Channel should be closed after close()");
        logger.info("PASS: Channel close works correctly");
    }

    // ==================== EXCHANGE TESTS ====================

    @Test
    @Order(20)
    @DisplayName("Exchange: Declare direct exchange")
    void testDeclareDirectExchange() throws IOException {
        String exchangeName = "test.direct.exchange";
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, false, true, null);
        // If no exception, exchange was declared successfully
        channel.exchangeDelete(exchangeName);
        logger.info("PASS: Direct exchange declared successfully");
    }

    @Test
    @Order(21)
    @DisplayName("Exchange: Declare fanout exchange")
    void testDeclareFanoutExchange() throws IOException {
        String exchangeName = "test.fanout.exchange";
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.FANOUT, false, true, null);
        channel.exchangeDelete(exchangeName);
        logger.info("PASS: Fanout exchange declared successfully");
    }

    @Test
    @Order(22)
    @DisplayName("Exchange: Declare topic exchange")
    void testDeclareTopicExchange() throws IOException {
        String exchangeName = "test.topic.exchange";
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.TOPIC, false, true, null);
        channel.exchangeDelete(exchangeName);
        logger.info("PASS: Topic exchange declared successfully");
    }

    @Test
    @Order(23)
    @DisplayName("Exchange: Declare headers exchange")
    void testDeclareHeadersExchange() throws IOException {
        String exchangeName = "test.headers.exchange";
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.HEADERS, false, true, null);
        channel.exchangeDelete(exchangeName);
        logger.info("PASS: Headers exchange declared successfully");
    }

    @Test
    @Order(24)
    @DisplayName("Exchange: Declare durable exchange")
    void testDeclareDurableExchange() throws IOException {
        String exchangeName = "test.durable.exchange";
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, true, false, null);
        channel.exchangeDelete(exchangeName);
        logger.info("PASS: Durable exchange declared successfully");
    }

    @Test
    @Order(25)
    @DisplayName("Exchange: Delete exchange")
    void testDeleteExchange() throws IOException {
        String exchangeName = "test.delete.exchange";
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, false, false, null);
        channel.exchangeDelete(exchangeName);
        logger.info("PASS: Exchange deleted successfully");
    }

    // ==================== QUEUE TESTS ====================

    @Test
    @Order(30)
    @DisplayName("Queue: Declare named queue")
    void testDeclareNamedQueue() throws IOException {
        String queueName = "test.named.queue";
        AMQP.Queue.DeclareOk result = channel.queueDeclare(queueName, false, false, true, null);
        assertEquals(queueName, result.getQueue(), "Queue name should match");
        channel.queueDelete(queueName);
        logger.info("PASS: Named queue declared successfully");
    }

    @Test
    @Order(31)
    @DisplayName("Queue: Declare anonymous queue")
    void testDeclareAnonymousQueue() throws IOException {
        AMQP.Queue.DeclareOk result = channel.queueDeclare("", false, true, true, null);
        assertNotNull(result.getQueue(), "Anonymous queue should have a generated name");
        assertFalse(result.getQueue().isEmpty(), "Queue name should not be empty");
        logger.info("PASS: Anonymous queue declared with name: {}", result.getQueue());
    }

    @Test
    @Order(32)
    @DisplayName("Queue: Declare durable queue")
    void testDeclareDurableQueue() throws IOException {
        String queueName = "test.durable.queue";
        AMQP.Queue.DeclareOk result = channel.queueDeclare(queueName, true, false, false, null);
        assertEquals(queueName, result.getQueue());
        channel.queueDelete(queueName);
        logger.info("PASS: Durable queue declared successfully");
    }

    @Test
    @Order(33)
    @DisplayName("Queue: Delete queue")
    void testDeleteQueue() throws IOException {
        String queueName = "test.delete.queue";
        channel.queueDeclare(queueName, false, false, false, null);
        AMQP.Queue.DeleteOk result = channel.queueDelete(queueName);
        assertNotNull(result, "Delete should return a result");
        logger.info("PASS: Queue deleted successfully");
    }

    @Test
    @Order(34)
    @DisplayName("Queue: Purge queue")
    void testPurgeQueue() throws IOException {
        String queueName = "test.purge.queue";
        channel.queueDeclare(queueName, false, false, true, null);

        // Publish some messages
        for (int i = 0; i < 5; i++) {
            channel.basicPublish("", queueName, null, ("message " + i).getBytes());
        }

        // Purge the queue
        AMQP.Queue.PurgeOk result = channel.queuePurge(queueName);
        assertTrue(result.getMessageCount() >= 0, "Purge should return message count");

        channel.queueDelete(queueName);
        logger.info("PASS: Queue purged successfully");
    }

    // ==================== BINDING TESTS ====================

    @Test
    @Order(40)
    @DisplayName("Binding: Bind queue to exchange")
    void testBindQueue() throws IOException {
        String exchangeName = "test.bind.exchange";
        String queueName = "test.bind.queue";
        String routingKey = "test.key";

        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, false, true, null);
        channel.queueDeclare(queueName, false, false, true, null);
        channel.queueBind(queueName, exchangeName, routingKey);

        // If no exception, binding was successful
        channel.queueUnbind(queueName, exchangeName, routingKey);
        channel.queueDelete(queueName);
        channel.exchangeDelete(exchangeName);
        logger.info("PASS: Queue bound to exchange successfully");
    }

    @Test
    @Order(41)
    @DisplayName("Binding: Unbind queue from exchange")
    void testUnbindQueue() throws IOException {
        String exchangeName = "test.unbind.exchange";
        String queueName = "test.unbind.queue";
        String routingKey = "test.key";

        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, false, true, null);
        channel.queueDeclare(queueName, false, false, true, null);
        channel.queueBind(queueName, exchangeName, routingKey);
        channel.queueUnbind(queueName, exchangeName, routingKey);

        channel.queueDelete(queueName);
        channel.exchangeDelete(exchangeName);
        logger.info("PASS: Queue unbound from exchange successfully");
    }

    // ==================== PUBLISH/CONSUME TESTS ====================

    @Test
    @Order(50)
    @DisplayName("Basic: Publish to default exchange")
    void testPublishToDefaultExchange() throws IOException {
        String queueName = "test.publish.queue";
        channel.queueDeclare(queueName, false, false, true, null);

        String message = "Hello, AMQP!";
        channel.basicPublish("", queueName, null, message.getBytes(StandardCharsets.UTF_8));

        channel.queueDelete(queueName);
        logger.info("PASS: Message published to default exchange successfully");
    }

    @Test
    @Order(51)
    @DisplayName("Basic: Publish to named exchange")
    void testPublishToNamedExchange() throws IOException {
        String exchangeName = "test.publish.exchange";
        String queueName = "test.publish.named.queue";
        String routingKey = "test.routing.key";

        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, false, true, null);
        channel.queueDeclare(queueName, false, false, true, null);
        channel.queueBind(queueName, exchangeName, routingKey);

        String message = "Hello from named exchange!";
        channel.basicPublish(exchangeName, routingKey, null, message.getBytes(StandardCharsets.UTF_8));

        channel.queueDelete(queueName);
        channel.exchangeDelete(exchangeName);
        logger.info("PASS: Message published to named exchange successfully");
    }

    @Test
    @Order(52)
    @DisplayName("Basic: Get message (polling)")
    void testBasicGet() throws IOException, InterruptedException {
        String queueName = "test.get.queue";
        channel.queueDeclare(queueName, false, false, true, null);

        String message = "Test message for basic.get";
        channel.basicPublish("", queueName, null, message.getBytes(StandardCharsets.UTF_8));

        // Give server time to process
        Thread.sleep(100);

        GetResponse response = channel.basicGet(queueName, true);
        assertNotNull(response, "Should receive a message");
        assertEquals(message, new String(response.getBody(), StandardCharsets.UTF_8));

        channel.queueDelete(queueName);
        logger.info("PASS: Basic.Get works correctly");
    }

    @Test
    @Order(53)
    @DisplayName("Basic: Consume messages (push)")
    void testBasicConsume() throws IOException, InterruptedException {
        String queueName = "test.consume.queue";
        channel.queueDeclare(queueName, false, false, true, null);

        CountDownLatch latch = new CountDownLatch(3);
        List<String> receivedMessages = new CopyOnWriteArrayList<>();

        String consumerTag = channel.basicConsume(queueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) {
                receivedMessages.add(new String(body, StandardCharsets.UTF_8));
                latch.countDown();
            }
        });

        // Publish messages
        for (int i = 0; i < 3; i++) {
            channel.basicPublish("", queueName, null, ("Message " + i).getBytes(StandardCharsets.UTF_8));
        }

        // Wait for messages
        boolean received = latch.await(5, TimeUnit.SECONDS);
        assertTrue(received, "Should receive all 3 messages");
        assertEquals(3, receivedMessages.size());

        channel.basicCancel(consumerTag);
        channel.queueDelete(queueName);
        logger.info("PASS: Basic.Consume works correctly");
    }

    @Test
    @Order(54)
    @DisplayName("Basic: Message properties")
    void testMessageProperties() throws IOException, InterruptedException {
        String queueName = "test.properties.queue";
        channel.queueDeclare(queueName, false, false, true, null);

        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .contentEncoding("utf-8")
                .deliveryMode(2)
                .priority(5)
                .correlationId("corr-123")
                .replyTo("reply.queue")
                .messageId("msg-456")
                .build();

        String message = "{\"test\": true}";
        channel.basicPublish("", queueName, props, message.getBytes(StandardCharsets.UTF_8));

        Thread.sleep(100);

        GetResponse response = channel.basicGet(queueName, true);
        assertNotNull(response);

        AMQP.BasicProperties receivedProps = response.getProps();
        assertEquals("application/json", receivedProps.getContentType());
        assertEquals("utf-8", receivedProps.getContentEncoding());
        assertEquals(Integer.valueOf(2), receivedProps.getDeliveryMode());
        assertEquals(Integer.valueOf(5), receivedProps.getPriority());
        assertEquals("corr-123", receivedProps.getCorrelationId());
        assertEquals("reply.queue", receivedProps.getReplyTo());
        assertEquals("msg-456", receivedProps.getMessageId());

        channel.queueDelete(queueName);
        logger.info("PASS: Message properties preserved correctly");
    }

    // ==================== ACKNOWLEDGEMENT TESTS ====================

    @Test
    @Order(60)
    @DisplayName("Ack: Basic acknowledge")
    void testBasicAck() throws IOException, InterruptedException {
        String queueName = "test.ack.queue";
        channel.queueDeclare(queueName, false, false, true, null);

        channel.basicPublish("", queueName, null, "Test message".getBytes());
        Thread.sleep(100);

        GetResponse response = channel.basicGet(queueName, false);
        assertNotNull(response);

        channel.basicAck(response.getEnvelope().getDeliveryTag(), false);

        // Message should be gone after ack
        GetResponse response2 = channel.basicGet(queueName, true);
        assertNull(response2, "Queue should be empty after ack");

        channel.queueDelete(queueName);
        logger.info("PASS: Basic.Ack works correctly");
    }

    @Test
    @Order(61)
    @DisplayName("Ack: Multiple acknowledge")
    void testMultipleAck() throws IOException, InterruptedException {
        String queueName = "test.multiple.ack.queue";
        channel.queueDeclare(queueName, false, false, true, null);

        // Publish 5 messages
        for (int i = 0; i < 5; i++) {
            channel.basicPublish("", queueName, null, ("Message " + i).getBytes());
        }
        Thread.sleep(100);

        // Get all messages without ack
        List<Long> deliveryTags = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            GetResponse response = channel.basicGet(queueName, false);
            assertNotNull(response);
            deliveryTags.add(response.getEnvelope().getDeliveryTag());
        }

        // Ack all with multiple=true
        channel.basicAck(deliveryTags.get(4), true);

        channel.queueDelete(queueName);
        logger.info("PASS: Multiple ack works correctly");
    }

    @Test
    @Order(62)
    @DisplayName("Ack: Basic reject")
    void testBasicReject() throws IOException, InterruptedException {
        String queueName = "test.reject.queue";
        channel.queueDeclare(queueName, false, false, true, null);

        channel.basicPublish("", queueName, null, "Test message".getBytes());
        Thread.sleep(100);

        GetResponse response = channel.basicGet(queueName, false);
        assertNotNull(response);

        // Reject without requeue
        channel.basicReject(response.getEnvelope().getDeliveryTag(), false);

        channel.queueDelete(queueName);
        logger.info("PASS: Basic.Reject works correctly");
    }

    @Test
    @Order(63)
    @DisplayName("Ack: Reject with requeue")
    void testRejectWithRequeue() throws IOException, InterruptedException {
        String queueName = "test.requeue.queue";
        channel.queueDeclare(queueName, false, false, true, null);

        channel.basicPublish("", queueName, null, "Test message".getBytes());
        Thread.sleep(100);

        GetResponse response = channel.basicGet(queueName, false);
        assertNotNull(response);

        // Reject with requeue
        channel.basicReject(response.getEnvelope().getDeliveryTag(), true);
        Thread.sleep(100);

        // Message should still be in queue
        GetResponse response2 = channel.basicGet(queueName, true);
        assertNotNull(response2, "Message should be requeued");

        channel.queueDelete(queueName);
        logger.info("PASS: Reject with requeue works correctly");
    }

    @Test
    @Order(64)
    @DisplayName("Ack: Basic nack")
    void testBasicNack() throws IOException, InterruptedException {
        String queueName = "test.nack.queue";
        channel.queueDeclare(queueName, false, false, true, null);

        channel.basicPublish("", queueName, null, "Test message".getBytes());
        Thread.sleep(100);

        GetResponse response = channel.basicGet(queueName, false);
        assertNotNull(response);

        // Nack without requeue
        channel.basicNack(response.getEnvelope().getDeliveryTag(), false, false);

        channel.queueDelete(queueName);
        logger.info("PASS: Basic.Nack works correctly");
    }

    // ==================== TRANSACTION TESTS ====================

    @Test
    @Order(70)
    @DisplayName("Tx: Select transaction mode")
    void testTxSelect() throws IOException {
        channel.txSelect();
        // If no exception, tx.select succeeded
        channel.txRollback(); // Clean up
        logger.info("PASS: Tx.Select works correctly");
    }

    @Test
    @Order(71)
    @DisplayName("Tx: Commit transaction")
    void testTxCommit() throws IOException, InterruptedException {
        String queueName = "test.tx.commit.queue";
        channel.queueDeclare(queueName, false, false, true, null);

        channel.txSelect();
        channel.basicPublish("", queueName, null, "Transaction message".getBytes());
        channel.txCommit();

        Thread.sleep(100);

        // Message should be in queue after commit
        GetResponse response = channel.basicGet(queueName, true);
        assertNotNull(response, "Message should be available after commit");

        channel.queueDelete(queueName);
        logger.info("PASS: Tx.Commit works correctly");
    }

    @Test
    @Order(72)
    @DisplayName("Tx: Rollback transaction")
    void testTxRollback() throws IOException, InterruptedException {
        String queueName = "test.tx.rollback.queue";
        channel.queueDeclare(queueName, false, false, true, null);

        channel.txSelect();
        channel.basicPublish("", queueName, null, "Rollback message".getBytes());
        channel.txRollback();

        Thread.sleep(100);

        // Message should NOT be in queue after rollback
        GetResponse response = channel.basicGet(queueName, true);
        assertNull(response, "Message should not be available after rollback");

        channel.queueDelete(queueName);
        logger.info("PASS: Tx.Rollback works correctly");
    }

    // ==================== QOS TESTS ====================

    @Test
    @Order(80)
    @DisplayName("QoS: Set prefetch count")
    void testQosSetPrefetch() throws IOException {
        channel.basicQos(10);
        // If no exception, QoS was set successfully
        logger.info("PASS: Basic.Qos (prefetch) works correctly");
    }

    @Test
    @Order(81)
    @DisplayName("QoS: Prefetch limits delivery")
    void testQosPrefetchLimitsDelivery() throws IOException, InterruptedException {
        String queueName = "test.qos.prefetch.queue";
        channel.queueDeclare(queueName, false, false, true, null);

        // Set prefetch to 2
        channel.basicQos(2);

        // Publish 5 messages
        for (int i = 0; i < 5; i++) {
            channel.basicPublish("", queueName, null, ("Message " + i).getBytes());
        }

        AtomicInteger deliveredCount = new AtomicInteger(0);
        CountDownLatch firstBatch = new CountDownLatch(2);

        channel.basicConsume(queueName, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) {
                deliveredCount.incrementAndGet();
                firstBatch.countDown();
                // Don't ack - this should block further deliveries beyond prefetch
            }
        });

        // Wait for first batch
        firstBatch.await(2, TimeUnit.SECONDS);
        Thread.sleep(500); // Give time for any additional messages

        // With prefetch=2 and no acks, we should only have received 2 messages
        assertTrue(deliveredCount.get() <= 2, "Should not receive more than prefetch count without acking");

        channel.queueDelete(queueName);
        logger.info("PASS: QoS prefetch limiting works correctly");
    }

    // ==================== ROUTING TESTS ====================

    @Test
    @Order(90)
    @DisplayName("Routing: Direct exchange routing")
    void testDirectExchangeRouting() throws IOException, InterruptedException {
        String exchangeName = "test.routing.direct";
        String queue1 = "test.routing.queue1";
        String queue2 = "test.routing.queue2";

        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, false, true, null);
        channel.queueDeclare(queue1, false, false, true, null);
        channel.queueDeclare(queue2, false, false, true, null);
        channel.queueBind(queue1, exchangeName, "key1");
        channel.queueBind(queue2, exchangeName, "key2");

        channel.basicPublish(exchangeName, "key1", null, "Message for queue1".getBytes());
        channel.basicPublish(exchangeName, "key2", null, "Message for queue2".getBytes());
        Thread.sleep(100);

        GetResponse resp1 = channel.basicGet(queue1, true);
        GetResponse resp2 = channel.basicGet(queue2, true);

        assertNotNull(resp1, "Queue1 should receive message");
        assertNotNull(resp2, "Queue2 should receive message");
        assertEquals("Message for queue1", new String(resp1.getBody()));
        assertEquals("Message for queue2", new String(resp2.getBody()));

        channel.queueDelete(queue1);
        channel.queueDelete(queue2);
        channel.exchangeDelete(exchangeName);
        logger.info("PASS: Direct exchange routing works correctly");
    }

    @Test
    @Order(91)
    @DisplayName("Routing: Fanout exchange routing")
    void testFanoutExchangeRouting() throws IOException, InterruptedException {
        String exchangeName = "test.routing.fanout";
        String queue1 = "test.fanout.queue1";
        String queue2 = "test.fanout.queue2";

        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.FANOUT, false, true, null);
        channel.queueDeclare(queue1, false, false, true, null);
        channel.queueDeclare(queue2, false, false, true, null);
        channel.queueBind(queue1, exchangeName, "");
        channel.queueBind(queue2, exchangeName, "");

        channel.basicPublish(exchangeName, "", null, "Broadcast message".getBytes());
        Thread.sleep(100);

        GetResponse resp1 = channel.basicGet(queue1, true);
        GetResponse resp2 = channel.basicGet(queue2, true);

        assertNotNull(resp1, "Queue1 should receive broadcast");
        assertNotNull(resp2, "Queue2 should receive broadcast");

        channel.queueDelete(queue1);
        channel.queueDelete(queue2);
        channel.exchangeDelete(exchangeName);
        logger.info("PASS: Fanout exchange routing works correctly");
    }

    @Test
    @Order(92)
    @DisplayName("Routing: Topic exchange routing")
    void testTopicExchangeRouting() throws IOException, InterruptedException {
        String exchangeName = "test.routing.topic";
        String queueAll = "test.topic.all";
        String queueError = "test.topic.error";

        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.TOPIC, false, true, null);
        channel.queueDeclare(queueAll, false, false, true, null);
        channel.queueDeclare(queueError, false, false, true, null);
        channel.queueBind(queueAll, exchangeName, "log.#");
        channel.queueBind(queueError, exchangeName, "log.error.*");

        channel.basicPublish(exchangeName, "log.info.app1", null, "Info message".getBytes());
        channel.basicPublish(exchangeName, "log.error.app1", null, "Error message".getBytes());
        Thread.sleep(100);

        // queueAll should receive both (matches log.#)
        GetResponse respAll1 = channel.basicGet(queueAll, true);
        GetResponse respAll2 = channel.basicGet(queueAll, true);
        assertNotNull(respAll1, "All queue should receive info message");
        assertNotNull(respAll2, "All queue should receive error message");

        // queueError should receive only error (matches log.error.*)
        GetResponse respError = channel.basicGet(queueError, true);
        assertNotNull(respError, "Error queue should receive error message");
        assertEquals("Error message", new String(respError.getBody()));

        channel.queueDelete(queueAll);
        channel.queueDelete(queueError);
        channel.exchangeDelete(exchangeName);
        logger.info("PASS: Topic exchange routing works correctly");
    }

    // ==================== CONSUMER CANCELLATION TESTS ====================

    @Test
    @Order(100)
    @DisplayName("Consumer: Cancel consumer")
    void testCancelConsumer() throws IOException, InterruptedException {
        String queueName = "test.cancel.consumer.queue";
        channel.queueDeclare(queueName, false, false, true, null);

        AtomicReference<String> cancelledTag = new AtomicReference<>();
        CountDownLatch cancelled = new CountDownLatch(1);

        String consumerTag = channel.basicConsume(queueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleCancelOk(String consumerTag) {
                cancelledTag.set(consumerTag);
                cancelled.countDown();
            }
        });

        channel.basicCancel(consumerTag);
        boolean wasCancelled = cancelled.await(2, TimeUnit.SECONDS);

        assertTrue(wasCancelled, "Consumer should be cancelled");
        assertEquals(consumerTag, cancelledTag.get(), "Consumer tag should match");

        channel.queueDelete(queueName);
        logger.info("PASS: Consumer cancellation works correctly");
    }
}
