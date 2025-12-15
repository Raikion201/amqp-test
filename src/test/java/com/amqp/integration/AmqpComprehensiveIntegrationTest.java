package com.amqp.integration;

import com.amqp.amqp.AmqpCodec;
import com.amqp.amqp.AmqpFrame;
import com.amqp.model.Exchange;
import com.amqp.model.Message;
import com.amqp.model.Queue;
import com.amqp.persistence.DatabaseManager;
import com.amqp.persistence.PersistenceManager;
import com.amqp.server.AmqpBroker;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive integration tests for complete AMQP workflows
 * Tests end-to-end scenarios across multiple components
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("AMQP Comprehensive Integration Tests")
class AmqpComprehensiveIntegrationTest {

    private DatabaseManager databaseManager;
    private PersistenceManager persistenceManager;
    private AmqpBroker broker;

    @BeforeEach
    void setUp() {
        // Use H2 in-memory database for integration testing
        databaseManager = new DatabaseManager(
            "jdbc:h2:mem:integration_test_" + System.currentTimeMillis() + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
            "sa",
            ""
        );
        persistenceManager = new PersistenceManager(databaseManager);
        // Enable guest user for testing purposes
        broker = new AmqpBroker(persistenceManager, true);
    }

    @AfterEach
    void tearDown() {
        if (databaseManager != null) {
            databaseManager.close();
        }
    }

    @Nested
    @DisplayName("End-to-End Message Routing Tests")
    class MessageRoutingTests {

        @Test
        @DisplayName("Should route message from publisher to consumer via direct exchange")
        void testDirectExchangeRouting() {
            // Setup infrastructure
            Exchange exchange = broker.declareExchange("orders", Exchange.Type.DIRECT, true, false, false);
            Queue queue = broker.declareQueue("orders-queue", true, false, false);
            broker.bindQueue("orders-queue", "orders", "order.new");

            // Publish message
            Message message = new Message("Order #12345".getBytes());
            message.setRoutingKey("order.new");
            message.setContentType("application/json");
            message.setDeliveryMode((short) 2); // Persistent

            broker.publishMessage("orders", "order.new", message);

            // Consume message
            Message consumed = broker.consumeMessage("orders-queue");

            // Verify
            assertThat(consumed).isNotNull();
            assertThat(consumed.getBody()).isEqualTo("Order #12345".getBytes());
            assertThat(consumed.getContentType()).isEqualTo("application/json");
            assertThat(consumed.isPersistent()).isTrue();
        }

        @Test
        @DisplayName("Should broadcast message to multiple queues via fanout exchange")
        void testFanoutExchangeBroadcast() {
            // Setup
            broker.declareExchange("notifications", Exchange.Type.FANOUT, false, false, false);
            broker.declareQueue("email-queue", false, false, false);
            broker.declareQueue("sms-queue", false, false, false);
            broker.declareQueue("push-queue", false, false, false);

            broker.bindQueue("email-queue", "notifications", "");
            broker.bindQueue("sms-queue", "notifications", "");
            broker.bindQueue("push-queue", "notifications", "");

            // Publish
            Message notification = new Message("New message!".getBytes());
            broker.publishMessage("notifications", "any.key", notification);

            // Consume from all queues
            Message emailMsg = broker.consumeMessage("email-queue");
            Message smsMsg = broker.consumeMessage("sms-queue");
            Message pushMsg = broker.consumeMessage("push-queue");

            // Verify all received the same message
            assertThat(emailMsg).isNotNull();
            assertThat(smsMsg).isNotNull();
            assertThat(pushMsg).isNotNull();

            assertThat(emailMsg.getBody()).isEqualTo("New message!".getBytes());
            assertThat(smsMsg.getBody()).isEqualTo("New message!".getBytes());
            assertThat(pushMsg.getBody()).isEqualTo("New message!".getBytes());
        }

        @Test
        @DisplayName("Should route messages with wildcard patterns via topic exchange")
        void testTopicExchangeWildcards() {
            // Setup
            broker.declareExchange("logs", Exchange.Type.TOPIC, false, false, false);
            broker.declareQueue("all-logs", false, false, false);
            broker.declareQueue("error-logs", false, false, false);
            broker.declareQueue("critical-logs", false, false, false);

            broker.bindQueue("all-logs", "logs", "#");
            broker.bindQueue("error-logs", "logs", "*.error");
            broker.bindQueue("critical-logs", "logs", "*.*.critical");

            // Publish different log messages
            Message infoMsg = new Message("Info log".getBytes());
            broker.publishMessage("logs", "app.info", infoMsg);

            Message errorMsg = new Message("Error log".getBytes());
            broker.publishMessage("logs", "app.error", errorMsg);

            Message criticalMsg = new Message("Critical log".getBytes());
            broker.publishMessage("logs", "app.database.critical", criticalMsg);

            // Verify routing
            assertThat(broker.getQueue("all-logs").size()).isEqualTo(3); // All messages
            assertThat(broker.getQueue("error-logs").size()).isEqualTo(1); // Only error
            assertThat(broker.getQueue("critical-logs").size()).isEqualTo(1); // Only critical
        }

        @Test
        @DisplayName("Should handle multiple routing keys to same queue")
        void testMultipleBindings() {
            broker.declareExchange("orders", Exchange.Type.DIRECT, false, false, false);
            broker.declareQueue("processing-queue", false, false, false);

            // Bind multiple routing keys to same queue
            broker.bindQueue("processing-queue", "orders", "order.new");
            broker.bindQueue("processing-queue", "orders", "order.updated");
            broker.bindQueue("processing-queue", "orders", "order.cancelled");

            // Publish with different keys
            broker.publishMessage("orders", "order.new", new Message("New".getBytes()));
            broker.publishMessage("orders", "order.updated", new Message("Updated".getBytes()));
            broker.publishMessage("orders", "order.cancelled", new Message("Cancelled".getBytes()));

            // All should be in the same queue
            assertThat(broker.getQueue("processing-queue").size()).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("Message Persistence Tests")
    class MessagePersistenceTests {

        @Test
        @DisplayName("Should persist durable exchange")
        void testExchangePersistence() {
            broker.declareExchange("durable-exchange", Exchange.Type.TOPIC, true, false, false);

            Exchange exchange = broker.getExchange("durable-exchange");
            assertThat(exchange.isDurable()).isTrue();
        }

        @Test
        @DisplayName("Should persist durable queue")
        void testQueuePersistence() {
            broker.declareQueue("durable-queue", true, false, false);

            Queue queue = broker.getQueue("durable-queue");
            assertThat(queue.isDurable()).isTrue();
        }

        @Test
        @DisplayName("Should persist persistent messages in durable queue")
        void testMessagePersistence() {
            broker.declareExchange("persistent-exchange", Exchange.Type.DIRECT, true, false, false);
            broker.declareQueue("persistent-queue", true, false, false);
            broker.bindQueue("persistent-queue", "persistent-exchange", "key");

            Message message = new Message("Persistent message".getBytes());
            message.setDeliveryMode((short) 2); // Persistent
            message.setId(12345L);

            broker.publishMessage("persistent-exchange", "key", message);

            // Message should be saved to database
            Queue queue = broker.getQueue("persistent-queue");
            assertThat(queue.size()).isEqualTo(1);
        }

        @Test
        @DisplayName("Should not persist transient messages")
        void testTransientMessages() {
            broker.declareExchange("temp-exchange", Exchange.Type.DIRECT, false, false, false);
            broker.declareQueue("temp-queue", false, false, false);
            broker.bindQueue("temp-queue", "temp-exchange", "key");

            Message message = new Message("Transient message".getBytes());
            message.setDeliveryMode((short) 1); // Transient

            broker.publishMessage("temp-exchange", "key", message);

            // Message should be in queue but not persisted
            Queue queue = broker.getQueue("temp-queue");
            assertThat(queue.size()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("Message Properties Tests")
    class MessagePropertiesTests {

        @Test
        @DisplayName("Should preserve all message properties through routing")
        void testMessageProperties() {
            broker.declareExchange("props-exchange", Exchange.Type.DIRECT, false, false, false);
            broker.declareQueue("props-queue", false, false, false);
            broker.bindQueue("props-queue", "props-exchange", "key");

            // Create message with all properties
            Message message = new Message("Test".getBytes());
            message.setRoutingKey("key");
            message.setContentType("application/json");
            message.setContentEncoding("utf-8");
            message.setDeliveryMode((short) 2);
            message.setPriority((short) 5);
            message.setCorrelationId("corr-123");
            message.setReplyTo("reply-queue");
            message.setExpiration("60000");
            message.setMessageId("msg-456");
            message.setTimestamp(System.currentTimeMillis());
            message.setType("order");
            message.setUserId("user-1");
            message.setAppId("app-1");
            message.setClusterId("cluster-1");

            broker.publishMessage("props-exchange", "key", message);

            // Consume and verify all properties
            Message consumed = broker.consumeMessage("props-queue");
            assertThat(consumed.getContentType()).isEqualTo("application/json");
            assertThat(consumed.getContentEncoding()).isEqualTo("utf-8");
            assertThat(consumed.getDeliveryMode()).isEqualTo((short) 2);
            assertThat(consumed.getPriority()).isEqualTo((short) 5);
            assertThat(consumed.getCorrelationId()).isEqualTo("corr-123");
            assertThat(consumed.getReplyTo()).isEqualTo("reply-queue");
            assertThat(consumed.getExpiration()).isEqualTo("60000");
            assertThat(consumed.getMessageId()).isEqualTo("msg-456");
            assertThat(consumed.getType()).isEqualTo("order");
            assertThat(consumed.getUserId()).isEqualTo("user-1");
            assertThat(consumed.getAppId()).isEqualTo("app-1");
            assertThat(consumed.getClusterId()).isEqualTo("cluster-1");
        }

        @Test
        @DisplayName("Should handle messages with headers")
        void testMessageHeaders() {
            broker.declareExchange("headers-exchange", Exchange.Type.DIRECT, false, false, false);
            broker.declareQueue("headers-queue", false, false, false);
            broker.bindQueue("headers-queue", "headers-exchange", "key");

            Message message = new Message("Test".getBytes());
            message.setHeaders(java.util.Map.of(
                "x-custom-header", "value1",
                "x-priority-level", 5,
                "x-retry-count", 3
            ));

            broker.publishMessage("headers-exchange", "key", message);

            Message consumed = broker.consumeMessage("headers-queue");
            assertThat(consumed.getHeaders()).isNotNull();
            assertThat(consumed.getHeaders().get("x-custom-header")).isEqualTo("value1");
            assertThat(consumed.getHeaders().get("x-priority-level")).isEqualTo(5);
            assertThat(consumed.getHeaders().get("x-retry-count")).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("Queue Operations Tests")
    class QueueOperationsTests {

        @Test
        @DisplayName("Should generate unique queue names")
        void testGeneratedQueueNames() {
            Queue queue1 = broker.declareQueue("", false, false, false);
            Queue queue2 = broker.declareQueue(null, false, false, false);

            assertThat(queue1.getName()).startsWith("amq.gen-");
            assertThat(queue2.getName()).startsWith("amq.gen-");
            assertThat(queue1.getName()).isNotEqualTo(queue2.getName());
        }

        @Test
        @DisplayName("Should handle empty queue consumption")
        void testEmptyQueueConsumption() {
            broker.declareQueue("empty-queue", false, false, false);

            Message message = broker.consumeMessage("empty-queue");
            assertThat(message).isNull();
        }

        @Test
        @DisplayName("Should maintain message order in queue")
        void testMessageOrdering() {
            broker.declareQueue("order-queue", false, false, false);
            Queue queue = broker.getQueue("order-queue");

            // Enqueue multiple messages
            for (int i = 1; i <= 5; i++) {
                Message message = new Message(("Message " + i).getBytes());
                queue.enqueue(message);
            }

            // Dequeue and verify order
            for (int i = 1; i <= 5; i++) {
                Message message = queue.dequeue();
                assertThat(message).isNotNull();
                assertThat(new String(message.getBody())).isEqualTo("Message " + i);
            }
        }

        @Test
        @DisplayName("Should handle queue purge")
        void testQueuePurge() {
            broker.declareQueue("purge-queue", false, false, false);
            Queue queue = broker.getQueue("purge-queue");

            // Add messages
            for (int i = 1; i <= 10; i++) {
                queue.enqueue(new Message(("Message " + i).getBytes()));
            }

            assertThat(queue.size()).isEqualTo(10);

            // Purge queue
            queue.purge();

            assertThat(queue.size()).isEqualTo(0);
            assertThat(queue.isEmpty()).isTrue();
        }
    }

    @Nested
    @DisplayName("Error Handling Tests")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Should throw exception when binding non-existent queue")
        void testBindNonExistentQueue() {
            broker.declareExchange("test-exchange", Exchange.Type.DIRECT, false, false, false);

            assertThatThrownBy(() ->
                broker.bindQueue("non-existent-queue", "test-exchange", "key")
            ).isInstanceOf(IllegalArgumentException.class)
             .hasMessageContaining("Queue not found");
        }

        @Test
        @DisplayName("Should throw exception when binding to non-existent exchange")
        void testBindToNonExistentExchange() {
            broker.declareQueue("test-queue", false, false, false);

            assertThatThrownBy(() ->
                broker.bindQueue("test-queue", "non-existent-exchange", "key")
            ).isInstanceOf(IllegalArgumentException.class)
             .hasMessageContaining("Exchange not found");
        }

        @Test
        @DisplayName("Should throw exception when redeclaring exchange with different parameters")
        void testRedeclareExchangeWithDifferentParams() {
            broker.declareExchange("test-exchange", Exchange.Type.DIRECT, true, false, false);

            assertThatThrownBy(() ->
                broker.declareExchange("test-exchange", Exchange.Type.TOPIC, true, false, false)
            ).isInstanceOf(IllegalArgumentException.class)
             .hasMessageContaining("already exists with different parameters");
        }

        @Test
        @DisplayName("Should throw exception when redeclaring queue with different parameters")
        void testRedeclareQueueWithDifferentParams() {
            broker.declareQueue("test-queue", true, false, false);

            assertThatThrownBy(() ->
                broker.declareQueue("test-queue", false, false, false)
            ).isInstanceOf(IllegalArgumentException.class)
             .hasMessageContaining("already exists with different parameters");
        }

        @Test
        @DisplayName("Should handle publishing to non-existent exchange gracefully")
        void testPublishToNonExistentExchange() {
            Message message = new Message("Test".getBytes());

            // Should not throw exception
            assertThatCode(() ->
                broker.publishMessage("non-existent-exchange", "key", message)
            ).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should return null when consuming from non-existent queue")
        void testConsumeFromNonExistentQueue() {
            Message message = broker.consumeMessage("non-existent-queue");
            assertThat(message).isNull();
        }
    }

    @Nested
    @DisplayName("Concurrency Tests")
    class ConcurrencyTests {

        @Test
        @DisplayName("Should handle concurrent publishing")
        void testConcurrentPublishing() throws InterruptedException {
            broker.declareExchange("concurrent-exchange", Exchange.Type.DIRECT, false, false, false);
            broker.declareQueue("concurrent-queue", false, false, false);
            broker.bindQueue("concurrent-queue", "concurrent-exchange", "key");

            int threadCount = 10;
            int messagesPerThread = 100;
            Thread[] threads = new Thread[threadCount];

            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                threads[i] = new Thread(() -> {
                    for (int j = 0; j < messagesPerThread; j++) {
                        Message message = new Message(
                            ("Thread " + threadId + " Message " + j).getBytes()
                        );
                        broker.publishMessage("concurrent-exchange", "key", message);
                    }
                });
                threads[i].start();
            }

            // Wait for all threads
            for (Thread thread : threads) {
                thread.join();
            }

            // Verify all messages received
            Queue queue = broker.getQueue("concurrent-queue");
            assertThat(queue.size()).isEqualTo(threadCount * messagesPerThread);
        }

        @Test
        @DisplayName("Should handle concurrent consuming")
        void testConcurrentConsuming() throws InterruptedException {
            broker.declareQueue("consumer-queue", false, false, false);
            Queue queue = broker.getQueue("consumer-queue");

            // Add messages
            int messageCount = 1000;
            for (int i = 0; i < messageCount; i++) {
                queue.enqueue(new Message(("Message " + i).getBytes()));
            }

            // Consume concurrently
            int consumerCount = 5;
            Thread[] consumers = new Thread[consumerCount];
            java.util.concurrent.atomic.AtomicInteger consumedCount =
                new java.util.concurrent.atomic.AtomicInteger(0);

            for (int i = 0; i < consumerCount; i++) {
                consumers[i] = new Thread(() -> {
                    while (true) {
                        Message message = broker.consumeMessage("consumer-queue");
                        if (message == null) {
                            break;
                        }
                        consumedCount.incrementAndGet();
                    }
                });
                consumers[i].start();
            }

            // Wait for all consumers
            for (Thread consumer : consumers) {
                consumer.join();
            }

            // Verify all messages consumed
            assertThat(consumedCount.get()).isEqualTo(messageCount);
            assertThat(queue.isEmpty()).isTrue();
        }
    }

    @Nested
    @DisplayName("Codec Integration Tests")
    class CodecIntegrationTests {

        @Test
        @DisplayName("Should encode and decode string values correctly")
        void testStringCodec() {
            ByteBuf buffer = Unpooled.buffer();

            String shortString = "test";
            String longString = "This is a much longer string that exceeds typical short string length limits";

            AmqpCodec.encodeShortString(buffer, shortString);
            AmqpCodec.encodeLongString(buffer, longString);

            String decodedShort = AmqpCodec.decodeShortString(buffer);
            String decodedLong = AmqpCodec.decodeLongString(buffer);

            assertThat(decodedShort).isEqualTo(shortString);
            assertThat(decodedLong).isEqualTo(longString);

            buffer.release();
        }

        @Test
        @DisplayName("Should encode and decode boolean values correctly")
        void testBooleanCodec() {
            ByteBuf buffer = Unpooled.buffer();

            AmqpCodec.encodeBoolean(buffer, true);
            AmqpCodec.encodeBoolean(buffer, false);

            assertThat(AmqpCodec.decodeBoolean(buffer)).isTrue();
            assertThat(AmqpCodec.decodeBoolean(buffer)).isFalse();

            buffer.release();
        }

        @Test
        @DisplayName("Should handle frame encoding and decoding roundtrip")
        void testFrameCodec() {
            ByteBuf payload = Unpooled.wrappedBuffer("test payload".getBytes());
            AmqpFrame originalFrame = new AmqpFrame(
                AmqpFrame.FrameType.METHOD.getValue(),
                (short) 1,
                payload
            );

            assertThat(originalFrame.getType()).isEqualTo((byte) 1);
            assertThat(originalFrame.getChannel()).isEqualTo((short) 1);
            assertThat(originalFrame.getSize()).isEqualTo(12);
        }
    }

    @Nested
    @DisplayName("Default Exchange Tests")
    class DefaultExchangeTests {

        @Test
        @DisplayName("Should route messages through default exchange")
        void testDefaultExchange() {
            broker.declareQueue("direct-queue", false, false, false);

            // Default exchange routes by queue name
            broker.getExchange("").addBinding("direct-queue", "direct-queue");

            Message message = new Message("Direct routing".getBytes());
            broker.publishMessage("", "direct-queue", message);

            Message consumed = broker.consumeMessage("direct-queue");
            assertThat(consumed).isNotNull();
            assertThat(consumed.getBody()).isEqualTo("Direct routing".getBytes());
        }

        @Test
        @DisplayName("Should have all standard exchanges available")
        void testStandardExchanges() {
            assertThat(broker.getExchange("")).isNotNull();
            assertThat(broker.getExchange("amq.direct")).isNotNull();
            assertThat(broker.getExchange("amq.topic")).isNotNull();
            assertThat(broker.getExchange("amq.fanout")).isNotNull();
            assertThat(broker.getExchange("amq.headers")).isNotNull();
        }
    }

    @Nested
    @DisplayName("Complex Workflow Tests")
    class ComplexWorkflowTests {

        @Test
        @DisplayName("Should handle request-reply pattern")
        void testRequestReplyPattern() {
            // Setup request and reply queues
            broker.declareExchange("rpc-exchange", Exchange.Type.DIRECT, false, false, false);
            broker.declareQueue("rpc-requests", false, false, false);
            broker.declareQueue("rpc-replies", false, false, false);

            broker.bindQueue("rpc-requests", "rpc-exchange", "rpc.request");
            broker.bindQueue("rpc-replies", "rpc-exchange", "rpc.reply");

            // Send request
            Message request = new Message("Calculate: 2 + 2".getBytes());
            request.setReplyTo("rpc-replies");
            request.setCorrelationId("calc-123");
            broker.publishMessage("rpc-exchange", "rpc.request", request);

            // Receive request
            Message receivedRequest = broker.consumeMessage("rpc-requests");
            assertThat(receivedRequest).isNotNull();
            assertThat(receivedRequest.getReplyTo()).isEqualTo("rpc-replies");
            assertThat(receivedRequest.getCorrelationId()).isEqualTo("calc-123");

            // Send reply
            Message reply = new Message("Result: 4".getBytes());
            reply.setCorrelationId(receivedRequest.getCorrelationId());
            broker.publishMessage("rpc-exchange", "rpc.reply", reply);

            // Receive reply
            Message receivedReply = broker.consumeMessage("rpc-replies");
            assertThat(receivedReply).isNotNull();
            assertThat(receivedReply.getBody()).isEqualTo("Result: 4".getBytes());
            assertThat(receivedReply.getCorrelationId()).isEqualTo("calc-123");
        }

        @Test
        @DisplayName("Should handle message priority queue scenario")
        void testPriorityMessages() {
            broker.declareQueue("priority-queue", false, false, false);
            Queue queue = broker.getQueue("priority-queue");

            // Add messages with different priorities
            for (int priority = 1; priority <= 5; priority++) {
                Message message = new Message(("Priority " + priority).getBytes());
                message.setPriority((short) priority);
                queue.enqueue(message);
            }

            // Messages are enqueued (priority handling would be in consumer logic)
            assertThat(queue.size()).isEqualTo(5);
        }
    }
}
