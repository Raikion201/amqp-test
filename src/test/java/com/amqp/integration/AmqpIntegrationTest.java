package com.amqp.integration;

import com.amqp.model.Exchange;
import com.amqp.model.Message;
import com.amqp.model.Queue;
import com.amqp.persistence.PersistenceManager;
import com.amqp.server.AmqpBroker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.*;
import java.util.List;
import java.util.ArrayList;

import static org.assertj.core.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("Integration Tests - RabbitMQ-like Scenarios")
class AmqpIntegrationTest {

    @Mock
    private PersistenceManager persistenceManager;

    private AmqpBroker broker;

    @BeforeEach
    void setUp() {
        // Enable guest user for testing purposes
        broker = new AmqpBroker(persistenceManager, true);
    }

    @Test
    @DisplayName("Scenario: Work Queue Pattern")
    void testWorkQueuePattern() throws InterruptedException {
        // Setup: One queue with multiple consumers (workers)
        broker.declareExchange("", Exchange.Type.DIRECT, true, false, false);
        broker.declareQueue("task-queue", true, false, false);
        broker.bindQueue("task-queue", "", "task-queue");

        // Producer: Publish 10 tasks
        for (int i = 1; i <= 10; i++) {
            Message task = new Message(("Task #" + i).getBytes());
            task.setDeliveryMode((short) 2); // Persistent
            broker.publishMessage("", "task-queue", task);
        }

        // Consumers: 3 workers consume tasks
        ExecutorService executor = Executors.newFixedThreadPool(3);
        List<Future<Integer>> results = new ArrayList<>();
        
        for (int workerId = 1; workerId <= 3; workerId++) {
            Future<Integer> result = executor.submit(() -> {
                int count = 0;
                Message task;
                while ((task = broker.consumeMessage("task-queue")) != null) {
                    count++;
                    Thread.sleep(10); // Simulate work
                }
                return count;
            });
            results.add(result);
        }

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // Verify: All tasks consumed
        int totalConsumed = results.stream()
            .mapToInt(f -> {
                try {
                    return f.get();
                } catch (Exception e) {
                    return 0;
                }
            })
            .sum();

        assertThat(totalConsumed).isEqualTo(10);
        assertThat(broker.getQueue("task-queue").size()).isEqualTo(0);
    }

    @Test
    @DisplayName("Scenario: Pub/Sub Pattern with Fanout Exchange")
    void testPubSubPattern() {
        // Setup: Fanout exchange with multiple queues
        broker.declareExchange("notifications", Exchange.Type.FANOUT, false, false, false);
        broker.declareQueue("email-service", false, false, false);
        broker.declareQueue("sms-service", false, false, false);
        broker.declareQueue("push-service", false, false, false);
        broker.declareQueue("audit-service", false, false, false);

        broker.bindQueue("email-service", "notifications", "");
        broker.bindQueue("sms-service", "notifications", "");
        broker.bindQueue("push-service", "notifications", "");
        broker.bindQueue("audit-service", "notifications", "");

        // Publish notification
        Message notification = new Message("User logged in".getBytes());
        broker.publishMessage("notifications", "", notification);

        // Verify: All services received the notification
        assertThat(broker.getQueue("email-service").size()).isEqualTo(1);
        assertThat(broker.getQueue("sms-service").size()).isEqualTo(1);
        assertThat(broker.getQueue("push-service").size()).isEqualTo(1);
        assertThat(broker.getQueue("audit-service").size()).isEqualTo(1);
    }

    @Test
    @DisplayName("Scenario: Routing Pattern with Direct Exchange")
    void testRoutingPattern() {
        // Setup: Direct exchange for log levels
        broker.declareExchange("logs", Exchange.Type.DIRECT, false, false, false);
        broker.declareQueue("error-logs", false, false, false);
        broker.declareQueue("all-logs", false, false, false);

        broker.bindQueue("error-logs", "logs", "error");
        broker.bindQueue("all-logs", "logs", "info");
        broker.bindQueue("all-logs", "logs", "warning");
        broker.bindQueue("all-logs", "logs", "error");

        // Publish messages with different severities
        broker.publishMessage("logs", "info", new Message("Info message".getBytes()));
        broker.publishMessage("logs", "warning", new Message("Warning message".getBytes()));
        broker.publishMessage("logs", "error", new Message("Error message".getBytes()));

        // Verify routing
        assertThat(broker.getQueue("error-logs").size()).isEqualTo(1);
        assertThat(broker.getQueue("all-logs").size()).isEqualTo(3);
    }

    @Test
    @DisplayName("Scenario: Topic Pattern for Complex Routing")
    void testTopicPattern() {
        // Setup: Topic exchange for order events
        broker.declareExchange("orders", Exchange.Type.TOPIC, false, false, false);
        broker.declareQueue("us-orders", false, false, false);
        broker.declareQueue("critical-orders", false, false, false);
        broker.declareQueue("all-created-orders", false, false, false);

        broker.bindQueue("us-orders", "orders", "us.#");
        broker.bindQueue("critical-orders", "orders", "*.*.critical");
        broker.bindQueue("all-created-orders", "orders", "*.*.created");

        // Publish various order events
        broker.publishMessage("orders", "us.orders.created", new Message("US Order 1".getBytes()));
        broker.publishMessage("orders", "us.orders.critical", new Message("US Critical Order".getBytes()));
        broker.publishMessage("orders", "eu.orders.created", new Message("EU Order 1".getBytes()));
        broker.publishMessage("orders", "eu.orders.critical", new Message("EU Critical Order".getBytes()));
        broker.publishMessage("orders", "us.orders.updated", new Message("US Order Updated".getBytes()));

        // Verify topic routing
        assertThat(broker.getQueue("us-orders").size()).isEqualTo(3); // us.*
        assertThat(broker.getQueue("critical-orders").size()).isEqualTo(2); // *.*.critical
        assertThat(broker.getQueue("all-created-orders").size()).isEqualTo(2); // *.*.created
    }

    @Test
    @DisplayName("Scenario: RPC Pattern with Reply-To")
    void testRPCPattern() {
        // Setup: Request and reply queues
        broker.declareExchange("", Exchange.Type.DIRECT, true, false, false);
        broker.declareQueue("rpc-queue", false, false, false);
        broker.declareQueue("reply-queue", false, false, false);
        
        broker.bindQueue("rpc-queue", "", "rpc-queue");
        broker.bindQueue("reply-queue", "", "reply-queue");

        // Client: Send request with reply-to
        Message request = new Message("Calculate 5 + 3".getBytes());
        request.setCorrelationId("req-123");
        request.setReplyTo("reply-queue");
        broker.publishMessage("", "rpc-queue", request);

        // Server: Process request and send reply
        Message receivedRequest = broker.consumeMessage("rpc-queue");
        assertThat(receivedRequest).isNotNull();
        
        Message response = new Message("Result: 8".getBytes());
        response.setCorrelationId(receivedRequest.getCorrelationId());
        broker.publishMessage("", receivedRequest.getReplyTo(), response);

        // Client: Receive reply
        Message reply = broker.consumeMessage("reply-queue");
        assertThat(reply).isNotNull();
        assertThat(reply.getCorrelationId()).isEqualTo("req-123");
        assertThat(new String(reply.getBody())).isEqualTo("Result: 8");
    }

    @Test
    @DisplayName("Scenario: Dead Letter Queue")
    void testDeadLetterQueuePattern() {
        // Setup: Main queue and DLQ
        broker.declareExchange("main-exchange", Exchange.Type.DIRECT, true, false, false);
        broker.declareExchange("dlx", Exchange.Type.DIRECT, true, false, false);
        broker.declareQueue("main-queue", true, false, false);
        broker.declareQueue("dead-letter-queue", true, false, false);

        broker.bindQueue("main-queue", "main-exchange", "order");
        broker.bindQueue("dead-letter-queue", "dlx", "order");

        // Simulate message that fails processing multiple times
        Message message = new Message("Problematic order".getBytes());
        message.setDeliveryMode((short) 2);
        broker.publishMessage("main-exchange", "order", message);

        // Consume and simulate failure (would normally retry)
        Message consumed = broker.consumeMessage("main-queue");
        assertThat(consumed).isNotNull();

        // After max retries, send to DLQ
        broker.publishMessage("dlx", "order", consumed);

        // Verify in DLQ
        assertThat(broker.getQueue("dead-letter-queue").size()).isEqualTo(1);
        assertThat(broker.getQueue("main-queue").size()).isEqualTo(0);
    }

    @Test
    @DisplayName("Scenario: Priority Queue Simulation")
    void testPriorityQueuePattern() {
        broker.declareQueue("priority-queue", false, false, false);

        // Publish messages with different priorities
        for (int i = 0; i < 5; i++) {
            Message lowPriority = new Message(("Low priority " + i).getBytes());
            lowPriority.setPriority((short) 1);
            broker.getQueue("priority-queue").enqueue(lowPriority);
        }

        Message highPriority = new Message("High priority".getBytes());
        highPriority.setPriority((short) 10);
        broker.getQueue("priority-queue").enqueue(highPriority);

        // Note: Current implementation uses FIFO queue
        // This test demonstrates the structure; actual priority handling would require PriorityQueue
        assertThat(broker.getQueue("priority-queue").size()).isEqualTo(6);
    }

    @Test
    @DisplayName("Scenario: Message TTL Simulation")
    void testMessageTTLPattern() throws InterruptedException {
        broker.declareQueue("ttl-queue", false, false, false);

        Message message = new Message("Expiring message".getBytes());
        message.setExpiration("1000"); // 1 second TTL
        broker.getQueue("ttl-queue").enqueue(message);

        // Immediate consumption should work
        Message consumed = broker.consumeMessage("ttl-queue");
        assertThat(consumed).isNotNull();
        assertThat(consumed.getExpiration()).isEqualTo("1000");
    }

    @Test
    @DisplayName("Scenario: Multiple Exchanges and Complex Routing")
    void testComplexRoutingScenario() {
        // Setup multiple exchanges
        broker.declareExchange("events", Exchange.Type.TOPIC, true, false, false);
        broker.declareExchange("notifications", Exchange.Type.FANOUT, true, false, false);
        broker.declareExchange("priority", Exchange.Type.DIRECT, true, false, false);

        // Setup queues
        broker.declareQueue("user-events", true, false, false);
        broker.declareQueue("admin-notifications", true, false, false);
        broker.declareQueue("high-priority", true, false, false);

        // Bindings
        broker.bindQueue("user-events", "events", "user.#");
        broker.bindQueue("admin-notifications", "notifications", "");
        broker.bindQueue("high-priority", "priority", "urgent");

        // Publish to different exchanges
        broker.publishMessage("events", "user.login", new Message("User logged in".getBytes()));
        broker.publishMessage("notifications", "", new Message("System update".getBytes()));
        broker.publishMessage("priority", "urgent", new Message("Critical alert".getBytes()));

        // Verify routing
        assertThat(broker.getQueue("user-events").size()).isEqualTo(1);
        assertThat(broker.getQueue("admin-notifications").size()).isEqualTo(1);
        assertThat(broker.getQueue("high-priority").size()).isEqualTo(1);
    }

    @Test
    @DisplayName("Scenario: High Throughput Test")
    void testHighThroughput() throws InterruptedException, ExecutionException {
        broker.declareExchange("throughput-test", Exchange.Type.DIRECT, false, false, false);
        broker.declareQueue("throughput-queue", false, false, false);
        broker.bindQueue("throughput-queue", "throughput-test", "test");

        int messageCount = 10000;
        
        // Publisher thread
        ExecutorService publisherExecutor = Executors.newSingleThreadExecutor();
        Future<?> publisherFuture = publisherExecutor.submit(() -> {
            for (int i = 0; i < messageCount; i++) {
                Message message = new Message(("Message " + i).getBytes());
                broker.publishMessage("throughput-test", "test", message);
            }
        });

        // Wait for publishing to complete
        publisherFuture.get();
        publisherExecutor.shutdown();

        // Verify all messages in queue
        assertThat(broker.getQueue("throughput-queue").size()).isEqualTo(messageCount);

        // Consumer threads
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(4);
        CountDownLatch latch = new CountDownLatch(messageCount);

        for (int i = 0; i < 4; i++) {
            consumerExecutor.submit(() -> {
                while (true) {
                    Message message = broker.consumeMessage("throughput-queue");
                    if (message == null) break;
                    latch.countDown();
                }
            });
        }

        // Wait for all messages to be consumed
        boolean completed = latch.await(10, TimeUnit.SECONDS);
        consumerExecutor.shutdown();

        assertThat(completed).isTrue();
        assertThat(broker.getQueue("throughput-queue").size()).isEqualTo(0);
    }
}
