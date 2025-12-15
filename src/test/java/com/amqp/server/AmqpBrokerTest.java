package com.amqp.server;

import com.amqp.model.Exchange;
import com.amqp.model.Message;
import com.amqp.model.Queue;
import com.amqp.persistence.PersistenceManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("AMQP Broker Tests")
class AmqpBrokerTest {

    @Mock
    private PersistenceManager persistenceManager;

    private AmqpBroker broker;

    @BeforeEach
    void setUp() {
        // Enable guest user for testing purposes
        broker = new AmqpBroker(persistenceManager, true);
    }

    @Nested
    @DisplayName("Default Exchange Tests")
    class DefaultExchangeTests {

        @Test
        @DisplayName("Should initialize with default exchanges")
        void testDefaultExchangesCreated() {
            assertThat(broker.getExchange("")).isNotNull();
            assertThat(broker.getExchange("amq.direct")).isNotNull();
            assertThat(broker.getExchange("amq.topic")).isNotNull();
            assertThat(broker.getExchange("amq.fanout")).isNotNull();
            assertThat(broker.getExchange("amq.headers")).isNotNull();
        }

        @Test
        @DisplayName("Default exchanges should have correct types")
        void testDefaultExchangeTypes() {
            assertThat(broker.getExchange("").getType()).isEqualTo(Exchange.Type.DIRECT);
            assertThat(broker.getExchange("amq.direct").getType()).isEqualTo(Exchange.Type.DIRECT);
            assertThat(broker.getExchange("amq.topic").getType()).isEqualTo(Exchange.Type.TOPIC);
            assertThat(broker.getExchange("amq.fanout").getType()).isEqualTo(Exchange.Type.FANOUT);
            assertThat(broker.getExchange("amq.headers").getType()).isEqualTo(Exchange.Type.HEADERS);
        }
    }

    @Nested
    @DisplayName("Exchange Declaration Tests")
    class ExchangeDeclarationTests {

        @Test
        @DisplayName("Should declare new exchange")
        void testDeclareNewExchange() {
            Exchange exchange = broker.declareExchange("test-exchange", Exchange.Type.DIRECT, 
                                                       true, false, false);

            assertThat(exchange.getName()).isEqualTo("test-exchange");
            assertThat(exchange.getType()).isEqualTo(Exchange.Type.DIRECT);
            assertThat(exchange.isDurable()).isTrue();
            assertThat(broker.getExchange("test-exchange")).isEqualTo(exchange);
        }

        @Test
        @DisplayName("Should persist durable exchange")
        void testDeclareExchangePersistence() {
            broker.declareExchange("durable-exchange", Exchange.Type.TOPIC, 
                                  true, false, false);

            verify(persistenceManager).saveExchange(any(Exchange.class));
        }

        @Test
        @DisplayName("Should not persist non-durable exchange")
        void testDeclareNonDurableExchangeNoPersistence() {
            broker.declareExchange("temp-exchange", Exchange.Type.FANOUT, 
                                  false, true, false);

            verify(persistenceManager, never()).saveExchange(any(Exchange.class));
        }

        @Test
        @DisplayName("Should return existing exchange with same parameters")
        void testDeclareExistingExchangeSameParameters() {
            Exchange first = broker.declareExchange("test-exchange", Exchange.Type.DIRECT, 
                                                    true, false, false);
            Exchange second = broker.declareExchange("test-exchange", Exchange.Type.DIRECT, 
                                                     true, false, false);

            assertThat(second).isSameAs(first);
        }

        @Test
        @DisplayName("Should throw exception when declaring exchange with different parameters")
        void testDeclareExistingExchangeDifferentParameters() {
            broker.declareExchange("test-exchange", Exchange.Type.DIRECT, 
                                  true, false, false);

            assertThatThrownBy(() -> 
                broker.declareExchange("test-exchange", Exchange.Type.TOPIC, 
                                      true, false, false)
            ).isInstanceOf(IllegalArgumentException.class)
             .hasMessageContaining("already exists with different parameters");
        }
    }

    @Nested
    @DisplayName("Queue Declaration Tests")
    class QueueDeclarationTests {

        @Test
        @DisplayName("Should declare new queue")
        void testDeclareNewQueue() {
            Queue queue = broker.declareQueue("test-queue", true, false, false);

            assertThat(queue.getName()).isEqualTo("test-queue");
            assertThat(queue.isDurable()).isTrue();
            assertThat(broker.getQueue("test-queue")).isEqualTo(queue);
        }

        @Test
        @DisplayName("Should generate queue name when empty")
        void testDeclareQueueGenerateName() {
            Queue queue = broker.declareQueue("", false, false, false);

            assertThat(queue.getName()).startsWith("amq.gen-");
            assertThat(broker.getQueue(queue.getName())).isEqualTo(queue);
        }

        @Test
        @DisplayName("Should generate queue name when null")
        void testDeclareQueueGenerateNameNull() {
            Queue queue = broker.declareQueue(null, false, false, false);

            assertThat(queue.getName()).startsWith("amq.gen-");
        }

        @Test
        @DisplayName("Should persist durable queue")
        void testDeclareQueuePersistence() {
            broker.declareQueue("durable-queue", true, false, false);

            verify(persistenceManager).saveQueue(any(Queue.class));
        }

        @Test
        @DisplayName("Should not persist non-durable queue")
        void testDeclareNonDurableQueueNoPersistence() {
            broker.declareQueue("temp-queue", false, false, true);

            verify(persistenceManager, never()).saveQueue(any(Queue.class));
        }

        @Test
        @DisplayName("Should return existing queue with same parameters")
        void testDeclareExistingQueueSameParameters() {
            Queue first = broker.declareQueue("test-queue", true, false, false);
            Queue second = broker.declareQueue("test-queue", true, false, false);

            assertThat(second).isSameAs(first);
        }

        @Test
        @DisplayName("Should throw exception when declaring queue with different parameters")
        void testDeclareExistingQueueDifferentParameters() {
            broker.declareQueue("test-queue", true, false, false);

            assertThatThrownBy(() -> 
                broker.declareQueue("test-queue", false, false, false)
            ).isInstanceOf(IllegalArgumentException.class)
             .hasMessageContaining("already exists with different parameters");
        }

        @Test
        @DisplayName("Should declare exclusive queue")
        void testDeclareExclusiveQueue() {
            Queue queue = broker.declareQueue("exclusive-queue", false, true, false);

            assertThat(queue.isExclusive()).isTrue();
        }
    }

    @Nested
    @DisplayName("Queue Binding Tests")
    class QueueBindingTests {

        @Test
        @DisplayName("Should bind queue to exchange")
        void testBindQueue() {
            broker.declareExchange("test-exchange", Exchange.Type.DIRECT, true, false, false);
            broker.declareQueue("test-queue", true, false, false);

            broker.bindQueue("test-queue", "test-exchange", "test.key");

            verify(persistenceManager).saveBinding("test-exchange", "test-queue", "test.key");
        }

        @Test
        @DisplayName("Should throw exception when binding to non-existent queue")
        void testBindNonExistentQueue() {
            broker.declareExchange("test-exchange", Exchange.Type.DIRECT, true, false, false);

            assertThatThrownBy(() -> 
                broker.bindQueue("non-existent-queue", "test-exchange", "test.key")
            ).isInstanceOf(IllegalArgumentException.class)
             .hasMessageContaining("Queue not found");
        }

        @Test
        @DisplayName("Should throw exception when binding to non-existent exchange")
        void testBindToNonExistentExchange() {
            broker.declareQueue("test-queue", true, false, false);

            assertThatThrownBy(() -> 
                broker.bindQueue("test-queue", "non-existent-exchange", "test.key")
            ).isInstanceOf(IllegalArgumentException.class)
             .hasMessageContaining("Exchange not found");
        }
    }

    @Nested
    @DisplayName("Message Publishing Tests")
    class MessagePublishingTests {

        @Test
        @DisplayName("Should publish message to direct exchange")
        void testPublishToDirect() {
            broker.declareExchange("test-exchange", Exchange.Type.DIRECT, true, false, false);
            broker.declareQueue("test-queue", true, false, false);
            broker.bindQueue("test-queue", "test-exchange", "test.key");

            Message message = new Message("Hello".getBytes());
            broker.publishMessage("test-exchange", "test.key", message);

            Queue queue = broker.getQueue("test-queue");
            Message received = queue.dequeue();

            assertThat(received).isEqualTo(message);
        }

        @Test
        @DisplayName("Should publish message to multiple queues")
        void testPublishToMultipleQueues() {
            broker.declareExchange("test-exchange", Exchange.Type.FANOUT, true, false, false);
            broker.declareQueue("queue1", true, false, false);
            broker.declareQueue("queue2", true, false, false);
            broker.declareQueue("queue3", true, false, false);
            
            broker.bindQueue("queue1", "test-exchange", "");
            broker.bindQueue("queue2", "test-exchange", "");
            broker.bindQueue("queue3", "test-exchange", "");

            Message message = new Message("Broadcast".getBytes());
            broker.publishMessage("test-exchange", "", message);

            assertThat(broker.getQueue("queue1").dequeue()).isEqualTo(message);
            assertThat(broker.getQueue("queue2").dequeue()).isEqualTo(message);
            assertThat(broker.getQueue("queue3").dequeue()).isEqualTo(message);
        }

        @Test
        @DisplayName("Should persist message when queue and message are durable")
        void testPublishPersistentMessage() {
            broker.declareExchange("test-exchange", Exchange.Type.DIRECT, true, false, false);
            broker.declareQueue("durable-queue", true, false, false);
            broker.bindQueue("durable-queue", "test-exchange", "test.key");

            Message message = new Message("Persistent".getBytes());
            message.setDeliveryMode((short) 2); // Persistent

            broker.publishMessage("test-exchange", "test.key", message);

            verify(persistenceManager).saveMessage(eq("durable-queue"), any(Message.class));
        }

        @Test
        @DisplayName("Should not persist non-durable message")
        void testPublishNonPersistentMessage() {
            broker.declareExchange("test-exchange", Exchange.Type.DIRECT, true, false, false);
            broker.declareQueue("durable-queue", true, false, false);
            broker.bindQueue("durable-queue", "test-exchange", "test.key");

            Message message = new Message("Non-persistent".getBytes());
            message.setDeliveryMode((short) 1); // Non-persistent

            broker.publishMessage("test-exchange", "test.key", message);

            verify(persistenceManager, never()).saveMessage(anyString(), any(Message.class));
        }

        @Test
        @DisplayName("Should handle publishing to non-existent exchange gracefully")
        void testPublishToNonExistentExchange() {
            Message message = new Message("Hello".getBytes());
            
            // Should not throw exception
            assertThatCode(() -> 
                broker.publishMessage("non-existent", "test.key", message)
            ).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should publish to topic exchange with wildcard routing")
        void testPublishToTopicExchange() {
            broker.declareExchange("topic-exchange", Exchange.Type.TOPIC, true, false, false);
            broker.declareQueue("all-orders", true, false, false);
            broker.declareQueue("critical-messages", true, false, false);
            
            broker.bindQueue("all-orders", "topic-exchange", "orders.#");
            broker.bindQueue("critical-messages", "topic-exchange", "*.critical");

            Message message1 = new Message("Order 1".getBytes());
            broker.publishMessage("topic-exchange", "orders.new", message1);

            Message message2 = new Message("Critical".getBytes());
            broker.publishMessage("topic-exchange", "orders.critical", message2);

            assertThat(broker.getQueue("all-orders").size()).isEqualTo(2);
            assertThat(broker.getQueue("critical-messages").size()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("Message Consumption Tests")
    class MessageConsumptionTests {

        @Test
        @DisplayName("Should consume message from queue")
        void testConsumeMessage() {
            broker.declareQueue("test-queue", true, false, false);
            
            Message message = new Message("Hello".getBytes());
            broker.getQueue("test-queue").enqueue(message);

            Message consumed = broker.consumeMessage("test-queue");

            assertThat(consumed).isEqualTo(message);
        }

        @Test
        @DisplayName("Should return null when consuming from empty queue")
        void testConsumeFromEmptyQueue() {
            broker.declareQueue("test-queue", true, false, false);

            Message consumed = broker.consumeMessage("test-queue");

            assertThat(consumed).isNull();
        }

        @Test
        @DisplayName("Should return null when consuming from non-existent queue")
        void testConsumeFromNonExistentQueue() {
            Message consumed = broker.consumeMessage("non-existent");

            assertThat(consumed).isNull();
        }

        @Test
        @DisplayName("Should delete persistent message after consumption")
        void testConsumePersistentMessageDeletion() {
            broker.declareQueue("durable-queue", true, false, false);
            
            Message message = new Message("Persistent".getBytes());
            message.setId(123L);
            message.setDeliveryMode((short) 2);
            
            broker.getQueue("durable-queue").enqueue(message);
            broker.consumeMessage("durable-queue");

            verify(persistenceManager).deleteMessage(123L);
        }

        @Test
        @DisplayName("Should not delete non-persistent message after consumption")
        void testConsumeNonPersistentMessageNoDeletion() {
            broker.declareQueue("test-queue", false, false, false);
            
            Message message = new Message("Non-persistent".getBytes());
            message.setDeliveryMode((short) 1);
            
            broker.getQueue("test-queue").enqueue(message);
            broker.consumeMessage("test-queue");

            verify(persistenceManager, never()).deleteMessage(anyLong());
        }
    }

    @Nested
    @DisplayName("Integration Tests")
    class IntegrationTests {

        @Test
        @DisplayName("Should handle complete publish-consume workflow")
        void testCompleteWorkflow() {
            // Setup
            broker.declareExchange("orders-exchange", Exchange.Type.DIRECT, true, false, false);
            broker.declareQueue("orders-queue", true, false, false);
            broker.bindQueue("orders-queue", "orders-exchange", "order.created");

            // Publish
            Message message = new Message("Order #123".getBytes());
            message.setRoutingKey("order.created");
            message.setContentType("application/json");
            broker.publishMessage("orders-exchange", "order.created", message);

            // Consume
            Message consumed = broker.consumeMessage("orders-queue");

            assertThat(consumed).isNotNull();
            assertThat(consumed.getBody()).isEqualTo("Order #123".getBytes());
            assertThat(consumed.getContentType()).isEqualTo("application/json");
        }

        @Test
        @DisplayName("Should handle multiple publishers and consumers")
        void testMultiplePublishersConsumers() {
            broker.declareExchange("notifications", Exchange.Type.FANOUT, false, false, false);
            broker.declareQueue("email-queue", false, false, false);
            broker.declareQueue("sms-queue", false, false, false);
            broker.declareQueue("push-queue", false, false, false);
            
            broker.bindQueue("email-queue", "notifications", "");
            broker.bindQueue("sms-queue", "notifications", "");
            broker.bindQueue("push-queue", "notifications", "");

            // Publish
            Message notification = new Message("New message!".getBytes());
            broker.publishMessage("notifications", "", notification);

            // Consume from all queues
            assertThat(broker.consumeMessage("email-queue")).isNotNull();
            assertThat(broker.consumeMessage("sms-queue")).isNotNull();
            assertThat(broker.consumeMessage("push-queue")).isNotNull();
        }
    }
}
