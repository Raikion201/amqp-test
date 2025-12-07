package com.amqp.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

@DisplayName("Exchange Tests")
class ExchangeTest {

    @Nested
    @DisplayName("Direct Exchange Tests")
    class DirectExchangeTests {

        @Test
        @DisplayName("Should route to queue with exact routing key match")
        void testDirectRoutingExactMatch() {
            Exchange exchange = new Exchange("test.direct", Exchange.Type.DIRECT, true, false, false);
            
            exchange.addBinding("orders.new", "queue1");
            exchange.addBinding("orders.cancelled", "queue2");

            List<String> routes = exchange.route("orders.new");

            assertThat(routes).containsExactly("queue1");
        }

        @Test
        @DisplayName("Should route to multiple queues with same routing key")
        void testDirectRoutingMultipleQueues() {
            Exchange exchange = new Exchange("test.direct", Exchange.Type.DIRECT, true, false, false);
            
            exchange.addBinding("orders.new", "queue1");
            exchange.addBinding("orders.new", "queue2");
            exchange.addBinding("orders.new", "queue3");

            List<String> routes = exchange.route("orders.new");

            assertThat(routes).containsExactlyInAnyOrder("queue1", "queue2", "queue3");
        }

        @Test
        @DisplayName("Should return empty list when no matching routing key")
        void testDirectRoutingNoMatch() {
            Exchange exchange = new Exchange("test.direct", Exchange.Type.DIRECT, true, false, false);
            
            exchange.addBinding("orders.new", "queue1");

            List<String> routes = exchange.route("orders.cancelled");

            assertThat(routes).isEmpty();
        }

        @Test
        @DisplayName("Should remove binding")
        void testRemoveBinding() {
            Exchange exchange = new Exchange("test.direct", Exchange.Type.DIRECT, true, false, false);
            
            exchange.addBinding("orders.new", "queue1");
            exchange.removeBinding("orders.new", "queue1");

            List<String> routes = exchange.route("orders.new");

            assertThat(routes).isEmpty();
        }
    }

    @Nested
    @DisplayName("Fanout Exchange Tests")
    class FanoutExchangeTests {

        @Test
        @DisplayName("Should route to all bound queues regardless of routing key")
        void testFanoutRoutingIgnoresRoutingKey() {
            Exchange exchange = new Exchange("test.fanout", Exchange.Type.FANOUT, true, false, false);
            
            exchange.addBinding("", "queue1");
            exchange.addBinding("any.key", "queue2");
            exchange.addBinding("another.key", "queue3");

            List<String> routes = exchange.route("some.random.key");

            assertThat(routes).containsExactlyInAnyOrder("queue1", "queue2", "queue3");
        }

        @Test
        @DisplayName("Should return empty list when no queues bound")
        void testFanoutNoQueues() {
            Exchange exchange = new Exchange("test.fanout", Exchange.Type.FANOUT, true, false, false);

            List<String> routes = exchange.route("any.key");

            assertThat(routes).isEmpty();
        }
    }

    @Nested
    @DisplayName("Topic Exchange Tests")
    class TopicExchangeTests {

        @Test
        @DisplayName("Should route with star wildcard matching single word")
        void testTopicRoutingStarWildcard() {
            Exchange exchange = new Exchange("test.topic", Exchange.Type.TOPIC, true, false, false);
            
            exchange.addBinding("orders.*.created", "queue1");

            List<String> routesMatch = exchange.route("orders.new.created");
            List<String> routesNoMatch = exchange.route("orders.new.updated");

            assertThat(routesMatch).containsExactly("queue1");
            assertThat(routesNoMatch).isEmpty();
        }

        @Test
        @DisplayName("Should route with hash wildcard matching multiple words")
        void testTopicRoutingHashWildcard() {
            Exchange exchange = new Exchange("test.topic", Exchange.Type.TOPIC, true, false, false);
            
            exchange.addBinding("orders.#", "queue1");

            assertThat(exchange.route("orders.new")).containsExactly("queue1");
            assertThat(exchange.route("orders.new.created")).containsExactly("queue1");
            assertThat(exchange.route("orders.new.created.urgent")).containsExactly("queue1");
            assertThat(exchange.route("payments.new")).isEmpty();
        }

        @Test
        @DisplayName("Should route with hash wildcard at beginning")
        void testTopicRoutingHashAtBeginning() {
            Exchange exchange = new Exchange("test.topic", Exchange.Type.TOPIC, true, false, false);
            
            exchange.addBinding("#.critical", "queue1");

            assertThat(exchange.route("orders.critical")).containsExactly("queue1");
            assertThat(exchange.route("orders.new.critical")).containsExactly("queue1");
            assertThat(exchange.route("critical")).containsExactly("queue1");
        }

        @Test
        @DisplayName("Should route with hash wildcard matching everything")
        void testTopicRoutingHashMatchAll() {
            Exchange exchange = new Exchange("test.topic", Exchange.Type.TOPIC, true, false, false);
            
            exchange.addBinding("#", "queue1");

            assertThat(exchange.route("anything")).containsExactly("queue1");
            assertThat(exchange.route("any.thing")).containsExactly("queue1");
            assertThat(exchange.route("any.thing.at.all")).containsExactly("queue1");
        }

        @Test
        @DisplayName("Should route with multiple wildcards")
        void testTopicRoutingMultipleWildcards() {
            Exchange exchange = new Exchange("test.topic", Exchange.Type.TOPIC, true, false, false);
            
            exchange.addBinding("*.orders.*.created", "queue1");

            assertThat(exchange.route("us.orders.new.created")).containsExactly("queue1");
            assertThat(exchange.route("eu.orders.bulk.created")).containsExactly("queue1");
            assertThat(exchange.route("us.orders.created")).isEmpty();
        }

        @Test
        @DisplayName("Should route with complex patterns")
        void testTopicRoutingComplexPatterns() {
            Exchange exchange = new Exchange("test.topic", Exchange.Type.TOPIC, true, false, false);
            
            exchange.addBinding("*.critical", "critical-queue");
            exchange.addBinding("orders.#", "orders-queue");
            exchange.addBinding("*.*.error", "error-queue");

            assertThat(exchange.route("orders.critical")).containsExactlyInAnyOrder("critical-queue", "orders-queue");
            assertThat(exchange.route("orders.new.error")).containsExactlyInAnyOrder("orders-queue", "error-queue");
            assertThat(exchange.route("payments.critical")).containsExactly("critical-queue");
        }

        @Test
        @DisplayName("Should match exact routing key without wildcards")
        void testTopicRoutingExactMatch() {
            Exchange exchange = new Exchange("test.topic", Exchange.Type.TOPIC, true, false, false);
            
            exchange.addBinding("orders.new.created", "queue1");

            assertThat(exchange.route("orders.new.created")).containsExactly("queue1");
            assertThat(exchange.route("orders.new.updated")).isEmpty();
        }
    }

    @Test
    @DisplayName("Should create exchange with correct properties")
    void testExchangeCreation() {
        Exchange exchange = new Exchange("test-exchange", Exchange.Type.DIRECT, true, false, true);

        assertThat(exchange.getName()).isEqualTo("test-exchange");
        assertThat(exchange.getType()).isEqualTo(Exchange.Type.DIRECT);
        assertThat(exchange.isDurable()).isTrue();
        assertThat(exchange.isAutoDelete()).isFalse();
        assertThat(exchange.isInternal()).isTrue();
    }

    @Test
    @DisplayName("Should create non-durable exchange")
    void testNonDurableExchange() {
        Exchange exchange = new Exchange("temp-exchange", Exchange.Type.FANOUT, false, true, false);

        assertThat(exchange.isDurable()).isFalse();
        assertThat(exchange.isAutoDelete()).isTrue();
    }
}
