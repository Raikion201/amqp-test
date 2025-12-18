package com.amqp.consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

class ConsumerManagerTest {

    private ConsumerManager manager;

    @BeforeEach
    void setUp() {
        manager = new ConsumerManager();
    }

    @Test
    void testAddConsumer() {
        ConsumerManager.Consumer consumer = manager.addConsumer(
            "ctag-1", "test-queue", (short) 1, false, false, null, null
        );

        assertThat(consumer).isNotNull();
        assertThat(consumer.getConsumerTag()).isEqualTo("ctag-1");
        assertThat(consumer.getQueueName()).isEqualTo("test-queue");
        assertThat(consumer.getChannelNumber()).isEqualTo((short) 1);
        assertThat(consumer.isActive()).isTrue();
    }

    @Test
    void testRemoveConsumer() {
        manager.addConsumer("ctag-1", "test-queue", (short) 1, false, false, null, null);

        ConsumerManager.Consumer removed = manager.removeConsumer("ctag-1");

        assertThat(removed).isNotNull();
        assertThat(manager.getConsumer("ctag-1")).isNull();
    }

    @Test
    void testGetConsumersForQueue() {
        manager.addConsumer("ctag-1", "queue-1", (short) 1, false, false, null, null);
        manager.addConsumer("ctag-2", "queue-1", (short) 1, false, false, null, null);
        manager.addConsumer("ctag-3", "queue-2", (short) 1, false, false, null, null);

        Set<ConsumerManager.Consumer> consumers = manager.getConsumersForQueue("queue-1");

        assertThat(consumers).hasSize(2);
        assertThat(consumers).extracting("consumerTag").containsExactlyInAnyOrder("ctag-1", "ctag-2");
    }

    @Test
    void testGetConsumersForChannel() {
        manager.addConsumer("ctag-1", "queue-1", (short) 1, false, false, null, null);
        manager.addConsumer("ctag-2", "queue-2", (short) 1, false, false, null, null);
        manager.addConsumer("ctag-3", "queue-3", (short) 2, false, false, null, null);

        Set<ConsumerManager.Consumer> consumers = manager.getConsumersForChannel((short) 1);

        assertThat(consumers).hasSize(2);
        assertThat(consumers).extracting("consumerTag").containsExactlyInAnyOrder("ctag-1", "ctag-2");
    }

    @Test
    void testExclusiveConsumer() {
        manager.addConsumer("ctag-1", "queue-1", (short) 1, false, true, null, null);

        assertThatThrownBy(() ->
            manager.addConsumer("ctag-2", "queue-1", (short) 1, false, false, null, null)
        ).isInstanceOf(IllegalStateException.class)
         .hasMessageContaining("exclusive");
    }

    @Test
    void testCannotAddExclusiveWhenOthersExist() {
        manager.addConsumer("ctag-1", "queue-1", (short) 1, false, false, null, null);

        assertThatThrownBy(() ->
            manager.addConsumer("ctag-2", "queue-1", (short) 1, false, true, null, null)
        ).isInstanceOf(IllegalStateException.class)
         .hasMessageContaining("exclusive");
    }

    @Test
    void testCancelConsumersForQueue() {
        manager.addConsumer("ctag-1", "queue-1", (short) 1, false, false, null, null);
        manager.addConsumer("ctag-2", "queue-1", (short) 1, false, false, null, null);
        manager.addConsumer("ctag-3", "queue-2", (short) 1, false, false, null, null);

        AtomicInteger cancelCount = new AtomicInteger(0);
        manager.cancelConsumersForQueue("queue-1",
            (consumer, reason) -> cancelCount.incrementAndGet());

        assertThat(cancelCount.get()).isEqualTo(2);
        assertThat(manager.getConsumerCountForQueue("queue-1")).isEqualTo(0);
        assertThat(manager.getConsumerCountForQueue("queue-2")).isEqualTo(1);
    }

    @Test
    void testCancelConsumersForChannel() {
        manager.addConsumer("ctag-1", "queue-1", (short) 1, false, false, null, null);
        manager.addConsumer("ctag-2", "queue-2", (short) 1, false, false, null, null);
        manager.addConsumer("ctag-3", "queue-3", (short) 2, false, false, null, null);

        manager.cancelConsumersForChannel((short) 1);

        assertThat(manager.getConsumerCount()).isEqualTo(1);
        assertThat(manager.getConsumer("ctag-3")).isNotNull();
    }

    @Test
    void testGetConsumerCount() {
        assertThat(manager.getConsumerCount()).isEqualTo(0);

        manager.addConsumer("ctag-1", "queue-1", (short) 1, false, false, null, null);
        manager.addConsumer("ctag-2", "queue-2", (short) 1, false, false, null, null);

        assertThat(manager.getConsumerCount()).isEqualTo(2);
    }

    @Test
    void testConsumerArguments() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-priority", 10);
        args.put("x-cancel-on-ha-failover", true);

        ConsumerManager.Consumer consumer = manager.addConsumer(
            "ctag-1", "queue-1", (short) 1, false, false, args, null
        );

        Map<String, Object> retrievedArgs = consumer.getArguments();
        assertThat(retrievedArgs).containsEntry("x-priority", 10);
        assertThat(retrievedArgs).containsEntry("x-cancel-on-ha-failover", true);
    }
}
