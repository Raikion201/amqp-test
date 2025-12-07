package com.amqp.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;

import static org.assertj.core.api.Assertions.*;

class EnhancedQueueTest {

    private EnhancedQueue queue;

    @AfterEach
    void tearDown() {
        if (queue != null) {
            queue.shutdown();
        }
    }

    @Test
    void testBasicEnqueue() {
        QueueArguments args = new QueueArguments();
        queue = new EnhancedQueue("test", false, false, false, args);

        Message msg = new Message("Hello".getBytes());
        queue.enqueue(msg);

        assertThat(queue.size()).isEqualTo(1);
    }

    @Test
    void testMaxLength() {
        QueueArguments args = new QueueArguments();
        args.setMaxLength(3);
        args.setOverflowBehavior("drop-head");
        queue = new EnhancedQueue("test", false, false, false, args);

        // Enqueue 4 messages, oldest should be dropped
        for (int i = 0; i < 4; i++) {
            Message msg = new Message(("Message " + i).getBytes());
            queue.enqueue(msg);
        }

        assertThat(queue.size()).isEqualTo(3);
    }

    @Test
    void testMaxLengthRejectPublish() {
        QueueArguments args = new QueueArguments();
        args.setMaxLength(2);
        args.setOverflowBehavior("reject-publish");
        queue = new EnhancedQueue("test", false, false, false, args);

        queue.enqueue(new Message("Msg1".getBytes()));
        queue.enqueue(new Message("Msg2".getBytes()));

        assertThatThrownBy(() -> queue.enqueue(new Message("Msg3".getBytes())))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("overflow");
    }

    @Test
    void testPriorityQueue() {
        QueueArguments args = new QueueArguments();
        args.setMaxPriority(10);
        queue = new EnhancedQueue("test", false, false, false, args);

        Message lowPriority = new Message("Low".getBytes());
        lowPriority.setPriority((short) 1);

        Message highPriority = new Message("High".getBytes());
        highPriority.setPriority((short) 9);

        Message midPriority = new Message("Mid".getBytes());
        midPriority.setPriority((short) 5);

        queue.enqueue(lowPriority);
        queue.enqueue(highPriority);
        queue.enqueue(midPriority);

        // Should dequeue in priority order: high, mid, low
        Message first = queue.dequeue();
        assertThat(new String(first.getBody())).isEqualTo("High");

        Message second = queue.dequeue();
        assertThat(new String(second.getBody())).isEqualTo("Mid");

        Message third = queue.dequeue();
        assertThat(new String(third.getBody())).isEqualTo("Low");
    }

    @Test
    void testMessageTTL() throws InterruptedException {
        QueueArguments args = new QueueArguments();
        args.setMessageTtl(100L); // 100ms TTL
        queue = new EnhancedQueue("test", false, false, false, args);

        Message msg = new Message("Expiring".getBytes());
        queue.enqueue(msg);

        // Wait for message to expire
        Thread.sleep(200);

        Message dequeued = queue.dequeue();
        // Message should have been expired and skipped
        assertThat(dequeued).isNull();
    }

    @Test
    void testDeadLetterExchange() {
        QueueArguments args = new QueueArguments();
        args.setDeadLetterExchange("dlx");
        args.setDeadLetterRoutingKey("dlx.key");
        args.setMaxLength(1);
        args.setOverflowBehavior("drop-head");
        queue = new EnhancedQueue("test", false, false, false, args);

        Message msg1 = new Message("First".getBytes());
        Message msg2 = new Message("Second".getBytes());

        queue.enqueue(msg1);
        queue.enqueue(msg2); // This should cause msg1 to be dead-lettered

        assertThat(queue.getAndClearDeadLetterQueue()).hasSize(1);
    }

    @Test
    void testQueueArguments() {
        QueueArguments args = new QueueArguments();
        args.setMaxLength(100);
        args.setMaxPriority(10);
        args.setMessageTtl(5000L);
        args.setDeadLetterExchange("dlx");

        assertThat(args.hasMaxLength()).isTrue();
        assertThat(args.isPriorityQueue()).isTrue();
        assertThat(args.hasTTL()).isTrue();
        assertThat(args.hasDLX()).isTrue();
    }

    @Test
    void testMaxPriorityValidation() {
        QueueArguments args = new QueueArguments();

        assertThatThrownBy(() -> args.setMaxPriority(256))
            .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> args.setMaxPriority(-1))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testQueueExpiry() throws InterruptedException {
        QueueArguments args = new QueueArguments();
        args.setExpires(100L); // Queue expires after 100ms of inactivity
        queue = new EnhancedQueue("test", false, false, false, args);

        Message msg = new Message("Test".getBytes());
        queue.enqueue(msg);

        Thread.sleep(150);

        assertThat(queue.hasExpired()).isTrue();
    }

    @Test
    void testBytesSizeTracking() {
        QueueArguments args = new QueueArguments();
        args.setMaxLengthBytes(1000L);
        queue = new EnhancedQueue("test", false, false, false, args);

        Message msg = new Message(new byte[100]);
        queue.enqueue(msg);

        assertThat(queue.getTotalBytesSize()).isGreaterThan(0);
    }
}
