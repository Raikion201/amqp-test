package com.amqp.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.*;

@DisplayName("Queue Tests")
class QueueTest {

    private Queue queue;

    @BeforeEach
    void setUp() {
        queue = new Queue("test-queue", true, false, false);
    }

    @Test
    @DisplayName("Should create queue with correct properties")
    void testQueueCreation() {
        assertThat(queue.getName()).isEqualTo("test-queue");
        assertThat(queue.isDurable()).isTrue();
        assertThat(queue.isExclusive()).isFalse();
        assertThat(queue.isAutoDelete()).isFalse();
        assertThat(queue.isEmpty()).isTrue();
        assertThat(queue.size()).isEqualTo(0);
    }

    @Test
    @DisplayName("Should enqueue and dequeue messages in FIFO order")
    void testEnqueueDequeueFIFO() {
        Message msg1 = new Message("message1".getBytes());
        Message msg2 = new Message("message2".getBytes());
        Message msg3 = new Message("message3".getBytes());

        queue.enqueue(msg1);
        queue.enqueue(msg2);
        queue.enqueue(msg3);

        assertThat(queue.size()).isEqualTo(3);
        assertThat(queue.isEmpty()).isFalse();

        assertThat(queue.dequeue()).isEqualTo(msg1);
        assertThat(queue.dequeue()).isEqualTo(msg2);
        assertThat(queue.dequeue()).isEqualTo(msg3);
        assertThat(queue.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("Should return null when dequeuing from empty queue")
    void testDequeueFromEmptyQueue() {
        assertThat(queue.dequeue()).isNull();
    }

    @Test
    @DisplayName("Should purge all messages from queue")
    void testPurgeQueue() {
        queue.enqueue(new Message("msg1".getBytes()));
        queue.enqueue(new Message("msg2".getBytes()));
        queue.enqueue(new Message("msg3".getBytes()));

        assertThat(queue.size()).isEqualTo(3);

        queue.purge();

        assertThat(queue.isEmpty()).isTrue();
        assertThat(queue.size()).isEqualTo(0);
    }

    @Test
    @DisplayName("Should create non-durable queue")
    void testNonDurableQueue() {
        Queue nonDurable = new Queue("temp-queue", false, false, true);
        
        assertThat(nonDurable.getName()).isEqualTo("temp-queue");
        assertThat(nonDurable.isDurable()).isFalse();
        assertThat(nonDurable.isAutoDelete()).isTrue();
    }

    @Test
    @DisplayName("Should create exclusive queue")
    void testExclusiveQueue() {
        Queue exclusive = new Queue("exclusive-queue", false, true, false);
        
        assertThat(exclusive.isExclusive()).isTrue();
    }

    @Test
    @DisplayName("Should handle multiple concurrent enqueues")
    void testConcurrentEnqueue() throws InterruptedException {
        int threadCount = 10;
        int messagesPerThread = 100;
        Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < messagesPerThread; j++) {
                    Message msg = new Message(("Thread" + threadId + "-Msg" + j).getBytes());
                    queue.enqueue(msg);
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertThat(queue.size()).isEqualTo(threadCount * messagesPerThread);
    }
}
