package com.amqp.queue;

import com.amqp.model.Message;
import com.amqp.model.QueueArguments;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for StreamQueue.
 */
class StreamQueueTest {

    private StreamQueue streamQueue;
    private QueueArguments arguments;

    @BeforeEach
    void setUp() {
        arguments = new QueueArguments();
        arguments.put("x-max-age", 60000L); // 1 minute
        arguments.put("x-max-bytes", 1024 * 1024L); // 1 MB
        streamQueue = new StreamQueue("test-stream", true, arguments);
    }

    @AfterEach
    void tearDown() {
        if (streamQueue != null) {
            streamQueue.shutdown();
        }
    }

    @Test
    void testAppendMessage() {
        Message message = new Message();
        message.setBody("test message".getBytes());

        streamQueue.enqueue(message);

        assertEquals(1, streamQueue.size());
        assertEquals(0, streamQueue.getFirstOffset());
        assertEquals(0, streamQueue.getLastOffset());
    }

    @Test
    void testConsumeFromOffset() {
        // Append messages
        for (int i = 0; i < 10; i++) {
            Message message = new Message();
            message.setBody(("message-" + i).getBytes());
            streamQueue.enqueue(message);
        }

        // Consume from offset 5
        Message consumed = streamQueue.consumeFrom("consumer-1", 5);
        assertNotNull(consumed);
        assertEquals("message-5", new String(consumed.getBody()));

        // Consumer offset should be updated
        assertEquals(6, streamQueue.getConsumerOffset("consumer-1"));
    }

    @Test
    void testConsumeNext() {
        // Append messages
        for (int i = 0; i < 5; i++) {
            Message message = new Message();
            message.setBody(("message-" + i).getBytes());
            streamQueue.enqueue(message);
        }

        // Consume sequentially
        Message msg1 = streamQueue.consumeNext("consumer-1");
        assertEquals("message-0", new String(msg1.getBody()));

        Message msg2 = streamQueue.consumeNext("consumer-1");
        assertEquals("message-1", new String(msg2.getBody()));

        assertEquals(2, streamQueue.getConsumerOffset("consumer-1"));
    }

    @Test
    void testConsumeBatch() {
        // Append messages
        for (int i = 0; i < 20; i++) {
            Message message = new Message();
            message.setBody(("message-" + i).getBytes());
            streamQueue.enqueue(message);
        }

        // Consume batch of 5 messages starting from offset 10
        List<Message> batch = streamQueue.consumeBatch("consumer-1", 10, 5);

        assertEquals(5, batch.size());
        assertEquals("message-10", new String(batch.get(0).getBody()));
        assertEquals("message-14", new String(batch.get(4).getBody()));
        assertEquals(15, streamQueue.getConsumerOffset("consumer-1"));
    }

    @Test
    void testSetConsumerOffset() {
        for (int i = 0; i < 10; i++) {
            Message message = new Message();
            message.setBody(("message-" + i).getBytes());
            streamQueue.enqueue(message);
        }

        streamQueue.setConsumerOffset("consumer-1", 5);
        assertEquals(5, streamQueue.getConsumerOffset("consumer-1"));

        Message consumed = streamQueue.consumeNext("consumer-1");
        assertEquals("message-5", new String(consumed.getBody()));
    }

    @Test
    void testResetConsumerOffset() {
        for (int i = 0; i < 10; i++) {
            Message message = new Message();
            message.setBody(("message-" + i).getBytes());
            streamQueue.enqueue(message);
        }

        streamQueue.setConsumerOffset("consumer-1", 5);
        streamQueue.resetConsumerOffset("consumer-1");

        assertEquals(0, streamQueue.getConsumerOffset("consumer-1"));

        Message consumed = streamQueue.consumeNext("consumer-1");
        assertEquals("message-0", new String(consumed.getBody()));
    }

    @Test
    void testMultipleConsumers() {
        for (int i = 0; i < 10; i++) {
            Message message = new Message();
            message.setBody(("message-" + i).getBytes());
            streamQueue.enqueue(message);
        }

        // Different consumers can have different offsets
        streamQueue.setConsumerOffset("consumer-1", 3);
        streamQueue.setConsumerOffset("consumer-2", 7);

        Message msg1 = streamQueue.consumeNext("consumer-1");
        Message msg2 = streamQueue.consumeNext("consumer-2");

        assertEquals("message-3", new String(msg1.getBody()));
        assertEquals("message-7", new String(msg2.getBody()));
    }

    @Test
    void testDequeueThrowsException() {
        assertThrows(UnsupportedOperationException.class, () -> {
            streamQueue.dequeue();
        });
    }

    @Test
    void testGetStats() {
        for (int i = 0; i < 5; i++) {
            Message message = new Message();
            message.setBody(("message-" + i).getBytes());
            streamQueue.enqueue(message);
        }

        streamQueue.setConsumerOffset("consumer-1", 2);
        streamQueue.setConsumerOffset("consumer-2", 3);

        StreamQueue.StreamStats stats = streamQueue.getStats();

        assertEquals("test-stream", stats.name);
        assertEquals(5, stats.messageCount);
        assertEquals(0, stats.firstOffset);
        assertEquals(4, stats.lastOffset);
        assertEquals(2, stats.consumerCount);
        assertTrue(stats.totalBytes > 0);
    }

    @Test
    void testPurge() {
        for (int i = 0; i < 10; i++) {
            Message message = new Message();
            message.setBody(("message-" + i).getBytes());
            streamQueue.enqueue(message);
        }

        streamQueue.setConsumerOffset("consumer-1", 5);

        streamQueue.purge();

        assertEquals(0, streamQueue.size());
        assertEquals(0, streamQueue.getTotalBytes());
        // Consumer offsets are also cleared
        assertEquals(0, streamQueue.getConsumerOffset("consumer-1"));
    }

    @Test
    void testInvalidOffsetThrowsException() {
        for (int i = 0; i < 5; i++) {
            Message message = new Message();
            message.setBody(("message-" + i).getBytes());
            streamQueue.enqueue(message);
        }

        assertThrows(IllegalArgumentException.class, () -> {
            streamQueue.setConsumerOffset("consumer-1", -1);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            streamQueue.setConsumerOffset("consumer-1", 100);
        });
    }
}
