package com.amqp;

import com.amqp.amqp.AmqpCodec;
import com.amqp.model.Message;
import com.amqp.model.Queue;
import com.amqp.persistence.DatabaseManager;
import com.amqp.persistence.PersistenceManager;
import com.amqp.security.User;
import com.amqp.server.AmqpBroker;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.*;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration test for message headers flow through the broker.
 */
public class HeadersIntegrationTest {

    private AmqpBroker broker;
    private DatabaseManager dbManager;
    private PersistenceManager persistenceManager;

    @BeforeEach
    void setUp() throws Exception {
        // Use in-memory H2 database
        String dbUrl = "jdbc:h2:mem:test_" + System.currentTimeMillis();
        dbManager = new DatabaseManager(dbUrl, "SA", "");
        persistenceManager = new PersistenceManager(dbManager);
        broker = new AmqpBroker(persistenceManager, true); // Enable guest user
        broker.start();
    }

    @AfterEach
    void tearDown() {
        if (broker != null) {
            broker.stop();
        }
    }

    @Test
    @DisplayName("Message headers should survive publish and queue storage")
    void testHeadersSurvivePublish() throws Exception {
        // Create a message with headers
        Message message = new Message();
        message.setRoutingKey("test-queue");
        message.setBody("test body".getBytes());

        Map<String, Object> headers = new HashMap<>();
        headers.put("x-custom", "value");
        headers.put("x-number", 42);
        message.setHeaders(headers);

        System.out.println("Original message headers: " + message.getHeaders());

        // Create a queue
        String queueName = "test-queue-" + System.currentTimeMillis();
        User guest = broker.getAuthenticationManager().getUser("guest");
        broker.declareQueue("/", guest, queueName, false, false, true);

        // Publish the message
        broker.publishMessage("/", guest, "", queueName, message);

        // Get the queue and dequeue the message
        Queue queue = broker.getAuthenticationManager().getVirtualHost("/").getQueue(queueName);
        assertThat(queue).isNotNull();
        assertThat(queue.size()).isEqualTo(1);

        Message dequeued = queue.dequeue();
        assertThat(dequeued).isNotNull();

        System.out.println("Dequeued message headers: " + dequeued.getHeaders());

        // Verify headers survived
        assertThat(dequeued.getHeaders()).isNotNull();
        assertThat(dequeued.getHeaders()).hasSize(2);
        assertThat(dequeued.getHeaders()).containsEntry("x-custom", "value");
        assertThat(dequeued.getHeaders()).containsEntry("x-number", 42);
    }

    @Test
    @DisplayName("Message headers should be correctly encoded in content header frame")
    void testHeadersEncodingInContentHeader() {
        // Simulate what MessageDeliveryService does
        Message message = new Message();
        message.setBody("test".getBytes());

        Map<String, Object> headers = new HashMap<>();
        headers.put("x-custom", "value");
        headers.put("x-number", 42);
        message.setHeaders(headers);

        // Build content header like MessageDeliveryService
        ByteBuf headerPayload = Unpooled.buffer();

        short propertyFlags = 0;
        Map<String, Object> msgHeaders = message.getHeaders();
        if (msgHeaders != null && !msgHeaders.isEmpty()) {
            propertyFlags |= (1 << 13);
        }

        System.out.println("Property flags: 0x" + Integer.toHexString(propertyFlags & 0xFFFF));
        System.out.println("Headers flag set: " + ((propertyFlags & (1 << 13)) != 0));

        headerPayload.writeShort(propertyFlags);

        if (msgHeaders != null && !msgHeaders.isEmpty()) {
            AmqpCodec.encodeTable(headerPayload, msgHeaders);
        }

        int payloadSize = headerPayload.readableBytes();
        System.out.println("Total payload size: " + payloadSize);

        // Now decode
        short decodedFlags = headerPayload.readShort();
        System.out.println("Decoded flags: 0x" + Integer.toHexString(decodedFlags & 0xFFFF));

        if ((decodedFlags & (1 << 13)) != 0) {
            Map<String, Object> decodedHeaders = AmqpCodec.decodeTable(headerPayload);
            System.out.println("Decoded headers: " + decodedHeaders);
            assertThat(decodedHeaders).hasSize(2);
            assertThat(decodedHeaders).containsEntry("x-custom", "value");
            assertThat(decodedHeaders).containsEntry("x-number", 42);
        } else {
            fail("Headers flag was not set in decoded flags!");
        }
    }
}
