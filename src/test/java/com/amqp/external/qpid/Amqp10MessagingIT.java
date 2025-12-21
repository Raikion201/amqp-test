package com.amqp.external.qpid;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for AMQP 1.0 messaging.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class Amqp10MessagingIT extends QpidProtonTestBase {

    @Test
    @Order(1)
    @DisplayName("Test message with AmqpValue body")
    void testAmqpValueBody() {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue("Test string value"));

        assertNotNull(message.getBody());
        assertTrue(message.getBody() instanceof AmqpValue);
        assertEquals("Test string value", ((AmqpValue) message.getBody()).getValue());
    }

    @Test
    @Order(2)
    @DisplayName("Test message with Data body")
    void testDataBody() {
        byte[] data = "Binary data content".getBytes(StandardCharsets.UTF_8);
        Message message = Message.Factory.create();
        message.setBody(new Data(new Binary(data)));

        assertNotNull(message.getBody());
        assertTrue(message.getBody() instanceof Data);

        Binary binary = ((Data) message.getBody()).getValue();
        assertEquals("Binary data content", new String(binary.getArray(), StandardCharsets.UTF_8));
    }

    @Test
    @Order(3)
    @DisplayName("Test message properties")
    void testMessageProperties() {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue("Test"));

        // Set properties
        Properties props = new Properties();
        props.setMessageId(UUID.randomUUID().toString());
        props.setCorrelationId("correlation-123");
        props.setTo("test-queue");
        props.setSubject("test-subject");
        props.setReplyTo("reply-queue");
        props.setContentType(org.apache.qpid.proton.amqp.Symbol.valueOf("text/plain"));
        message.setProperties(props);

        // Verify
        assertNotNull(message.getProperties());
        assertNotNull(message.getProperties().getMessageId());
        assertEquals("correlation-123", message.getProperties().getCorrelationId());
        assertEquals("test-queue", message.getProperties().getTo());
        assertEquals("test-subject", message.getProperties().getSubject());
    }

    @Test
    @Order(4)
    @DisplayName("Test message header")
    void testMessageHeader() {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue("Test"));

        Header header = new Header();
        header.setDurable(true);
        header.setPriority(org.apache.qpid.proton.amqp.UnsignedByte.valueOf((byte) 5));
        header.setTtl(org.apache.qpid.proton.amqp.UnsignedInteger.valueOf(60000));
        header.setFirstAcquirer(true);
        header.setDeliveryCount(org.apache.qpid.proton.amqp.UnsignedInteger.valueOf(0));
        message.setHeader(header);

        assertNotNull(message.getHeader());
        assertTrue(message.getHeader().getDurable());
        assertEquals(5, message.getHeader().getPriority().intValue());
        assertEquals(60000L, message.getHeader().getTtl().longValue());
    }

    @Test
    @Order(5)
    @DisplayName("Test application properties")
    void testApplicationProperties() {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue("Test"));

        Map<String, Object> appProps = new HashMap<>();
        appProps.put("custom-header", "custom-value");
        appProps.put("int-header", 42);
        appProps.put("bool-header", true);
        message.setApplicationProperties(new ApplicationProperties(appProps));

        assertNotNull(message.getApplicationProperties());
        assertEquals("custom-value", message.getApplicationProperties().getValue().get("custom-header"));
        assertEquals(42, message.getApplicationProperties().getValue().get("int-header"));
    }

    @Test
    @Order(6)
    @DisplayName("Test message annotations")
    void testMessageAnnotations() {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue("Test"));

        Map<org.apache.qpid.proton.amqp.Symbol, Object> annotations = new HashMap<>();
        annotations.put(org.apache.qpid.proton.amqp.Symbol.valueOf("x-routing-key"), "my.routing.key");
        annotations.put(org.apache.qpid.proton.amqp.Symbol.valueOf("x-exchange"), "test-exchange");
        message.setMessageAnnotations(new MessageAnnotations(annotations));

        assertNotNull(message.getMessageAnnotations());
        assertEquals("my.routing.key",
                message.getMessageAnnotations().getValue().get(
                        org.apache.qpid.proton.amqp.Symbol.valueOf("x-routing-key")));
    }

    @Test
    @Order(7)
    @DisplayName("Test large message")
    void testLargeMessage() {
        // Create a 1MB message
        byte[] largeData = new byte[1024 * 1024];
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }

        Message message = Message.Factory.create();
        message.setBody(new Data(new Binary(largeData)));

        Data body = (Data) message.getBody();
        assertEquals(1024 * 1024, body.getValue().getLength());
    }

    @Test
    @Order(8)
    @DisplayName("Test message encoding/decoding")
    void testMessageEncodingDecoding() {
        // Create message
        Message original = Message.Factory.create();
        original.setBody(new AmqpValue("Encoded message"));

        Properties props = new Properties();
        props.setMessageId("msg-001");
        original.setProperties(props);

        // Encode
        byte[] encoded = new byte[1024];
        int length = original.encode(encoded, 0, encoded.length);
        assertTrue(length > 0);

        // Decode
        Message decoded = Message.Factory.create();
        decoded.decode(encoded, 0, length);

        assertEquals("Encoded message", ((AmqpValue) decoded.getBody()).getValue());
        assertEquals("msg-001", decoded.getProperties().getMessageId());
    }
}
