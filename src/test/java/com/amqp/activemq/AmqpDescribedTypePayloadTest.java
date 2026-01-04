/*
 * Adapted from Apache ActiveMQ AmqpDescribedTypePayloadTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 described type payload handling.
 * Adapted from Apache ActiveMQ AmqpDescribedTypePayloadTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Described Type Payload Tests (ActiveMQ)")
public class AmqpDescribedTypePayloadTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: AmqpValue with string payload")
    @Timeout(30)
    void testAmqpValueWithStringPayload() throws Exception {
        String queueName = generateQueueName("described-string");
        String payload = "Test string payload";

        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue(payload));
        sent.setMessageId("string-payload-1");
        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");
        assertTrue(received.getBody() instanceof AmqpValue);
        assertEquals(payload, ((AmqpValue) received.getBody()).getValue());

        log.info("PASS: AmqpValue with string payload");
    }

    @Test
    @Order(2)
    @DisplayName("Test: AmqpValue with map payload")
    @Timeout(30)
    void testAmqpValueWithMapPayload() throws Exception {
        String queueName = generateQueueName("described-map");

        Map<String, Object> payload = new HashMap<>();
        payload.put("key1", "value1");
        payload.put("key2", 42);
        payload.put("key3", true);

        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue(payload));
        sent.setMessageId("map-payload-1");
        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");
        assertTrue(received.getBody() instanceof AmqpValue);

        @SuppressWarnings("unchecked")
        Map<String, Object> receivedMap = (Map<String, Object>) ((AmqpValue) received.getBody()).getValue();
        assertEquals("value1", receivedMap.get("key1"));
        assertEquals(42, receivedMap.get("key2"));
        assertEquals(true, receivedMap.get("key3"));

        log.info("PASS: AmqpValue with map payload");
    }

    @Test
    @Order(3)
    @DisplayName("Test: AmqpValue with list payload")
    @Timeout(30)
    void testAmqpValueWithListPayload() throws Exception {
        String queueName = generateQueueName("described-list");

        List<Object> payload = new ArrayList<>();
        payload.add("item1");
        payload.add(123);
        payload.add(3.14);

        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue(payload));
        sent.setMessageId("list-payload-1");
        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");
        assertTrue(received.getBody() instanceof AmqpValue);

        @SuppressWarnings("unchecked")
        List<Object> receivedList = (List<Object>) ((AmqpValue) received.getBody()).getValue();
        assertEquals(3, receivedList.size());
        assertEquals("item1", receivedList.get(0));

        log.info("PASS: AmqpValue with list payload");
    }

    @Test
    @Order(4)
    @DisplayName("Test: Data section payload")
    @Timeout(30)
    void testDataSectionPayload() throws Exception {
        String queueName = generateQueueName("described-data");
        byte[] payload = "Binary data payload".getBytes();

        Message sent = Message.Factory.create();
        sent.setBody(new Data(new Binary(payload)));
        sent.setMessageId("data-payload-1");
        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");
        assertTrue(received.getBody() instanceof Data);

        byte[] receivedData = ((Data) received.getBody()).getValue().getArray();
        assertArrayEquals(payload, receivedData);

        log.info("PASS: Data section payload");
    }

    @Test
    @Order(5)
    @DisplayName("Test: AmqpSequence payload")
    @Timeout(30)
    void testAmqpSequencePayload() throws Exception {
        String queueName = generateQueueName("described-sequence");

        List<Object> sequence = new ArrayList<>();
        sequence.add("seq-item-1");
        sequence.add("seq-item-2");
        sequence.add(100);

        Message sent = Message.Factory.create();
        sent.setBody(new AmqpSequence(sequence));
        sent.setMessageId("sequence-payload-1");
        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");
        assertTrue(received.getBody() instanceof AmqpSequence);

        @SuppressWarnings("unchecked")
        List<Object> receivedSeq = ((AmqpSequence) received.getBody()).getValue();
        assertEquals(3, receivedSeq.size());

        log.info("PASS: AmqpSequence payload");
    }

    @Test
    @Order(6)
    @DisplayName("Test: Message with all sections")
    @Timeout(30)
    void testMessageWithAllSections() throws Exception {
        String queueName = generateQueueName("described-all");

        Message sent = Message.Factory.create();

        // Header
        Header header = new Header();
        header.setDurable(true);
        header.setPriority(UnsignedByte.valueOf((byte) 5));
        sent.setHeader(header);

        // Properties
        org.apache.qpid.proton.amqp.messaging.Properties props =
            new org.apache.qpid.proton.amqp.messaging.Properties();
        props.setMessageId("full-msg-1");
        props.setContentType(Symbol.valueOf("application/json"));
        props.setSubject("test-subject");
        sent.setProperties(props);

        // Message annotations
        Map<Symbol, Object> annotations = new HashMap<>();
        annotations.put(Symbol.valueOf("x-custom-annotation"), "annotation-value");
        sent.setMessageAnnotations(new MessageAnnotations(annotations));

        // Application properties
        Map<String, Object> appProps = new HashMap<>();
        appProps.put("app-prop-1", "app-value-1");
        sent.setApplicationProperties(new ApplicationProperties(appProps));

        // Body
        sent.setBody(new AmqpValue("Full message body"));

        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");

        // Verify body
        assertEquals("Full message body", ((AmqpValue) received.getBody()).getValue());

        // Verify some properties were preserved
        assertNotNull(received.getProperties());

        log.info("PASS: Message with all sections");
    }

    @Test
    @Order(7)
    @DisplayName("Test: Nested map payload")
    @Timeout(30)
    void testNestedMapPayload() throws Exception {
        String queueName = generateQueueName("described-nested");

        Map<String, Object> nested = new HashMap<>();
        nested.put("nestedKey", "nestedValue");

        Map<String, Object> payload = new HashMap<>();
        payload.put("outer", nested);
        payload.put("simple", "simpleValue");

        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue(payload));
        sent.setMessageId("nested-map-1");
        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");

        @SuppressWarnings("unchecked")
        Map<String, Object> receivedMap = (Map<String, Object>) ((AmqpValue) received.getBody()).getValue();
        assertEquals("simpleValue", receivedMap.get("simple"));

        log.info("PASS: Nested map payload");
    }
}
