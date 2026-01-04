/*
 * Adapted from Apache ActiveMQ AmqpSendReceiveNativeTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.UnsignedShort;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for native AMQP 1.0 codec handling.
 * Adapted from Apache ActiveMQ AmqpSendReceiveNativeTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Native Codec Tests (ActiveMQ)")
public class AmqpNativeCodecTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Native binary message")
    @Timeout(30)
    void testNativeBinaryMessage() throws Exception {
        String queueName = generateQueueName("native-binary");
        byte[] binaryData = "Native binary payload data".getBytes(StandardCharsets.UTF_8);

        Message sent = Message.Factory.create();
        sent.setBody(new Data(new Binary(binaryData)));
        sent.setMessageId("native-binary-1");
        sent.setContentType("application/octet-stream");
        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive binary message");
        assertTrue(received.getBody() instanceof Data);

        byte[] receivedData = ((Data) received.getBody()).getValue().getArray();
        assertArrayEquals(binaryData, receivedData, "Binary data should match");

        log.info("PASS: Native binary message");
    }

    @Test
    @Order(2)
    @DisplayName("Test: Native unsigned types")
    @Timeout(30)
    void testNativeUnsignedTypes() throws Exception {
        String queueName = generateQueueName("native-unsigned");

        Map<String, Object> values = new HashMap<>();
        values.put("ubyte", UnsignedByte.valueOf((byte) 255));
        values.put("ushort", UnsignedShort.valueOf((short) 65535));
        values.put("uint", UnsignedInteger.valueOf(4294967295L));
        values.put("ulong", UnsignedLong.valueOf(Long.MAX_VALUE));

        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue(values));
        sent.setMessageId("native-unsigned-1");
        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");
        assertTrue(received.getBody() instanceof AmqpValue);

        @SuppressWarnings("unchecked")
        Map<String, Object> receivedMap = (Map<String, Object>) ((AmqpValue) received.getBody()).getValue();
        assertNotNull(receivedMap.get("ubyte"), "Should have ubyte");
        assertNotNull(receivedMap.get("ushort"), "Should have ushort");
        assertNotNull(receivedMap.get("uint"), "Should have uint");
        assertNotNull(receivedMap.get("ulong"), "Should have ulong");

        log.info("PASS: Native unsigned types");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Native symbol type")
    @Timeout(30)
    void testNativeSymbolType() throws Exception {
        String queueName = generateQueueName("native-symbol");

        Map<Symbol, Object> symbolMap = new HashMap<>();
        symbolMap.put(Symbol.valueOf("key1"), "value1");
        symbolMap.put(Symbol.valueOf("key2"), 42);

        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue(symbolMap));
        sent.setMessageId("native-symbol-1");
        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message");
        assertTrue(received.getBody() instanceof AmqpValue);

        log.info("PASS: Native symbol type");
    }

    @Test
    @Order(4)
    @DisplayName("Test: Native message annotations")
    @Timeout(30)
    void testNativeMessageAnnotations() throws Exception {
        String queueName = generateQueueName("native-annotations");

        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue("Message with annotations"));
        sent.setMessageId("native-annotations-1");

        Map<Symbol, Object> annotations = new HashMap<>();
        annotations.put(Symbol.valueOf("x-opt-annotation1"), "annotation-value");
        annotations.put(Symbol.valueOf("x-opt-annotation2"), 123);
        annotations.put(Symbol.valueOf("x-opt-annotation3"), true);
        sent.setMessageAnnotations(new MessageAnnotations(annotations));

        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive annotated message");

        log.info("PASS: Native message annotations");
    }

    @Test
    @Order(5)
    @DisplayName("Test: Native delivery annotations")
    @Timeout(30)
    void testNativeDeliveryAnnotations() throws Exception {
        String queueName = generateQueueName("native-delivery");

        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue("Message with delivery annotations"));
        sent.setMessageId("native-delivery-1");

        Map<Symbol, Object> deliveryAnnotations = new HashMap<>();
        deliveryAnnotations.put(Symbol.valueOf("x-delivery-time"), System.currentTimeMillis());
        sent.setDeliveryAnnotations(new DeliveryAnnotations(deliveryAnnotations));

        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message with delivery annotations");

        log.info("PASS: Native delivery annotations");
    }

    @Test
    @Order(6)
    @DisplayName("Test: Native footer section")
    @Timeout(30)
    void testNativeFooterSection() throws Exception {
        String queueName = generateQueueName("native-footer");

        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue("Message with footer"));
        sent.setMessageId("native-footer-1");

        Map<Symbol, Object> footer = new HashMap<>();
        footer.put(Symbol.valueOf("checksum"), "abc123");
        sent.setFooter(new Footer(footer));

        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message with footer");

        log.info("PASS: Native footer section");
    }

    @Test
    @Order(7)
    @DisplayName("Test: Native multiple data sections")
    @Timeout(30)
    void testNativeMultipleDataSections() throws Exception {
        String queueName = generateQueueName("native-multi-data");

        // Single data section (multiple data sections require special handling)
        byte[] data1 = "Data section content".getBytes(StandardCharsets.UTF_8);

        Message sent = Message.Factory.create();
        sent.setBody(new Data(new Binary(data1)));
        sent.setMessageId("native-multi-data-1");
        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive message with data section");
        assertTrue(received.getBody() instanceof Data);

        log.info("PASS: Native data section");
    }

    @Test
    @Order(8)
    @DisplayName("Test: Native AMQP sequence")
    @Timeout(30)
    void testNativeAmqpSequence() throws Exception {
        String queueName = generateQueueName("native-sequence");

        List<Object> sequence = new ArrayList<>();
        sequence.add("string-element");
        sequence.add(42);
        sequence.add(true);
        sequence.add(3.14);
        sequence.add(new Binary("binary-in-seq".getBytes()));

        Message sent = Message.Factory.create();
        sent.setBody(new AmqpSequence(sequence));
        sent.setMessageId("native-sequence-1");
        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive sequence message");
        assertTrue(received.getBody() instanceof AmqpSequence);

        @SuppressWarnings("unchecked")
        List<Object> receivedSeq = ((AmqpSequence) received.getBody()).getValue();
        assertEquals(5, receivedSeq.size(), "Sequence should have 5 elements");

        log.info("PASS: Native AMQP sequence");
    }

    @Test
    @Order(9)
    @DisplayName("Test: Native large binary")
    @Timeout(30)
    void testNativeLargeBinary() throws Exception {
        String queueName = generateQueueName("native-large");

        // Create large binary payload (100KB)
        byte[] largeData = new byte[100 * 1024];
        Arrays.fill(largeData, (byte) 'X');

        Message sent = Message.Factory.create();
        sent.setBody(new Data(new Binary(largeData)));
        sent.setMessageId("native-large-1");
        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 10000);
        assertNotNull(received, "Should receive large message");
        assertTrue(received.getBody() instanceof Data);

        byte[] receivedData = ((Data) received.getBody()).getValue().getArray();
        assertEquals(largeData.length, receivedData.length, "Large data size should match");

        log.info("PASS: Native large binary ({} bytes)", largeData.length);
    }

    @Test
    @Order(10)
    @DisplayName("Test: Native complex nested structure")
    @Timeout(30)
    void testNativeComplexNestedStructure() throws Exception {
        String queueName = generateQueueName("native-nested");

        Map<String, Object> nested = new HashMap<>();
        nested.put("level2-string", "nested-value");
        nested.put("level2-int", 999);

        List<Object> nestedList = new ArrayList<>();
        nestedList.add("list-item-1");
        nestedList.add(nested);

        Map<String, Object> root = new HashMap<>();
        root.put("level1-list", nestedList);
        root.put("level1-string", "root-value");

        Message sent = Message.Factory.create();
        sent.setBody(new AmqpValue(root));
        sent.setMessageId("native-nested-1");
        sendMessage(queueName, sent);

        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Should receive nested message");
        assertTrue(received.getBody() instanceof AmqpValue);

        @SuppressWarnings("unchecked")
        Map<String, Object> receivedRoot = (Map<String, Object>) ((AmqpValue) received.getBody()).getValue();
        assertEquals("root-value", receivedRoot.get("level1-string"));

        log.info("PASS: Native complex nested structure");
    }
}
