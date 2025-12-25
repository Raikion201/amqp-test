/*
 * Adapted from Apache ActiveMQ Artemis AMQP protocol tests.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.protocol.v10.transport;

import com.amqp.protocol.v10.types.Symbol;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 Begin performative.
 * Adapted from Apache ActiveMQ Artemis BeginTest.
 */
@DisplayName("AMQP 1.0 Begin Performative Tests")
public class BeginTest {

    @Test
    @DisplayName("Begin: Should have correct descriptor")
    void testDescriptor() {
        Begin begin = new Begin(0L, 2048L, 2048L);
        assertEquals(0x11L, begin.getDescriptor());
    }

    @Test
    @DisplayName("Begin: Constructor sets mandatory fields")
    void testConstructor() {
        Begin begin = new Begin(100L, 1000L, 500L);
        assertEquals(100L, begin.getNextOutgoingId());
        assertEquals(1000L, begin.getIncomingWindow());
        assertEquals(500L, begin.getOutgoingWindow());
    }

    @Test
    @DisplayName("Begin: Default remote channel should be null")
    void testDefaultRemoteChannel() {
        Begin begin = new Begin(0L, 2048L, 2048L);
        assertNull(begin.getRemoteChannel());
    }

    @Test
    @DisplayName("Begin: Default handle max should be uint max")
    void testDefaultHandleMax() {
        Begin begin = new Begin(0L, 2048L, 2048L);
        assertEquals(Begin.DEFAULT_HANDLE_MAX, begin.getHandleMax());
    }

    @Test
    @DisplayName("Begin: Should set remote channel")
    void testSetRemoteChannel() {
        Begin begin = new Begin(0L, 2048L, 2048L).setRemoteChannel(5);
        assertEquals(Integer.valueOf(5), begin.getRemoteChannel());
    }

    @Test
    @DisplayName("Begin: Should set handle max")
    void testSetHandleMax() {
        Begin begin = new Begin(0L, 2048L, 2048L).setHandleMax(256L);
        assertEquals(256L, begin.getHandleMax());
    }

    @Test
    @DisplayName("Begin: Should set offered capabilities")
    void testSetOfferedCapabilities() {
        Symbol[] capabilities = {Symbol.valueOf("cap1"), Symbol.valueOf("cap2")};
        Begin begin = new Begin(0L, 2048L, 2048L).setOfferedCapabilities(capabilities);
        assertArrayEquals(capabilities, begin.getOfferedCapabilities());
    }

    @Test
    @DisplayName("Begin: Should set desired capabilities")
    void testSetDesiredCapabilities() {
        Symbol[] capabilities = {Symbol.valueOf("cap1")};
        Begin begin = new Begin(0L, 2048L, 2048L).setDesiredCapabilities(capabilities);
        assertArrayEquals(capabilities, begin.getDesiredCapabilities());
    }

    @Test
    @DisplayName("Begin: Should set properties")
    void testSetProperties() {
        Map<Symbol, Object> properties = new HashMap<>();
        properties.put(Symbol.valueOf("key"), "value");
        Begin begin = new Begin(0L, 2048L, 2048L).setProperties(properties);
        assertEquals(properties, begin.getProperties());
    }

    @Test
    @DisplayName("Begin: Builder pattern should be chainable")
    void testBuilderChaining() {
        Begin begin = new Begin(0L, 2048L, 2048L)
                .setRemoteChannel(1)
                .setHandleMax(128L);

        assertEquals(Integer.valueOf(1), begin.getRemoteChannel());
        assertEquals(128L, begin.getHandleMax());
    }

    @Test
    @DisplayName("Begin: getFields should return mandatory fields")
    void testGetFields() {
        Begin begin = new Begin(10L, 100L, 200L);
        List<Object> fields = begin.getFields();

        assertTrue(fields.size() >= 4);
        assertNull(fields.get(0)); // remote channel is null
        // Fields are UInt per AMQP 1.0 spec
        assertUIntEquals(10L, fields.get(1)); // nextOutgoingId
        assertUIntEquals(100L, fields.get(2)); // incomingWindow
        assertUIntEquals(200L, fields.get(3)); // outgoingWindow
    }

    private void assertUIntEquals(long expected, Object actual) {
        if (actual instanceof com.amqp.protocol.v10.types.UInt) {
            assertEquals(expected, ((com.amqp.protocol.v10.types.UInt) actual).longValue());
        } else {
            assertEquals(expected, actual);
        }
    }

    private void assertUShortEquals(int expected, Object actual) {
        if (actual instanceof com.amqp.protocol.v10.types.UShort) {
            assertEquals(expected, ((com.amqp.protocol.v10.types.UShort) actual).intValue());
        } else {
            assertEquals(expected, actual);
        }
    }

    @Test
    @DisplayName("Begin: getFields with remote channel")
    void testGetFieldsWithRemoteChannel() {
        Begin begin = new Begin(0L, 100L, 100L).setRemoteChannel(5);
        List<Object> fields = begin.getFields();

        // remote-channel is UShort per AMQP 1.0 spec
        assertUShortEquals(5, fields.get(0));
    }

    @Test
    @DisplayName("Begin: toString should include key fields")
    void testToString() {
        Begin begin = new Begin(0L, 1024L, 512L)
                .setRemoteChannel(3);

        String str = begin.toString();
        assertTrue(str.contains("remoteChannel=3"));
        assertTrue(str.contains("incomingWindow=1024"));
        assertTrue(str.contains("outgoingWindow=512"));
    }

    @Test
    @DisplayName("Begin: Large window values")
    void testLargeWindowValues() {
        long largeWindow = 0x7FFFFFFFL; // Large but valid uint value
        Begin begin = new Begin(0L, largeWindow, largeWindow);
        assertEquals(largeWindow, begin.getIncomingWindow());
        assertEquals(largeWindow, begin.getOutgoingWindow());
    }

    @Test
    @DisplayName("Begin: Zero window should be valid")
    void testZeroWindow() {
        Begin begin = new Begin(0L, 0L, 0L);
        assertEquals(0L, begin.getIncomingWindow());
        assertEquals(0L, begin.getOutgoingWindow());
    }

    @Test
    @DisplayName("Begin: Null capabilities by default")
    void testNullCapabilities() {
        Begin begin = new Begin(0L, 100L, 100L);
        assertNull(begin.getOfferedCapabilities());
        assertNull(begin.getDesiredCapabilities());
    }

    @Test
    @DisplayName("Begin: Null properties by default")
    void testNullProperties() {
        Begin begin = new Begin(0L, 100L, 100L);
        assertNull(begin.getProperties());
    }
}
