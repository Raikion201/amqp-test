/*
 * Adapted from Apache Qpid Broker-J AMQP 1.0 protocol tests.
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
 * Tests for AMQP 1.0 Flow performative.
 * Adapted from Apache Qpid Broker-J protocol tests.
 */
@DisplayName("AMQP 1.0 Flow Performative Tests")
public class FlowTest {

    // --- Basic Tests ---

    @Test
    @DisplayName("Flow: Should have correct descriptor")
    void testDescriptor() {
        Flow flow = new Flow(100, 0, 100);
        assertEquals(0x13L, flow.getDescriptor());
    }

    @Test
    @DisplayName("Flow: Constructor sets mandatory fields")
    void testConstructor() {
        Flow flow = new Flow(1000L, 50L, 500L);
        assertEquals(1000L, flow.getIncomingWindow());
        assertEquals(50L, flow.getNextOutgoingId());
        assertEquals(500L, flow.getOutgoingWindow());
    }

    // --- Session Flow Tests ---

    @Test
    @DisplayName("Flow: Session flow has no handle")
    void testSessionFlowNoHandle() {
        Flow flow = new Flow(100, 0, 100);
        assertNull(flow.getHandle());
        assertFalse(flow.isLinkFlow());
    }

    @Test
    @DisplayName("Flow: Session flow with next incoming ID")
    void testSessionFlowWithNextIncomingId() {
        Flow flow = new Flow(100, 0, 100)
                .setNextIncomingId(10L);
        assertEquals(10L, flow.getNextIncomingId());
    }

    @Test
    @DisplayName("Flow: Default next incoming ID is null")
    void testDefaultNextIncomingId() {
        Flow flow = new Flow(100, 0, 100);
        assertNull(flow.getNextIncomingId());
    }

    // --- Link Flow Tests ---

    @Test
    @DisplayName("Flow: Link flow has handle")
    void testLinkFlowHasHandle() {
        Flow flow = new Flow(100, 0, 100)
                .setHandle(5L);
        assertEquals(5L, flow.getHandle());
        assertTrue(flow.isLinkFlow());
    }

    @Test
    @DisplayName("Flow: Link flow with delivery count and credit")
    void testLinkFlowWithDeliveryCountAndCredit() {
        Flow flow = new Flow(100, 0, 100)
                .setHandle(0L)
                .setDeliveryCount(100L)
                .setLinkCredit(10L);

        assertEquals(0L, flow.getHandle());
        assertEquals(100L, flow.getDeliveryCount());
        assertEquals(10L, flow.getLinkCredit());
    }

    // --- Delivery Count Tests ---

    @Test
    @DisplayName("Flow: Default delivery count is null")
    void testDefaultDeliveryCount() {
        Flow flow = new Flow(100, 0, 100);
        assertNull(flow.getDeliveryCount());
    }

    @Test
    @DisplayName("Flow: Should set delivery count")
    void testSetDeliveryCount() {
        Flow flow = new Flow(100, 0, 100)
                .setDeliveryCount(50L);
        assertEquals(50L, flow.getDeliveryCount());
    }

    @Test
    @DisplayName("Flow: Delivery count can be zero")
    void testZeroDeliveryCount() {
        Flow flow = new Flow(100, 0, 100)
                .setDeliveryCount(0L);
        assertEquals(0L, flow.getDeliveryCount());
    }

    // --- Link Credit Tests ---

    @Test
    @DisplayName("Flow: Default link credit is null")
    void testDefaultLinkCredit() {
        Flow flow = new Flow(100, 0, 100);
        assertNull(flow.getLinkCredit());
    }

    @Test
    @DisplayName("Flow: Should set link credit")
    void testSetLinkCredit() {
        Flow flow = new Flow(100, 0, 100)
                .setLinkCredit(100L);
        assertEquals(100L, flow.getLinkCredit());
    }

    @Test
    @DisplayName("Flow: Link credit can be zero (no credit)")
    void testZeroLinkCredit() {
        Flow flow = new Flow(100, 0, 100)
                .setLinkCredit(0L);
        assertEquals(0L, flow.getLinkCredit());
    }

    @Test
    @DisplayName("Flow: Large link credit value")
    void testLargeLinkCredit() {
        long largeCredit = 0xFFFFFFFFL;
        Flow flow = new Flow(100, 0, 100)
                .setLinkCredit(largeCredit);
        assertEquals(largeCredit, flow.getLinkCredit());
    }

    // --- Available Tests ---

    @Test
    @DisplayName("Flow: Default available is null")
    void testDefaultAvailable() {
        Flow flow = new Flow(100, 0, 100);
        assertNull(flow.getAvailable());
    }

    @Test
    @DisplayName("Flow: Should set available")
    void testSetAvailable() {
        Flow flow = new Flow(100, 0, 100)
                .setAvailable(50L);
        assertEquals(50L, flow.getAvailable());
    }

    // --- Drain Tests ---

    @Test
    @DisplayName("Flow: Default drain is null")
    void testDefaultDrain() {
        Flow flow = new Flow(100, 0, 100);
        assertNull(flow.getDrain());
    }

    @Test
    @DisplayName("Flow: isDrain returns false when null")
    void testIsDrainNull() {
        Flow flow = new Flow(100, 0, 100);
        assertFalse(flow.isDrain());
    }

    @Test
    @DisplayName("Flow: Should set drain true")
    void testSetDrainTrue() {
        Flow flow = new Flow(100, 0, 100)
                .setDrain(true);
        assertTrue(flow.isDrain());
        assertEquals(Boolean.TRUE, flow.getDrain());
    }

    @Test
    @DisplayName("Flow: Drain mode with link credit")
    void testDrainModeWithCredit() {
        // Drain mode asks receiver to send all available messages up to credit
        Flow flow = new Flow(100, 0, 100)
                .setHandle(0L)
                .setDeliveryCount(0L)
                .setLinkCredit(100L)
                .setDrain(true);

        assertTrue(flow.isDrain());
        assertEquals(100L, flow.getLinkCredit());
    }

    // --- Echo Tests ---

    @Test
    @DisplayName("Flow: Default echo is null")
    void testDefaultEcho() {
        Flow flow = new Flow(100, 0, 100);
        assertNull(flow.getEcho());
    }

    @Test
    @DisplayName("Flow: isEcho returns false when null")
    void testIsEchoNull() {
        Flow flow = new Flow(100, 0, 100);
        assertFalse(flow.isEcho());
    }

    @Test
    @DisplayName("Flow: Should set echo true")
    void testSetEchoTrue() {
        Flow flow = new Flow(100, 0, 100)
                .setEcho(true);
        assertTrue(flow.isEcho());
        assertEquals(Boolean.TRUE, flow.getEcho());
    }

    @Test
    @DisplayName("Flow: Echo requests flow state response")
    void testEchoRequestsResponse() {
        Flow flow = new Flow(100, 0, 100)
                .setEcho(true);
        assertTrue(flow.isEcho());
    }

    // --- Properties Tests ---

    @Test
    @DisplayName("Flow: Default properties is null")
    void testDefaultProperties() {
        Flow flow = new Flow(100, 0, 100);
        assertNull(flow.getProperties());
    }

    @Test
    @DisplayName("Flow: Should set properties")
    void testSetProperties() {
        Map<Symbol, Object> properties = new HashMap<>();
        properties.put(Symbol.valueOf("key"), "value");
        Flow flow = new Flow(100, 0, 100)
                .setProperties(properties);
        assertEquals(properties, flow.getProperties());
    }

    // --- Window Tests ---

    @Test
    @DisplayName("Flow: Zero incoming window")
    void testZeroIncomingWindow() {
        Flow flow = new Flow(0, 0, 100);
        assertEquals(0L, flow.getIncomingWindow());
    }

    @Test
    @DisplayName("Flow: Zero outgoing window")
    void testZeroOutgoingWindow() {
        Flow flow = new Flow(100, 0, 0);
        assertEquals(0L, flow.getOutgoingWindow());
    }

    @Test
    @DisplayName("Flow: Large window values")
    void testLargeWindowValues() {
        long largeWindow = 0x7FFFFFFFL;
        Flow flow = new Flow(largeWindow, 0, largeWindow);
        assertEquals(largeWindow, flow.getIncomingWindow());
        assertEquals(largeWindow, flow.getOutgoingWindow());
    }

    @Test
    @DisplayName("Flow: setIncomingWindow updates value")
    void testSetIncomingWindow() {
        Flow flow = new Flow(100, 0, 100)
                .setIncomingWindow(500L);
        assertEquals(500L, flow.getIncomingWindow());
    }

    // --- Builder Pattern Tests ---

    @Test
    @DisplayName("Flow: Builder pattern should be chainable for session flow")
    void testBuilderChainingSession() {
        Flow flow = new Flow(1000, 100, 500)
                .setNextIncomingId(50L)
                .setEcho(true);

        assertEquals(1000L, flow.getIncomingWindow());
        assertEquals(100L, flow.getNextOutgoingId());
        assertEquals(500L, flow.getOutgoingWindow());
        assertEquals(50L, flow.getNextIncomingId());
        assertTrue(flow.isEcho());
        assertFalse(flow.isLinkFlow());
    }

    @Test
    @DisplayName("Flow: Builder pattern should be chainable for link flow")
    void testBuilderChainingLink() {
        Flow flow = new Flow(1000, 100, 500)
                .setHandle(0L)
                .setDeliveryCount(50L)
                .setLinkCredit(100L)
                .setAvailable(25L)
                .setDrain(false);

        assertTrue(flow.isLinkFlow());
        assertEquals(0L, flow.getHandle());
        assertEquals(50L, flow.getDeliveryCount());
        assertEquals(100L, flow.getLinkCredit());
        assertEquals(25L, flow.getAvailable());
        assertFalse(flow.isDrain());
    }

    // --- getFields Tests ---

    @Test
    @DisplayName("Flow: getFields should return mandatory fields for session flow")
    void testGetFieldsSession() {
        Flow flow = new Flow(100, 0, 50);
        List<Object> fields = flow.getFields();

        assertTrue(fields.size() >= 4);
        // nextIncomingId is null
        // Fields are UInt per AMQP 1.0 spec
        assertUIntEquals(100L, fields.get(1)); // incomingWindow
        assertUIntEquals(0L, fields.get(2));   // nextOutgoingId
        assertUIntEquals(50L, fields.get(3));  // outgoingWindow
    }

    @Test
    @DisplayName("Flow: getFields should include link fields")
    void testGetFieldsLink() {
        Flow flow = new Flow(100, 0, 50)
                .setHandle(5L)
                .setDeliveryCount(10L)
                .setLinkCredit(20L);

        List<Object> fields = flow.getFields();

        assertTrue(fields.size() >= 7);
        // Fields are UInt per AMQP 1.0 spec
        assertUIntEquals(5L, fields.get(4));  // handle
        assertUIntEquals(10L, fields.get(5)); // deliveryCount
        assertUIntEquals(20L, fields.get(6)); // linkCredit
    }

    private void assertUIntEquals(long expected, Object actual) {
        if (actual instanceof com.amqp.protocol.v10.types.UInt) {
            assertEquals(expected, ((com.amqp.protocol.v10.types.UInt) actual).longValue());
        } else {
            assertEquals(expected, actual);
        }
    }

    // --- toString Tests ---

    @Test
    @DisplayName("Flow: toString for session flow")
    void testToStringSession() {
        Flow flow = new Flow(100, 0, 50)
                .setNextIncomingId(10L);

        String str = flow.toString();

        assertTrue(str.contains("nextIncomingId"));
        assertTrue(str.contains("incomingWindow"));
        assertTrue(str.contains("outgoingWindow"));
    }

    @Test
    @DisplayName("Flow: toString for link flow")
    void testToStringLink() {
        Flow flow = new Flow(100, 0, 50)
                .setHandle(5L)
                .setDeliveryCount(10L)
                .setLinkCredit(20L)
                .setDrain(true);

        String str = flow.toString();

        assertTrue(str.contains("handle"));
        assertTrue(str.contains("deliveryCount"));
        assertTrue(str.contains("linkCredit"));
        assertTrue(str.contains("drain"));
    }

    // --- Credit Management Tests ---

    @Test
    @DisplayName("Flow: Grant initial credit to receiver")
    void testGrantInitialCredit() {
        // Receiver sends flow to grant credit
        Flow flow = new Flow(2048, 0, 2048)
                .setHandle(0L)
                .setDeliveryCount(0L)
                .setLinkCredit(100L);

        assertTrue(flow.isLinkFlow());
        assertEquals(0L, flow.getDeliveryCount());
        assertEquals(100L, flow.getLinkCredit());
    }

    @Test
    @DisplayName("Flow: Replenish credit after messages received")
    void testReplenishCredit() {
        // After receiving 50 messages, receiver replenishes credit
        Flow flow = new Flow(2048, 0, 2048)
                .setHandle(0L)
                .setDeliveryCount(50L)
                .setLinkCredit(100L); // Total 150 deliveries possible

        assertEquals(50L, flow.getDeliveryCount());
        assertEquals(100L, flow.getLinkCredit());
    }

    @Test
    @DisplayName("Flow: Stop flow by setting zero credit")
    void testStopFlow() {
        Flow flow = new Flow(2048, 0, 2048)
                .setHandle(0L)
                .setDeliveryCount(100L)
                .setLinkCredit(0L);

        assertEquals(0L, flow.getLinkCredit());
    }

    @Test
    @DisplayName("Flow: Drain remaining messages")
    void testDrainRemaining() {
        Flow flow = new Flow(2048, 0, 2048)
                .setHandle(0L)
                .setDeliveryCount(50L)
                .setLinkCredit(100L)
                .setDrain(true);

        assertTrue(flow.isDrain());
        // Sender should send up to 100 more messages and then advance delivery count
    }
}
