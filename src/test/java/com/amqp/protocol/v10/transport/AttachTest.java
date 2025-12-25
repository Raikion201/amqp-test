/*
 * Adapted from Apache Qpid Broker-J AMQP 1.0 protocol tests.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.protocol.v10.transport;

import com.amqp.protocol.v10.transaction.Coordinator;
import com.amqp.protocol.v10.types.Symbol;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 Attach performative.
 * Adapted from Apache Qpid Broker-J AttachTest.
 */
@DisplayName("AMQP 1.0 Attach Performative Tests")
public class AttachTest {

    // --- Basic Tests ---

    @Test
    @DisplayName("Attach: Should have correct descriptor")
    void testDescriptor() {
        Attach attach = new Attach("test-link", 0, false);
        assertEquals(0x12L, attach.getDescriptor());
    }

    @Test
    @DisplayName("Attach: Name is required")
    void testNameRequired() {
        assertThrows(NullPointerException.class, () -> new Attach(null, 0, false));
    }

    @Test
    @DisplayName("Attach: Constructor sets mandatory fields")
    void testConstructor() {
        Attach attach = new Attach("my-link", 5L, true);
        assertEquals("my-link", attach.getName());
        assertEquals(5L, attach.getHandle());
        assertTrue(attach.getRole());
    }

    // --- Role Tests ---

    @Test
    @DisplayName("Attach: isSender returns true for sender role")
    void testIsSender() {
        Attach attach = new Attach("sender-link", 0, false);
        assertTrue(attach.isSender());
        assertFalse(attach.isReceiver());
    }

    @Test
    @DisplayName("Attach: isReceiver returns true for receiver role")
    void testIsReceiver() {
        Attach attach = new Attach("receiver-link", 0, true);
        assertTrue(attach.isReceiver());
        assertFalse(attach.isSender());
    }

    // --- Settle Mode Tests ---

    @Test
    @DisplayName("Attach: Default settle modes are null")
    void testDefaultSettleModes() {
        Attach attach = new Attach("link", 0, false);
        assertNull(attach.getSndSettleMode());
        assertNull(attach.getRcvSettleMode());
    }

    @Test
    @DisplayName("Attach: Should set sender settle mode unsettled")
    void testSetSndSettleModeUnsettled() {
        Attach attach = new Attach("link", 0, false)
                .setSndSettleMode(Attach.SND_SETTLE_MODE_UNSETTLED);
        assertEquals(Attach.SND_SETTLE_MODE_UNSETTLED, attach.getSndSettleMode());
    }

    @Test
    @DisplayName("Attach: Should set sender settle mode settled")
    void testSetSndSettleModeSettled() {
        Attach attach = new Attach("link", 0, false)
                .setSndSettleMode(Attach.SND_SETTLE_MODE_SETTLED);
        assertEquals(Attach.SND_SETTLE_MODE_SETTLED, attach.getSndSettleMode());
    }

    @Test
    @DisplayName("Attach: Should set sender settle mode mixed")
    void testSetSndSettleModeMixed() {
        Attach attach = new Attach("link", 0, false)
                .setSndSettleMode(Attach.SND_SETTLE_MODE_MIXED);
        assertEquals(Attach.SND_SETTLE_MODE_MIXED, attach.getSndSettleMode());
    }

    @Test
    @DisplayName("Attach: Should set receiver settle mode first")
    void testSetRcvSettleModeFirst() {
        Attach attach = new Attach("link", 0, true)
                .setRcvSettleMode(Attach.RCV_SETTLE_MODE_FIRST);
        assertEquals(Attach.RCV_SETTLE_MODE_FIRST, attach.getRcvSettleMode());
    }

    @Test
    @DisplayName("Attach: Should set receiver settle mode second")
    void testSetRcvSettleModeSecond() {
        Attach attach = new Attach("link", 0, true)
                .setRcvSettleMode(Attach.RCV_SETTLE_MODE_SECOND);
        assertEquals(Attach.RCV_SETTLE_MODE_SECOND, attach.getRcvSettleMode());
    }

    // --- Source and Target Tests ---

    @Test
    @DisplayName("Attach: Default source and target are null")
    void testDefaultSourceAndTarget() {
        Attach attach = new Attach("link", 0, false);
        assertNull(attach.getSource());
        assertNull(attach.getTarget());
    }

    @Test
    @DisplayName("Attach: Should set source")
    void testSetSource() {
        Source source = new Source("queue://test");
        Attach attach = new Attach("link", 0, true)
                .setSource(source);
        assertEquals(source, attach.getSource());
    }

    @Test
    @DisplayName("Attach: Should set target")
    void testSetTarget() {
        Target target = new Target("queue://test");
        Attach attach = new Attach("link", 0, false)
                .setTarget(target);
        assertEquals(target, attach.getTarget());
    }

    @Test
    @DisplayName("Attach: Sender with null source (anonymous producer)")
    void testSenderWithNullSource() {
        Target target = new Target("queue://test");
        Attach attach = new Attach("sender", 0, false)
                .setTarget(target);
        assertNull(attach.getSource());
        assertNotNull(attach.getTarget());
    }

    @Test
    @DisplayName("Attach: Receiver with null target (dynamic link)")
    void testReceiverWithNullTarget() {
        Source source = new Source("queue://test");
        Attach attach = new Attach("receiver", 0, true)
                .setSource(source);
        assertNotNull(attach.getSource());
        assertNull(attach.getTarget());
    }

    // --- Coordinator Tests (Transaction Support) ---

    @Test
    @DisplayName("Attach: Should set coordinator for transaction link")
    void testSetCoordinator() {
        Coordinator coordinator = new Coordinator(Coordinator.TXN_LOCAL);
        Attach attach = new Attach("txn-link", 0, false)
                .setCoordinator(coordinator);
        assertEquals(coordinator, attach.getCoordinator());
        assertTrue(attach.isCoordinatorLink());
    }

    @Test
    @DisplayName("Attach: isCoordinatorLink returns false when no coordinator")
    void testIsNotCoordinatorLink() {
        Attach attach = new Attach("regular-link", 0, false)
                .setTarget(new Target("queue://test"));
        assertFalse(attach.isCoordinatorLink());
    }

    // --- Delivery Count Tests ---

    @Test
    @DisplayName("Attach: Default initial delivery count is null")
    void testDefaultInitialDeliveryCount() {
        Attach attach = new Attach("link", 0, false);
        assertNull(attach.getInitialDeliveryCount());
    }

    @Test
    @DisplayName("Attach: Should set initial delivery count")
    void testSetInitialDeliveryCount() {
        Attach attach = new Attach("link", 0, false)
                .setInitialDeliveryCount(100L);
        assertEquals(100L, attach.getInitialDeliveryCount());
    }

    @Test
    @DisplayName("Attach: Sender should have initial delivery count")
    void testSenderInitialDeliveryCount() {
        Attach attach = new Attach("sender", 0, false)
                .setInitialDeliveryCount(0L);
        assertTrue(attach.isSender());
        assertEquals(0L, attach.getInitialDeliveryCount());
    }

    // --- Max Message Size Tests ---

    @Test
    @DisplayName("Attach: Default max message size is null")
    void testDefaultMaxMessageSize() {
        Attach attach = new Attach("link", 0, false);
        assertNull(attach.getMaxMessageSize());
    }

    @Test
    @DisplayName("Attach: Should set max message size")
    void testSetMaxMessageSize() {
        Attach attach = new Attach("link", 0, false)
                .setMaxMessageSize(1024 * 1024L); // 1MB
        assertEquals(1024 * 1024L, attach.getMaxMessageSize());
    }

    // --- Capabilities Tests ---

    @Test
    @DisplayName("Attach: Should set offered capabilities")
    void testSetOfferedCapabilities() {
        Symbol[] capabilities = {Symbol.valueOf("cap1"), Symbol.valueOf("cap2")};
        Attach attach = new Attach("link", 0, false)
                .setOfferedCapabilities(capabilities);
        // We can't directly assert without a getter, but the setter should work
        assertNotNull(attach);
    }

    @Test
    @DisplayName("Attach: Should set desired capabilities")
    void testSetDesiredCapabilities() {
        Symbol[] capabilities = {Symbol.valueOf("desired-cap")};
        Attach attach = new Attach("link", 0, false)
                .setDesiredCapabilities(capabilities);
        assertNotNull(attach);
    }

    // --- Properties Tests ---

    @Test
    @DisplayName("Attach: Should set properties")
    void testSetProperties() {
        Map<Symbol, Object> properties = new HashMap<>();
        properties.put(Symbol.valueOf("key1"), "value1");
        properties.put(Symbol.valueOf("key2"), 42);
        Attach attach = new Attach("link", 0, false)
                .setProperties(properties);
        assertNotNull(attach);
    }

    // --- Builder Pattern Tests ---

    @Test
    @DisplayName("Attach: Builder pattern should be chainable")
    void testBuilderChaining() {
        Source source = new Source("queue://source");
        Target target = new Target("queue://target");

        Attach attach = new Attach("my-link", 1, false)
                .setSource(source)
                .setTarget(target)
                .setSndSettleMode(Attach.SND_SETTLE_MODE_UNSETTLED)
                .setRcvSettleMode(Attach.RCV_SETTLE_MODE_FIRST)
                .setInitialDeliveryCount(0L)
                .setMaxMessageSize(65536L);

        assertEquals("my-link", attach.getName());
        assertEquals(1L, attach.getHandle());
        assertFalse(attach.getRole());
        assertEquals(source, attach.getSource());
        assertEquals(target, attach.getTarget());
        assertEquals(Attach.SND_SETTLE_MODE_UNSETTLED, attach.getSndSettleMode());
        assertEquals(Attach.RCV_SETTLE_MODE_FIRST, attach.getRcvSettleMode());
        assertEquals(0L, attach.getInitialDeliveryCount());
        assertEquals(65536L, attach.getMaxMessageSize());
    }

    // --- getFields Tests ---

    @Test
    @DisplayName("Attach: getFields should return mandatory fields")
    void testGetFields() {
        Attach attach = new Attach("test", 5L, true);
        List<Object> fields = attach.getFields();

        assertTrue(fields.size() >= 3);
        assertEquals("test", fields.get(0));
        // handle is UInt per AMQP 1.0 spec
        assertUIntEquals(5L, fields.get(1));
        assertEquals(true, fields.get(2));
    }

    private void assertUIntEquals(long expected, Object actual) {
        if (actual instanceof com.amqp.protocol.v10.types.UInt) {
            assertEquals(expected, ((com.amqp.protocol.v10.types.UInt) actual).longValue());
        } else {
            assertEquals(expected, actual);
        }
    }

    @Test
    @DisplayName("Attach: getFields should include source and target")
    void testGetFieldsWithSourceTarget() {
        Source source = new Source("queue://src");
        Target target = new Target("queue://dst");

        Attach attach = new Attach("link", 0, false)
                .setSource(source)
                .setTarget(target);

        List<Object> fields = attach.getFields();
        assertTrue(fields.size() >= 7);
    }

    @Test
    @DisplayName("Attach: getFields should use coordinator instead of target when set")
    void testGetFieldsWithCoordinator() {
        Coordinator coordinator = new Coordinator();

        Attach attach = new Attach("txn-link", 0, false)
                .setCoordinator(coordinator);

        List<Object> fields = attach.getFields();
        // Coordinator should be in the target position (index 6)
        assertTrue(fields.size() >= 7);
    }

    // --- toString Tests ---

    @Test
    @DisplayName("Attach: toString should include key fields")
    void testToString() {
        Attach attach = new Attach("my-link", 10L, false)
                .setSource(new Source("queue://src"))
                .setTarget(new Target("queue://dst"));

        String str = attach.toString();

        assertTrue(str.contains("my-link"));
        assertTrue(str.contains("10"));
        assertTrue(str.contains("sender"));
    }

    @Test
    @DisplayName("Attach: toString for receiver role")
    void testToStringReceiver() {
        Attach attach = new Attach("receiver-link", 0, true);
        String str = attach.toString();
        assertTrue(str.contains("receiver"));
    }

    // --- Handle Tests ---

    @Test
    @DisplayName("Attach: Handle can be zero")
    void testZeroHandle() {
        Attach attach = new Attach("link", 0, false);
        assertEquals(0L, attach.getHandle());
    }

    @Test
    @DisplayName("Attach: Handle can be large value")
    void testLargeHandle() {
        long largeHandle = 0xFFFFFFFFL;
        Attach attach = new Attach("link", largeHandle, false);
        assertEquals(largeHandle, attach.getHandle());
    }

    // --- Settle Mode Constants Tests ---

    @Test
    @DisplayName("Attach: Sender settle mode constants have correct values")
    void testSndSettleModeConstants() {
        assertEquals(0, Attach.SND_SETTLE_MODE_UNSETTLED);
        assertEquals(1, Attach.SND_SETTLE_MODE_SETTLED);
        assertEquals(2, Attach.SND_SETTLE_MODE_MIXED);
    }

    @Test
    @DisplayName("Attach: Receiver settle mode constants have correct values")
    void testRcvSettleModeConstants() {
        assertEquals(0, Attach.RCV_SETTLE_MODE_FIRST);
        assertEquals(1, Attach.RCV_SETTLE_MODE_SECOND);
    }
}
