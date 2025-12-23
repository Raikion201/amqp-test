package com.amqp.protocol.v10.transaction;

import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.Symbol;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 Coordinator terminus.
 */
@DisplayName("AMQP 1.0 Coordinator Tests")
public class CoordinatorTest {

    @Test
    @DisplayName("Coordinator: Should have correct descriptor")
    void testDescriptor() {
        Coordinator coordinator = new Coordinator();
        assertEquals(0x30L, Coordinator.DESCRIPTOR);
    }

    @Test
    @DisplayName("Coordinator: Default constructor creates empty capabilities")
    void testDefaultConstructor() {
        Coordinator coordinator = new Coordinator();
        assertNull(coordinator.getCapabilities());
    }

    @Test
    @DisplayName("Coordinator: Constructor with capabilities")
    void testConstructorWithCapabilities() {
        Symbol[] caps = {Coordinator.TXN_LOCAL, Coordinator.TXN_DISTRIBUTED};
        Coordinator coordinator = new Coordinator(caps);
        assertArrayEquals(caps, coordinator.getCapabilities());
    }

    @Test
    @DisplayName("Coordinator: Set capabilities")
    void testSetCapabilities() {
        Coordinator coordinator = new Coordinator();
        Symbol[] caps = {Coordinator.TXN_LOCAL};
        coordinator.setCapabilities(caps);
        assertArrayEquals(caps, coordinator.getCapabilities());
    }

    @Test
    @DisplayName("Coordinator: hasCapability returns true for present capability")
    void testHasCapabilityTrue() {
        Coordinator coordinator = new Coordinator(Coordinator.TXN_LOCAL, Coordinator.TXN_MULTI_TXNS);
        assertTrue(coordinator.hasCapability(Coordinator.TXN_LOCAL));
        assertTrue(coordinator.hasCapability(Coordinator.TXN_MULTI_TXNS));
    }

    @Test
    @DisplayName("Coordinator: hasCapability returns false for missing capability")
    void testHasCapabilityFalse() {
        Coordinator coordinator = new Coordinator(Coordinator.TXN_LOCAL);
        assertFalse(coordinator.hasCapability(Coordinator.TXN_DISTRIBUTED));
    }

    @Test
    @DisplayName("Coordinator: hasCapability returns false for null capabilities")
    void testHasCapabilityNullCapabilities() {
        Coordinator coordinator = new Coordinator();
        assertFalse(coordinator.hasCapability(Coordinator.TXN_LOCAL));
    }

    @Test
    @DisplayName("Coordinator: toDescribed produces valid described type")
    void testToDescribed() {
        Symbol[] caps = {Coordinator.TXN_LOCAL};
        Coordinator coordinator = new Coordinator(caps);

        DescribedType described = coordinator.toDescribed();

        assertNotNull(described);
        assertEquals(Coordinator.DESCRIPTOR, described.getDescriptor());
    }

    @Test
    @DisplayName("Coordinator: toString includes capabilities")
    void testToString() {
        Coordinator coordinator = new Coordinator(Coordinator.TXN_LOCAL);
        String str = coordinator.toString();

        assertTrue(str.contains("Coordinator"));
        assertTrue(str.contains("amqp:local-transactions"));
    }

    @Test
    @DisplayName("Coordinator: TXN_LOCAL symbol value")
    void testTxnLocalSymbol() {
        assertEquals("amqp:local-transactions", Coordinator.TXN_LOCAL.toString());
    }

    @Test
    @DisplayName("Coordinator: TXN_DISTRIBUTED symbol value")
    void testTxnDistributedSymbol() {
        assertEquals("amqp:distributed-transactions", Coordinator.TXN_DISTRIBUTED.toString());
    }
}
