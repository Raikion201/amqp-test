/*
 * Adapted from Apache Qpid Broker-J AMQP 1.0 protocol tests.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.protocol.v10.transport;

import com.amqp.protocol.v10.delivery.Accepted;
import com.amqp.protocol.v10.delivery.Rejected;
import com.amqp.protocol.v10.delivery.Released;
import com.amqp.protocol.v10.transaction.TransactionalState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 Disposition performative.
 * Adapted from Apache Qpid Broker-J protocol tests.
 */
@DisplayName("AMQP 1.0 Disposition Performative Tests")
public class DispositionTest {

    // --- Basic Tests ---

    @Test
    @DisplayName("Disposition: Should have correct descriptor")
    void testDescriptor() {
        Disposition disposition = new Disposition(true, 0);
        assertEquals(0x15L, disposition.getDescriptor());
    }

    @Test
    @DisplayName("Disposition: Constructor sets mandatory fields")
    void testConstructor() {
        Disposition disposition = new Disposition(true, 100L);
        assertTrue(disposition.getRole());
        assertEquals(100L, disposition.getFirst());
    }

    // --- Role Tests ---

    @Test
    @DisplayName("Disposition: isSender returns true for sender role")
    void testIsSender() {
        Disposition disposition = new Disposition(false, 0);
        assertTrue(disposition.isSender());
        assertFalse(disposition.isReceiver());
    }

    @Test
    @DisplayName("Disposition: isReceiver returns true for receiver role")
    void testIsReceiver() {
        Disposition disposition = new Disposition(true, 0);
        assertTrue(disposition.isReceiver());
        assertFalse(disposition.isSender());
    }

    // --- First Tests ---

    @Test
    @DisplayName("Disposition: First can be zero")
    void testFirstZero() {
        Disposition disposition = new Disposition(true, 0);
        assertEquals(0L, disposition.getFirst());
    }

    @Test
    @DisplayName("Disposition: First can be large value")
    void testFirstLargeValue() {
        long largeValue = 0xFFFFFFFFL;
        Disposition disposition = new Disposition(true, largeValue);
        assertEquals(largeValue, disposition.getFirst());
    }

    // --- Last Tests ---

    @Test
    @DisplayName("Disposition: Default last is null")
    void testDefaultLast() {
        Disposition disposition = new Disposition(true, 0);
        assertNull(disposition.getLast());
    }

    @Test
    @DisplayName("Disposition: Should set last")
    void testSetLast() {
        Disposition disposition = new Disposition(true, 0)
                .setLast(10L);
        assertEquals(10L, disposition.getLast());
    }

    @Test
    @DisplayName("Disposition: getLastOrFirst returns first when last is null")
    void testGetLastOrFirstNull() {
        Disposition disposition = new Disposition(true, 5);
        assertEquals(5L, disposition.getLastOrFirst());
    }

    @Test
    @DisplayName("Disposition: getLastOrFirst returns last when set")
    void testGetLastOrFirstSet() {
        Disposition disposition = new Disposition(true, 0)
                .setLast(10L);
        assertEquals(10L, disposition.getLastOrFirst());
    }

    // --- Settled Tests ---

    @Test
    @DisplayName("Disposition: Default settled is null")
    void testDefaultSettled() {
        Disposition disposition = new Disposition(true, 0);
        assertNull(disposition.getSettled());
    }

    @Test
    @DisplayName("Disposition: isSettled returns false when null")
    void testIsSettledNull() {
        Disposition disposition = new Disposition(true, 0);
        assertFalse(disposition.isSettled());
    }

    @Test
    @DisplayName("Disposition: Should set settled true")
    void testSetSettledTrue() {
        Disposition disposition = new Disposition(true, 0)
                .setSettled(true);
        assertTrue(disposition.isSettled());
        assertEquals(Boolean.TRUE, disposition.getSettled());
    }

    @Test
    @DisplayName("Disposition: Should set settled false")
    void testSetSettledFalse() {
        Disposition disposition = new Disposition(true, 0)
                .setSettled(false);
        assertFalse(disposition.isSettled());
        assertEquals(Boolean.FALSE, disposition.getSettled());
    }

    // --- State Tests ---

    @Test
    @DisplayName("Disposition: Default state is null")
    void testDefaultState() {
        Disposition disposition = new Disposition(true, 0);
        assertNull(disposition.getState());
    }

    @Test
    @DisplayName("Disposition: Should set Accepted state")
    void testSetAcceptedState() {
        Disposition disposition = new Disposition(true, 0)
                .setState(Accepted.INSTANCE);
        assertEquals(Accepted.INSTANCE, disposition.getState());
    }

    @Test
    @DisplayName("Disposition: Should set Rejected state")
    void testSetRejectedState() {
        Rejected rejected = new Rejected();
        Disposition disposition = new Disposition(true, 0)
                .setState(rejected);
        assertEquals(rejected, disposition.getState());
    }

    @Test
    @DisplayName("Disposition: Should set Released state")
    void testSetReleasedState() {
        Released released = new Released();
        Disposition disposition = new Disposition(true, 0)
                .setState(released);
        assertEquals(released, disposition.getState());
    }

    @Test
    @DisplayName("Disposition: Should set TransactionalState")
    void testSetTransactionalState() {
        byte[] txnId = {0x01, 0x02, 0x03};
        TransactionalState txnState = new TransactionalState(txnId, Accepted.INSTANCE);
        Disposition disposition = new Disposition(true, 0)
                .setState(txnState);
        assertEquals(txnState, disposition.getState());
    }

    // --- Batchable Tests ---

    @Test
    @DisplayName("Disposition: Default batchable is null")
    void testDefaultBatchable() {
        Disposition disposition = new Disposition(true, 0);
        assertNull(disposition.getBatchable());
    }

    @Test
    @DisplayName("Disposition: isBatchable returns false when null")
    void testIsBatchableNull() {
        Disposition disposition = new Disposition(true, 0);
        assertFalse(disposition.isBatchable());
    }

    @Test
    @DisplayName("Disposition: Should set batchable true")
    void testSetBatchableTrue() {
        Disposition disposition = new Disposition(true, 0)
                .setBatchable(true);
        assertTrue(disposition.isBatchable());
        assertEquals(Boolean.TRUE, disposition.getBatchable());
    }

    // --- Range Tests (covers method) ---

    @Test
    @DisplayName("Disposition: covers returns true for exact first match")
    void testCoversExactFirst() {
        Disposition disposition = new Disposition(true, 5);
        assertTrue(disposition.covers(5));
    }

    @Test
    @DisplayName("Disposition: covers returns false for value less than first")
    void testCoversLessThanFirst() {
        Disposition disposition = new Disposition(true, 5);
        assertFalse(disposition.covers(4));
    }

    @Test
    @DisplayName("Disposition: covers returns false for value greater than first when no last")
    void testCoversGreaterNoLast() {
        Disposition disposition = new Disposition(true, 5);
        assertFalse(disposition.covers(6));
    }

    @Test
    @DisplayName("Disposition: covers returns true for value in range")
    void testCoversInRange() {
        Disposition disposition = new Disposition(true, 5)
                .setLast(10L);
        assertTrue(disposition.covers(5));
        assertTrue(disposition.covers(7));
        assertTrue(disposition.covers(10));
    }

    @Test
    @DisplayName("Disposition: covers returns false for value outside range")
    void testCoversOutsideRange() {
        Disposition disposition = new Disposition(true, 5)
                .setLast(10L);
        assertFalse(disposition.covers(4));
        assertFalse(disposition.covers(11));
    }

    @Test
    @DisplayName("Disposition: covers handles single delivery (first == last)")
    void testCoversSingleDelivery() {
        Disposition disposition = new Disposition(true, 5)
                .setLast(5L);
        assertTrue(disposition.covers(5));
        assertFalse(disposition.covers(4));
        assertFalse(disposition.covers(6));
    }

    // --- Builder Pattern Tests ---

    @Test
    @DisplayName("Disposition: Builder pattern should be chainable")
    void testBuilderChaining() {
        Disposition disposition = new Disposition(true, 0)
                .setLast(10L)
                .setSettled(true)
                .setState(Accepted.INSTANCE)
                .setBatchable(false);

        assertTrue(disposition.getRole());
        assertEquals(0L, disposition.getFirst());
        assertEquals(10L, disposition.getLast());
        assertTrue(disposition.isSettled());
        assertEquals(Accepted.INSTANCE, disposition.getState());
        assertFalse(disposition.isBatchable());
    }

    // --- getFields Tests ---

    @Test
    @DisplayName("Disposition: getFields should return mandatory fields")
    void testGetFields() {
        Disposition disposition = new Disposition(true, 100L);
        List<Object> fields = disposition.getFields();

        assertTrue(fields.size() >= 2);
        assertEquals(true, fields.get(0)); // role
        assertEquals(100L, fields.get(1)); // first
    }

    @Test
    @DisplayName("Disposition: getFields should include all set values")
    void testGetFieldsWithValues() {
        Disposition disposition = new Disposition(true, 0)
                .setLast(10L)
                .setSettled(true)
                .setState(Accepted.INSTANCE);

        List<Object> fields = disposition.getFields();

        assertTrue(fields.size() >= 5);
        assertEquals(true, fields.get(0)); // role
        assertEquals(0L, fields.get(1)); // first
        assertEquals(10L, fields.get(2)); // last
        assertEquals(true, fields.get(3)); // settled
    }

    // --- toString Tests ---

    @Test
    @DisplayName("Disposition: toString should include key fields")
    void testToString() {
        Disposition disposition = new Disposition(true, 5)
                .setLast(10L)
                .setSettled(true)
                .setState(Accepted.INSTANCE);

        String str = disposition.toString();

        assertTrue(str.contains("receiver"));
        assertTrue(str.contains("5"));
        assertTrue(str.contains("10"));
        assertTrue(str.contains("true"));
    }

    @Test
    @DisplayName("Disposition: toString for sender role")
    void testToStringSender() {
        Disposition disposition = new Disposition(false, 0);
        String str = disposition.toString();
        assertTrue(str.contains("sender"));
    }

    // --- Batch Disposition Tests ---

    @Test
    @DisplayName("Disposition: Batch accept multiple deliveries")
    void testBatchAccept() {
        Disposition disposition = new Disposition(true, 0)
                .setLast(99L)
                .setSettled(true)
                .setState(Accepted.INSTANCE)
                .setBatchable(true);

        // Should cover all 100 deliveries (0-99)
        assertTrue(disposition.covers(0));
        assertTrue(disposition.covers(50));
        assertTrue(disposition.covers(99));
        assertFalse(disposition.covers(100));
    }

    @Test
    @DisplayName("Disposition: Single delivery disposition")
    void testSingleDeliveryDisposition() {
        Disposition disposition = new Disposition(true, 42)
                .setSettled(true)
                .setState(Accepted.INSTANCE);

        assertTrue(disposition.covers(42));
        assertFalse(disposition.covers(41));
        assertFalse(disposition.covers(43));
    }

    // --- Transactional Disposition Tests ---

    @Test
    @DisplayName("Disposition: Transactional accept with TransactionalState")
    void testTransactionalAccept() {
        byte[] txnId = {0x01, 0x02, 0x03, 0x04};
        TransactionalState txnState = new TransactionalState(txnId, Accepted.INSTANCE);

        Disposition disposition = new Disposition(true, 0)
                .setSettled(true)
                .setState(txnState);

        TransactionalState state = (TransactionalState) disposition.getState();
        assertArrayEquals(txnId, state.getTxnId());
        assertEquals(Accepted.INSTANCE, state.getOutcome());
    }

    @Test
    @DisplayName("Disposition: Transactional disposition with range")
    void testTransactionalDispositionRange() {
        byte[] txnId = {0x01};
        TransactionalState txnState = new TransactionalState(txnId, Accepted.INSTANCE);

        Disposition disposition = new Disposition(true, 0)
                .setLast(10L)
                .setSettled(true)
                .setState(txnState);

        // All 11 deliveries (0-10) are covered
        for (int i = 0; i <= 10; i++) {
            assertTrue(disposition.covers(i));
        }
    }

    // --- Receiver First/Second Settle Mode Tests ---

    @Test
    @DisplayName("Disposition: Receiver settle mode first - settle on accept")
    void testReceiverSettleModeFirst() {
        // In mode first, receiver settles when sending disposition
        Disposition disposition = new Disposition(true, 0)
                .setSettled(true)
                .setState(Accepted.INSTANCE);

        assertTrue(disposition.isSettled());
        assertTrue(disposition.isReceiver());
    }

    @Test
    @DisplayName("Disposition: Receiver settle mode second - unsettled then settled")
    void testReceiverSettleModeSecond() {
        // First disposition: unsettled with state
        Disposition first = new Disposition(true, 0)
                .setSettled(false)
                .setState(Accepted.INSTANCE);

        assertFalse(first.isSettled());

        // Second disposition: settled after sender echo
        Disposition second = new Disposition(true, 0)
                .setSettled(true);

        assertTrue(second.isSettled());
    }
}
