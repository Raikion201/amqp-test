/*
 * Adapted from Apache Qpid Broker-J AMQP 1.0 protocol tests.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.protocol.v10.transport;

import com.amqp.protocol.v10.delivery.Accepted;
import com.amqp.protocol.v10.transaction.TransactionalState;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 Transfer performative.
 * Adapted from Apache Qpid Broker-J TransferTest.
 */
@DisplayName("AMQP 1.0 Transfer Performative Tests")
public class TransferTest {

    // --- Basic Tests ---

    @Test
    @DisplayName("Transfer: Should have correct descriptor")
    void testDescriptor() {
        Transfer transfer = new Transfer(0);
        assertEquals(0x14L, transfer.getDescriptor());
    }

    @Test
    @DisplayName("Transfer: Constructor sets handle")
    void testConstructor() {
        Transfer transfer = new Transfer(10L);
        assertEquals(10L, transfer.getHandle());
    }

    // --- Handle Tests ---

    @Test
    @DisplayName("Transfer: Handle can be zero")
    void testZeroHandle() {
        Transfer transfer = new Transfer(0);
        assertEquals(0L, transfer.getHandle());
    }

    @Test
    @DisplayName("Transfer: Handle can be large value")
    void testLargeHandle() {
        long largeHandle = 0xFFFFFFFFL;
        Transfer transfer = new Transfer(largeHandle);
        assertEquals(largeHandle, transfer.getHandle());
    }

    // --- Delivery ID Tests ---

    @Test
    @DisplayName("Transfer: Default delivery ID is null")
    void testDefaultDeliveryId() {
        Transfer transfer = new Transfer(0);
        assertNull(transfer.getDeliveryId());
    }

    @Test
    @DisplayName("Transfer: Should set delivery ID")
    void testSetDeliveryId() {
        Transfer transfer = new Transfer(0)
                .setDeliveryId(100L);
        assertEquals(100L, transfer.getDeliveryId());
    }

    @Test
    @DisplayName("Transfer: Delivery ID can be zero")
    void testZeroDeliveryId() {
        Transfer transfer = new Transfer(0)
                .setDeliveryId(0L);
        assertEquals(0L, transfer.getDeliveryId());
    }

    // --- Delivery Tag Tests ---

    @Test
    @DisplayName("Transfer: Default delivery tag is null")
    void testDefaultDeliveryTag() {
        Transfer transfer = new Transfer(0);
        assertNull(transfer.getDeliveryTag());
    }

    @Test
    @DisplayName("Transfer: Should set delivery tag")
    void testSetDeliveryTag() {
        byte[] tag = {0x01, 0x02, 0x03, 0x04};
        Transfer transfer = new Transfer(0)
                .setDeliveryTag(tag);
        assertArrayEquals(tag, transfer.getDeliveryTag());
    }

    @Test
    @DisplayName("Transfer: Empty delivery tag is valid")
    void testEmptyDeliveryTag() {
        byte[] tag = new byte[0];
        Transfer transfer = new Transfer(0)
                .setDeliveryTag(tag);
        assertArrayEquals(tag, transfer.getDeliveryTag());
    }

    // --- Message Format Tests ---

    @Test
    @DisplayName("Transfer: MESSAGE_FORMAT_AMQP constant is zero")
    void testMessageFormatConstant() {
        assertEquals(0L, Transfer.MESSAGE_FORMAT_AMQP);
    }

    @Test
    @DisplayName("Transfer: Default message format is null")
    void testDefaultMessageFormat() {
        Transfer transfer = new Transfer(0);
        assertNull(transfer.getMessageFormat());
    }

    @Test
    @DisplayName("Transfer: Should set message format")
    void testSetMessageFormat() {
        Transfer transfer = new Transfer(0)
                .setMessageFormat(Transfer.MESSAGE_FORMAT_AMQP);
        assertEquals(Transfer.MESSAGE_FORMAT_AMQP, transfer.getMessageFormat());
    }

    // --- Settled Tests ---

    @Test
    @DisplayName("Transfer: Default settled is null")
    void testDefaultSettled() {
        Transfer transfer = new Transfer(0);
        assertNull(transfer.getSettled());
    }

    @Test
    @DisplayName("Transfer: isSettled returns false when null")
    void testIsSettledNull() {
        Transfer transfer = new Transfer(0);
        assertFalse(transfer.isSettled());
    }

    @Test
    @DisplayName("Transfer: Should set settled true")
    void testSetSettledTrue() {
        Transfer transfer = new Transfer(0)
                .setSettled(true);
        assertTrue(transfer.isSettled());
        assertEquals(Boolean.TRUE, transfer.getSettled());
    }

    @Test
    @DisplayName("Transfer: Should set settled false")
    void testSetSettledFalse() {
        Transfer transfer = new Transfer(0)
                .setSettled(false);
        assertFalse(transfer.isSettled());
        assertEquals(Boolean.FALSE, transfer.getSettled());
    }

    @Test
    @DisplayName("Transfer: Pre-settled transfer")
    void testPresettledTransfer() {
        Transfer transfer = new Transfer(0)
                .setDeliveryId(1L)
                .setDeliveryTag(new byte[]{0x01})
                .setSettled(true)
                .setMessageFormat(0L);

        assertTrue(transfer.isSettled());
    }

    // --- More Tests ---

    @Test
    @DisplayName("Transfer: Default more is null")
    void testDefaultMore() {
        Transfer transfer = new Transfer(0);
        assertNull(transfer.getMore());
    }

    @Test
    @DisplayName("Transfer: hasMore returns false when null")
    void testHasMoreNull() {
        Transfer transfer = new Transfer(0);
        assertFalse(transfer.hasMore());
    }

    @Test
    @DisplayName("Transfer: Should set more true for multi-frame message")
    void testSetMoreTrue() {
        Transfer transfer = new Transfer(0)
                .setMore(true);
        assertTrue(transfer.hasMore());
        assertEquals(Boolean.TRUE, transfer.getMore());
    }

    @Test
    @DisplayName("Transfer: Should set more false for last frame")
    void testSetMoreFalse() {
        Transfer transfer = new Transfer(0)
                .setMore(false);
        assertFalse(transfer.hasMore());
    }

    // --- Receiver Settle Mode Tests ---

    @Test
    @DisplayName("Transfer: Default receiver settle mode is null")
    void testDefaultRcvSettleMode() {
        Transfer transfer = new Transfer(0);
        assertNull(transfer.getRcvSettleMode());
    }

    @Test
    @DisplayName("Transfer: Should set receiver settle mode first")
    void testSetRcvSettleModeFirst() {
        Transfer transfer = new Transfer(0)
                .setRcvSettleMode(Attach.RCV_SETTLE_MODE_FIRST);
        assertEquals(Attach.RCV_SETTLE_MODE_FIRST, transfer.getRcvSettleMode());
    }

    @Test
    @DisplayName("Transfer: Should set receiver settle mode second")
    void testSetRcvSettleModeSecond() {
        Transfer transfer = new Transfer(0)
                .setRcvSettleMode(Attach.RCV_SETTLE_MODE_SECOND);
        assertEquals(Attach.RCV_SETTLE_MODE_SECOND, transfer.getRcvSettleMode());
    }

    // --- State Tests ---

    @Test
    @DisplayName("Transfer: Default state is null")
    void testDefaultState() {
        Transfer transfer = new Transfer(0);
        assertNull(transfer.getState());
    }

    @Test
    @DisplayName("Transfer: Should set delivery state")
    void testSetState() {
        Transfer transfer = new Transfer(0)
                .setState(Accepted.INSTANCE);
        assertEquals(Accepted.INSTANCE, transfer.getState());
    }

    @Test
    @DisplayName("Transfer: Should set transactional state")
    void testSetTransactionalState() {
        byte[] txnId = {0x01, 0x02};
        TransactionalState txnState = new TransactionalState(txnId);
        Transfer transfer = new Transfer(0)
                .setState(txnState);
        assertEquals(txnState, transfer.getState());
    }

    // --- Resume Tests ---

    @Test
    @DisplayName("Transfer: Default resume is null")
    void testDefaultResume() {
        Transfer transfer = new Transfer(0);
        assertNull(transfer.getResume());
    }

    @Test
    @DisplayName("Transfer: isResume returns false when null")
    void testIsResumeNull() {
        Transfer transfer = new Transfer(0);
        assertFalse(transfer.isResume());
    }

    @Test
    @DisplayName("Transfer: Should set resume true")
    void testSetResumeTrue() {
        Transfer transfer = new Transfer(0)
                .setResume(true);
        assertTrue(transfer.isResume());
    }

    // --- Aborted Tests ---

    @Test
    @DisplayName("Transfer: Default aborted is null")
    void testDefaultAborted() {
        Transfer transfer = new Transfer(0);
        assertNull(transfer.getAborted());
    }

    @Test
    @DisplayName("Transfer: isAborted returns false when null")
    void testIsAbortedNull() {
        Transfer transfer = new Transfer(0);
        assertFalse(transfer.isAborted());
    }

    @Test
    @DisplayName("Transfer: Should set aborted true")
    void testSetAbortedTrue() {
        Transfer transfer = new Transfer(0)
                .setAborted(true);
        assertTrue(transfer.isAborted());
    }

    @Test
    @DisplayName("Transfer: Aborted delivery does not require payload")
    void testAbortedNoPayload() {
        Transfer transfer = new Transfer(0)
                .setDeliveryId(1L)
                .setAborted(true);
        assertTrue(transfer.isAborted());
        assertNull(transfer.getPayload());
    }

    // --- Batchable Tests ---

    @Test
    @DisplayName("Transfer: Default batchable is null")
    void testDefaultBatchable() {
        Transfer transfer = new Transfer(0);
        assertNull(transfer.getBatchable());
    }

    @Test
    @DisplayName("Transfer: isBatchable returns false when null")
    void testIsBatchableNull() {
        Transfer transfer = new Transfer(0);
        assertFalse(transfer.isBatchable());
    }

    @Test
    @DisplayName("Transfer: Should set batchable true")
    void testSetBatchableTrue() {
        Transfer transfer = new Transfer(0)
                .setBatchable(true);
        assertTrue(transfer.isBatchable());
    }

    // --- Payload Tests ---

    @Test
    @DisplayName("Transfer: Default payload is null")
    void testDefaultPayload() {
        Transfer transfer = new Transfer(0);
        assertNull(transfer.getPayload());
    }

    @Test
    @DisplayName("Transfer: Should set payload")
    void testSetPayload() {
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[]{0x01, 0x02, 0x03});
        Transfer transfer = new Transfer(0)
                .setPayload(payload);
        assertEquals(payload, transfer.getPayload());
    }

    @Test
    @DisplayName("Transfer: Empty payload is valid")
    void testEmptyPayload() {
        ByteBuf payload = Unpooled.EMPTY_BUFFER;
        Transfer transfer = new Transfer(0)
                .setPayload(payload);
        assertEquals(0, transfer.getPayload().readableBytes());
    }

    // --- Builder Pattern Tests ---

    @Test
    @DisplayName("Transfer: Builder pattern should be chainable")
    void testBuilderChaining() {
        byte[] tag = {0x01, 0x02};
        ByteBuf payload = Unpooled.wrappedBuffer("Hello".getBytes());

        Transfer transfer = new Transfer(5L)
                .setDeliveryId(100L)
                .setDeliveryTag(tag)
                .setMessageFormat(Transfer.MESSAGE_FORMAT_AMQP)
                .setSettled(false)
                .setMore(false)
                .setRcvSettleMode(Attach.RCV_SETTLE_MODE_FIRST)
                .setBatchable(true)
                .setPayload(payload);

        assertEquals(5L, transfer.getHandle());
        assertEquals(100L, transfer.getDeliveryId());
        assertArrayEquals(tag, transfer.getDeliveryTag());
        assertEquals(Transfer.MESSAGE_FORMAT_AMQP, transfer.getMessageFormat());
        assertFalse(transfer.isSettled());
        assertFalse(transfer.hasMore());
        assertEquals(Attach.RCV_SETTLE_MODE_FIRST, transfer.getRcvSettleMode());
        assertTrue(transfer.isBatchable());
        assertEquals(payload, transfer.getPayload());
    }

    // --- getFields Tests ---

    @Test
    @DisplayName("Transfer: getFields should return handle")
    void testGetFields() {
        Transfer transfer = new Transfer(10L);
        List<Object> fields = transfer.getFields();

        assertTrue(fields.size() >= 1);
        assertEquals(10L, fields.get(0));
    }

    @Test
    @DisplayName("Transfer: getFields should include all set values")
    void testGetFieldsWithValues() {
        Transfer transfer = new Transfer(0)
                .setDeliveryId(1L)
                .setDeliveryTag(new byte[]{0x01})
                .setMessageFormat(0L)
                .setSettled(true);

        List<Object> fields = transfer.getFields();

        assertTrue(fields.size() >= 5);
        assertEquals(0L, fields.get(0)); // handle
        assertEquals(1L, fields.get(1)); // deliveryId
        assertArrayEquals(new byte[]{0x01}, (byte[]) fields.get(2)); // deliveryTag
        assertEquals(0L, fields.get(3)); // messageFormat
        assertEquals(true, fields.get(4)); // settled
    }

    // --- toString Tests ---

    @Test
    @DisplayName("Transfer: toString should include key fields")
    void testToString() {
        ByteBuf payload = Unpooled.wrappedBuffer("test".getBytes());
        Transfer transfer = new Transfer(5L)
                .setDeliveryId(100L)
                .setSettled(true)
                .setPayload(payload);

        String str = transfer.toString();

        assertTrue(str.contains("5")); // handle
        assertTrue(str.contains("100")); // deliveryId
        assertTrue(str.contains("true")); // settled
    }

    // --- Multi-Transfer Message Tests ---

    @Test
    @DisplayName("Transfer: First frame of multi-transfer should have more=true")
    void testMultiTransferFirstFrame() {
        Transfer first = new Transfer(0)
                .setDeliveryId(1L)
                .setDeliveryTag(new byte[]{0x01})
                .setMore(true)
                .setMessageFormat(0L);

        assertTrue(first.hasMore());
        assertEquals(1L, first.getDeliveryId());
    }

    @Test
    @DisplayName("Transfer: Continuation frame has no delivery-id or tag")
    void testMultiTransferContinuation() {
        Transfer continuation = new Transfer(0)
                .setMore(true);

        assertNull(continuation.getDeliveryId());
        assertNull(continuation.getDeliveryTag());
        assertTrue(continuation.hasMore());
    }

    @Test
    @DisplayName("Transfer: Last frame of multi-transfer should have more=false")
    void testMultiTransferLastFrame() {
        Transfer last = new Transfer(0)
                .setMore(false);

        assertFalse(last.hasMore());
    }

    // --- Transactional Transfer Tests ---

    @Test
    @DisplayName("Transfer: Transactional send with transactional state")
    void testTransactionalSend() {
        byte[] txnId = {0x01, 0x02, 0x03, 0x04};
        TransactionalState txnState = new TransactionalState(txnId);

        Transfer transfer = new Transfer(0)
                .setDeliveryId(1L)
                .setDeliveryTag(new byte[]{0x01})
                .setSettled(false) // Transactional sends are never pre-settled
                .setState(txnState);

        assertFalse(transfer.isSettled());
        assertEquals(txnState, transfer.getState());
    }

    @Test
    @DisplayName("Transfer: Transactional receive with transactional state and outcome")
    void testTransactionalReceive() {
        byte[] txnId = {0x01, 0x02, 0x03, 0x04};
        TransactionalState txnState = new TransactionalState(txnId, Accepted.INSTANCE);

        Transfer transfer = new Transfer(0)
                .setState(txnState);

        TransactionalState state = (TransactionalState) transfer.getState();
        assertArrayEquals(txnId, state.getTxnId());
        assertEquals(Accepted.INSTANCE, state.getOutcome());
    }
}
