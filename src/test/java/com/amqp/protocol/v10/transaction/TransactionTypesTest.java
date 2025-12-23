package com.amqp.protocol.v10.transaction;

import com.amqp.protocol.v10.delivery.Accepted;
import com.amqp.protocol.v10.types.DescribedType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 Transaction types: Declare, Discharge, Declared, TransactionalState.
 */
@DisplayName("AMQP 1.0 Transaction Types Tests")
public class TransactionTypesTest {

    // --- Declare Tests ---

    @Test
    @DisplayName("Declare: Should have correct descriptor")
    void testDeclareDescriptor() {
        assertEquals(0x31L, Declare.DESCRIPTOR);
    }

    @Test
    @DisplayName("Declare: Default constructor creates empty declare")
    void testDeclareDefaultConstructor() {
        Declare declare = new Declare();
        assertNull(declare.getGlobalId());
        assertFalse(declare.hasGlobalId());
    }

    @Test
    @DisplayName("Declare: Constructor with global ID")
    void testDeclareWithGlobalId() {
        byte[] globalId = {0x01, 0x02, 0x03, 0x04};
        Declare declare = new Declare(globalId);
        assertArrayEquals(globalId, declare.getGlobalId());
        assertTrue(declare.hasGlobalId());
    }

    @Test
    @DisplayName("Declare: toDescribed produces valid described type")
    void testDeclareToDescribed() {
        Declare declare = new Declare();
        DescribedType described = declare.toDescribed();
        assertNotNull(described);
        assertEquals(Declare.DESCRIPTOR, described.getDescriptor());
    }

    // --- Discharge Tests ---

    @Test
    @DisplayName("Discharge: Should have correct descriptor")
    void testDischargeDescriptor() {
        assertEquals(0x32L, Discharge.DESCRIPTOR);
    }

    @Test
    @DisplayName("Discharge: Constructor sets txnId and fail flag")
    void testDischargeConstructor() {
        byte[] txnId = {0x01, 0x02};
        Discharge discharge = new Discharge(txnId, true);
        assertArrayEquals(txnId, discharge.getTxnId());
        assertTrue(discharge.getFail());
    }

    @Test
    @DisplayName("Discharge: shouldRollback returns true when fail is true")
    void testDischargeShouldRollback() {
        Discharge discharge = new Discharge(new byte[]{0x01}, true);
        assertTrue(discharge.shouldRollback());
        assertFalse(discharge.shouldCommit());
    }

    @Test
    @DisplayName("Discharge: shouldCommit returns true when fail is false")
    void testDischargeShouldCommit() {
        Discharge discharge = new Discharge(new byte[]{0x01}, false);
        assertTrue(discharge.shouldCommit());
        assertFalse(discharge.shouldRollback());
    }

    @Test
    @DisplayName("Discharge: shouldCommit returns true when fail is null")
    void testDischargeShouldCommitWhenNull() {
        Discharge discharge = new Discharge();
        discharge.setTxnId(new byte[]{0x01});
        assertTrue(discharge.shouldCommit());
        assertFalse(discharge.shouldRollback());
    }

    @Test
    @DisplayName("Discharge: forCommit factory method")
    void testDischargeForCommit() {
        byte[] txnId = {0x01, 0x02};
        Discharge discharge = Discharge.forCommit(txnId);
        assertArrayEquals(txnId, discharge.getTxnId());
        assertTrue(discharge.shouldCommit());
    }

    @Test
    @DisplayName("Discharge: forRollback factory method")
    void testDischargeForRollback() {
        byte[] txnId = {0x01, 0x02};
        Discharge discharge = Discharge.forRollback(txnId);
        assertArrayEquals(txnId, discharge.getTxnId());
        assertTrue(discharge.shouldRollback());
    }

    // --- Declared Tests ---

    @Test
    @DisplayName("Declared: Should have correct descriptor")
    void testDeclaredDescriptor() {
        assertEquals(0x33L, Declared.DESCRIPTOR);
    }

    @Test
    @DisplayName("Declared: Constructor sets txnId")
    void testDeclaredConstructor() {
        byte[] txnId = {0x01, 0x02, 0x03};
        Declared declared = new Declared(txnId);
        assertArrayEquals(txnId, declared.getTxnId());
    }

    @Test
    @DisplayName("Declared: getDescriptor returns correct value")
    void testDeclaredGetDescriptor() {
        Declared declared = new Declared(new byte[]{0x01});
        assertEquals(Declared.DESCRIPTOR, declared.getDescriptor());
    }

    @Test
    @DisplayName("Declared: isTerminal returns true")
    void testDeclaredIsTerminal() {
        Declared declared = new Declared(new byte[]{0x01});
        assertTrue(declared.isTerminal());
    }

    @Test
    @DisplayName("Declared: toDescribed produces valid described type")
    void testDeclaredToDescribed() {
        Declared declared = new Declared(new byte[]{0x01, 0x02});
        DescribedType described = declared.toDescribed();
        assertNotNull(described);
        assertEquals(Declared.DESCRIPTOR, described.getDescriptor());
    }

    // --- TransactionalState Tests ---

    @Test
    @DisplayName("TransactionalState: Should have correct descriptor")
    void testTransactionalStateDescriptor() {
        assertEquals(0x34L, TransactionalState.DESCRIPTOR);
    }

    @Test
    @DisplayName("TransactionalState: Constructor with txnId only")
    void testTransactionalStateConstructorTxnIdOnly() {
        byte[] txnId = {0x01, 0x02};
        TransactionalState state = new TransactionalState(txnId);
        assertArrayEquals(txnId, state.getTxnId());
        assertNull(state.getOutcome());
    }

    @Test
    @DisplayName("TransactionalState: Constructor with txnId and outcome")
    void testTransactionalStateConstructorWithOutcome() {
        byte[] txnId = {0x01, 0x02};
        TransactionalState state = new TransactionalState(txnId, Accepted.INSTANCE);
        assertArrayEquals(txnId, state.getTxnId());
        assertEquals(Accepted.INSTANCE, state.getOutcome());
    }

    @Test
    @DisplayName("TransactionalState: isTerminal returns false")
    void testTransactionalStateIsTerminal() {
        TransactionalState state = new TransactionalState(new byte[]{0x01});
        assertFalse(state.isTerminal());
    }

    @Test
    @DisplayName("TransactionalState: toDescribed produces valid described type")
    void testTransactionalStateToDescribed() {
        TransactionalState state = new TransactionalState(new byte[]{0x01}, Accepted.INSTANCE);
        DescribedType described = state.toDescribed();
        assertNotNull(described);
        assertEquals(TransactionalState.DESCRIPTOR, described.getDescriptor());
    }

    @Test
    @DisplayName("TransactionalState: setOutcome updates outcome")
    void testTransactionalStateSetOutcome() {
        TransactionalState state = new TransactionalState(new byte[]{0x01});
        assertNull(state.getOutcome());

        state.setOutcome(Accepted.INSTANCE);
        assertEquals(Accepted.INSTANCE, state.getOutcome());
    }

    @Test
    @DisplayName("TransactionalState: toString includes txnId")
    void testTransactionalStateToString() {
        byte[] txnId = {0x01, 0x02};
        TransactionalState state = new TransactionalState(txnId);
        String str = state.toString();

        assertTrue(str.contains("TransactionalState"));
        assertTrue(str.contains("0102")); // hex representation
    }
}
