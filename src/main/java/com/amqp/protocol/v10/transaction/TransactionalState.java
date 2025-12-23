package com.amqp.protocol.v10.transaction;

import com.amqp.protocol.v10.delivery.DeliveryState;
import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * AMQP 1.0 Transactional State.
 *
 * This delivery state is used to associate a transfer or disposition with
 * a transaction. It wraps another outcome to indicate the final disposition
 * of the message within the transaction.
 *
 * Fields:
 * 0: txn-id (binary) - The transaction-id (required)
 * 1: outcome (delivery-state) - The outcome to be applied transactionally
 */
public class TransactionalState implements DeliveryState {

    public static final long DESCRIPTOR = AmqpType.Descriptor.TRANSACTIONAL_STATE;

    private byte[] txnId;
    private DeliveryState outcome;

    public TransactionalState() {
    }

    public TransactionalState(byte[] txnId) {
        this.txnId = txnId;
    }

    public TransactionalState(byte[] txnId, DeliveryState outcome) {
        this.txnId = txnId;
        this.outcome = outcome;
    }

    public byte[] getTxnId() {
        return txnId;
    }

    public TransactionalState setTxnId(byte[] txnId) {
        this.txnId = txnId;
        return this;
    }

    public DeliveryState getOutcome() {
        return outcome;
    }

    public TransactionalState setOutcome(DeliveryState outcome) {
        this.outcome = outcome;
        return this;
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public DescribedType toDescribed() {
        List<Object> fields = new ArrayList<>();
        fields.add(txnId);
        fields.add(outcome != null ? outcome.toDescribed() : null);

        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return new DescribedType.Default(DESCRIPTOR, fields);
    }

    @Override
    public boolean isTerminal() {
        // Transactional state is not terminal until the transaction completes
        return false;
    }

    public static TransactionalState decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);
        TransactionalState state = new TransactionalState();

        Object txnIdField = TypeDecoder.getField(fields, 0);
        if (txnIdField instanceof byte[]) {
            state.txnId = (byte[]) txnIdField;
        }

        Object outcomeField = TypeDecoder.getField(fields, 1);
        if (outcomeField instanceof DescribedType) {
            state.outcome = decodeOutcome((DescribedType) outcomeField);
        }

        return state;
    }

    private static DeliveryState decodeOutcome(DescribedType described) {
        long descriptor = ((Number) described.getDescriptor()).longValue();

        // Use DeliveryStateDecoder for standard outcomes
        switch ((int) descriptor) {
            case 0x24: // ACCEPTED
                return com.amqp.protocol.v10.delivery.Accepted.decode(described);
            case 0x25: // REJECTED
                return com.amqp.protocol.v10.delivery.Rejected.decode(described);
            case 0x26: // RELEASED
                return com.amqp.protocol.v10.delivery.Released.decode(described);
            case 0x27: // MODIFIED
                return com.amqp.protocol.v10.delivery.Modified.decode(described);
            default:
                return null;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("TransactionalState{txnId=");
        if (txnId != null) {
            for (byte b : txnId) {
                sb.append(String.format("%02x", b));
            }
        } else {
            sb.append("null");
        }
        sb.append(", outcome=").append(outcome).append("}");
        return sb.toString();
    }
}
