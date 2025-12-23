package com.amqp.protocol.v10.transaction;

import com.amqp.protocol.v10.delivery.DeliveryState;
import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * AMQP 1.0 Declared outcome.
 *
 * This outcome is returned when a transaction has been successfully declared.
 * It contains the transaction-id which must be used in subsequent transactional
 * operations.
 *
 * Fields:
 * 0: txn-id (binary) - The assigned transaction-id (required)
 */
public class Declared implements DeliveryState {

    public static final long DESCRIPTOR = AmqpType.Descriptor.DECLARED;

    private byte[] txnId;

    public Declared() {
    }

    public Declared(byte[] txnId) {
        this.txnId = txnId;
    }

    public byte[] getTxnId() {
        return txnId;
    }

    public Declared setTxnId(byte[] txnId) {
        this.txnId = txnId;
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
        return new DescribedType.Default(DESCRIPTOR, fields);
    }

    @Override
    public boolean isTerminal() {
        // Declared is a terminal state for the declare request
        return true;
    }

    public static Declared decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);
        Declared declared = new Declared();

        Object txnIdField = TypeDecoder.getField(fields, 0);
        if (txnIdField instanceof byte[]) {
            declared.txnId = (byte[]) txnIdField;
        }

        return declared;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Declared{txnId=");
        if (txnId != null) {
            for (byte b : txnId) {
                sb.append(String.format("%02x", b));
            }
        } else {
            sb.append("null");
        }
        sb.append("}");
        return sb.toString();
    }
}
