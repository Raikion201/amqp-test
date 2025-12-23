package com.amqp.protocol.v10.transaction;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * AMQP 1.0 Discharge message.
 *
 * Message body for discharging a transaction. The discharge message is sent
 * to the coordinator to commit or rollback a transaction. A successful
 * discharge results in an Accepted outcome if the transaction was successfully
 * discharged.
 *
 * Fields:
 * 0: txn-id (binary) - The transaction-id to discharge (required)
 * 1: fail (boolean) - If true, rollback the transaction; if false, commit
 */
public class Discharge {

    public static final long DESCRIPTOR = AmqpType.Descriptor.DISCHARGE;

    private byte[] txnId;
    private Boolean fail;

    public Discharge() {
    }

    public Discharge(byte[] txnId, boolean fail) {
        this.txnId = txnId;
        this.fail = fail;
    }

    public byte[] getTxnId() {
        return txnId;
    }

    public Discharge setTxnId(byte[] txnId) {
        this.txnId = txnId;
        return this;
    }

    public Boolean getFail() {
        return fail;
    }

    public boolean shouldRollback() {
        return fail != null && fail;
    }

    public boolean shouldCommit() {
        return fail == null || !fail;
    }

    public Discharge setFail(Boolean fail) {
        this.fail = fail;
        return this;
    }

    public static Discharge forCommit(byte[] txnId) {
        return new Discharge(txnId, false);
    }

    public static Discharge forRollback(byte[] txnId) {
        return new Discharge(txnId, true);
    }

    public DescribedType toDescribed() {
        List<Object> fields = new ArrayList<>();
        fields.add(txnId);
        fields.add(fail);

        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return new DescribedType.Default(DESCRIPTOR, fields);
    }

    public static Discharge decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);
        Discharge discharge = new Discharge();

        Object txnIdField = TypeDecoder.getField(fields, 0);
        if (txnIdField instanceof byte[]) {
            discharge.txnId = (byte[]) txnIdField;
        }

        discharge.fail = TypeDecoder.getField(fields, 1, Boolean.class);

        return discharge;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Discharge{txnId=");
        if (txnId != null) {
            for (byte b : txnId) {
                sb.append(String.format("%02x", b));
            }
        } else {
            sb.append("null");
        }
        sb.append(", fail=").append(fail).append("}");
        return sb.toString();
    }
}
